package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionMeta;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.CronFieldName;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class TtlMetaValidationUtil {

    private static final Logger logger = LoggerFactory.getLogger(TtlMetaValidationUtil.class);

    protected static Set<TtlTimeUnit> supportedTtlUnit = new HashSet<>();

    static {
        supportedTtlUnit.add(TtlTimeUnit.UNDEFINED);
        supportedTtlUnit.add(TtlTimeUnit.DAY);
        supportedTtlUnit.add(TtlTimeUnit.MONTH);
        supportedTtlUnit.add(TtlTimeUnit.YEAR);
        supportedTtlUnit.add(TtlTimeUnit.NUMBER);
    }

    public static void validateTtlInfoChange(TtlDefinitionInfo oldTtlInfo,
                                             TtlDefinitionInfo newTtlInfo,
                                             ExecutionContext ec) {
        TtlMetaValidationUtil.validateTtlColChange(oldTtlInfo, newTtlInfo, ec);
        TtlMetaValidationUtil.validateArcTblChange(oldTtlInfo, newTtlInfo, ec);
        TtlMetaValidationUtil.validateArcKindChange(oldTtlInfo, newTtlInfo, ec);
    }

    protected static void validateTtlColChange(TtlDefinitionInfo oldTtlInfo,
                                               TtlDefinitionInfo newTtlInfo,
                                               ExecutionContext ec) {
        try {
            if (oldTtlInfo == null) {
                return;
            }

            String oldTtlCol = oldTtlInfo.getTtlInfoRecord().getTtlCol();
            boolean useArchiveOfOldTtlInfo = oldTtlInfo.needPerformExpiredDataArchiving();

            String newTtlCol = newTtlInfo.getTtlInfoRecord().getTtlCol();
            if (useArchiveOfOldTtlInfo) {
                String arcTblName = oldTtlInfo.getTtlInfoRecord().getArcTblName();
                if (!newTtlCol.equalsIgnoreCase(oldTtlCol)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                        String.format(
                            "Forbid to change ttl column from `%s` to `%s` because archived table `%s` is using",
                            oldTtlCol,
                            newTtlCol, arcTblName));
                }
            }
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }
    }

    protected static void validateArcTblChange(TtlDefinitionInfo oldTtlInfo,
                                               TtlDefinitionInfo newTtlInfo,
                                               ExecutionContext ec) {

        if (oldTtlInfo == null) {

            return;
        }

        String oldArcTblSchema = oldTtlInfo.getTtlInfoRecord().getArcTblSchema();
        String oldArcTblName = oldTtlInfo.getTtlInfoRecord().getArcTblName();

        String newArcTblSchema = newTtlInfo.getTtlInfoRecord().getArcTblSchema();
        String newArcTblName = newTtlInfo.getTtlInfoRecord().getArcTblName();

        if (StringUtils.isEmpty(oldArcTblName) && StringUtils.isEmpty(oldArcTblSchema)) {
            // no archive table
            return;
        }

        if (StringUtils.isEmpty(newArcTblName) && StringUtils.isEmpty(newArcTblSchema)) {
            // unbind archive table
            return;
        }

        if (!StringUtils.isEmpty(newArcTblName) && !oldArcTblName.equalsIgnoreCase(newArcTblName) ||
            !StringUtils.isEmpty(newArcTblSchema) && !oldArcTblSchema.equalsIgnoreCase(newArcTblSchema)) {
            // found archive table changed
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format("Failed to modify ttl definition because archive table `%s`.`%s` is using",
                    oldArcTblSchema, oldArcTblName));
        }
    }

    protected static void validateArcKindChange(TtlDefinitionInfo oldTtlInfo,
                                                TtlDefinitionInfo newTtlInfo,
                                                ExecutionContext ec) {

        if (oldTtlInfo == null) {
            return;
        }

        TtlArchiveKind oldArcKind = TtlArchiveKind.of(oldTtlInfo.getTtlInfoRecord().getArcKind());
        TtlArchiveKind newArcKind = TtlArchiveKind.of(newTtlInfo.getTtlInfoRecord().getArcKind());

        boolean alreadyBoundArchiveTable = oldTtlInfo.alreadyBoundArchiveTable();
        String newArcTblSchema = newTtlInfo.getArchiveTableSchema();
        String newArcTblName = newTtlInfo.getArchiveTableName();
        if (newArcKind == TtlArchiveKind.UNDEFINED) {
            // no archive table

            if (!StringUtils.isEmpty(newArcTblName) || !StringUtils.isEmpty(newArcTblSchema)) {
                // found empty archive table name
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to modify ttl definition because archive table must be empty when the arcKind is undefined"));
            }

            if (oldArcKind == TtlArchiveKind.UNDEFINED) {
                /**
                 * arcKind from undefined to undefined,
                 * that means ttl-tbl bind no archive table,
                 * so ignore
                 */
                return;
            } else {

                /**
                 * arcKind from defined to undefined,
                 * that means ttl-tbl unbind archive table, so ignore
                 */
            }
        } else {

            if (StringUtils.isEmpty(newArcTblSchema) || StringUtils.isEmpty(newArcTblName)) {
                if (!newArcKind.archivedByPartitions()) {
                    if (alreadyBoundArchiveTable) {
                        // found archive table changed
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                            String.format(
                                "Failed to modify ttl definition because archive table must be not empty when the arcKind is %s",
                                newArcKind.getArchiveKindStr()));
                    }
                }
            }

            if (oldArcKind == TtlArchiveKind.UNDEFINED) {
                /**
                 * arcKind from undefined to defined,
                 * that means ttl-tbl bind new archive table, so ignore
                 */
                return;
            } else {

                /**
                 * arcKind from old defined to new defined,
                 * that means ttl-tbl want to bind new archive table instead of
                 * old archive table,
                 * so need check if arcKind is different,
                 * it is not allowed to be different.
                 */
                if (oldArcKind != newArcKind) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
//                        String.format("Failed to modify ttl definition because the archive table %s.%s is using",
//                            oldTtlInfo.getTtlInfoRecord().getTableSchema(),
//                            oldTtlInfo.getTtlInfoRecord().getTableName()));
                }
            }
        }

    }

    /**
     * Validate if the definition of ttl-col is valid
     */
    public static void validateTtlColExprAndPartInterval(TtlDefinitionInfo ttlInfo,
                                                         TableMeta ttlTableMeta,
                                                         ExecutionContext ec) {

        try {

            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();

            TableMeta tblMeta = ttlTableMeta;
            if (tblMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_TABLE_NOT_FOUND,
                    String.format("Failed to create ttl definition because `%s`.`%s` does not exist", ttlTblSchema,
                        ttlTblName));
            }

            ColumnMeta ttlCm = tblMeta.getColumn(ttlColName);
            if (ttlCm == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_COLUMN_NOT_FOUND,
                    String.format("Failed to create ttl definition because the column `%s` of `%s`.`%s` does not exist",
                        ttlColName, ttlTblSchema, ttlTblName));
            }

            DataType ttlColDt = ttlCm.getDataType();

            boolean isNumberType =
                DataTypeUtil.isUnderBigintUnsignedType(ttlColDt) || DataTypeUtil.isDecimalType(ttlColDt);
            boolean isMysqlTimeType = DataTypeUtil.isMysqlTimeType(ttlColDt);
            String sqlTypeName = ttlColDt.getStringSqlType();
            if (!(isNumberType || isMysqlTimeType)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                    "Failed to create ttl definition because the datatype `%s` of the column `%s` on `%s`.`%s` is not supported",
                    sqlTypeName, ttlColName, ttlTblSchema, ttlTblName));
            } else {
                /**
                 * Come here, ttlColDt will be numberType or mysqlTimeType(date/datetime/timestamp)
                 */

                boolean useTtlColFunExpr = ttlInfo.isTtlColUseFuncExpr();
                boolean useExpireOverPolicy = ttlInfo.useExpireOverPartitionsPolicy();
                boolean useNoExpirePolicy = ttlInfo.useUndefinedExpirePolicy();
                boolean useNumberAsPartUnit = ttlInfo.useNumberAsPartIntervalUnit();
                boolean archiveByRow = ttlInfo.performArchiveByRow();

                TtlTimeUnit artPartUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
                if (isNumberType) {
                    if (!useTtlColFunExpr) {
                        if (useNumberAsPartUnit) {

                            /**
                             * Use number part_interval unit like ttl_part_interval=interval(n,number)
                             */
                            if (useExpireOverPolicy) {
                                // params ok

                                if (archiveByRow) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                                        "Failed to create ttl definition for `%s`.`%s` because the archive_type row is not supported in using expire over policy",
                                        ttlTblSchema, ttlTblName, artPartUnit.getUnitName()));
                                }
                            } else {
                                if (!useNoExpirePolicy) {
                                    // err def
                                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                                        "Failed to create ttl definition for `%s`.`%s` because the `%s` unit of of ttl_part_interval` is not supported in using expire after policy",
                                        ttlTblSchema, ttlTblName, artPartUnit.getUnitName()));
                                }
                            }

                        } else {
                            // err def
                            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                                "Failed to create ttl definition for `%s`.`%s` because the `%s` unit of of ttl_part_interval` is not supported when using int type as the ttl column",
                                ttlTblSchema, ttlTblName, artPartUnit.getUnitName()));
                        }

                    } else {

                        /**
                         * Use func expr on ttl_col, like ttl_expr = from_unixtime(b) expire after
                         */

                        if (useNumberAsPartUnit) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                                "Failed to create ttl definition for `%s`.`%s` because the `%s` unit of of ttl_part_interval` is not supported when ttl_col is using function expression",
                                ttlTblSchema, ttlTblName, artPartUnit.getUnitName()));
                        }

                        if (useExpireOverPolicy) {
                            // err define
                            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                                "Failed to create ttl definition for `%s`.`%s` because the expire over policy is not supported when ttl_col is using function expression",
                                ttlTblSchema, ttlTblName));
                        }

                    }
                } else {

                    if (useTtlColFunExpr) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition for `%s`.`%s` because the function expression is only allowed using on int-type ttl_col",
                            ttlTblSchema, ttlTblName));
                    }

                    if (useNumberAsPartUnit) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition for `%s`.`%s` because the `%s` unit of of ttl_part_interval` is not supported when ttl_col is a time-based datatype",
                            ttlTblSchema, ttlTblName, artPartUnit.getUnitName()));
                    }

                }
            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }
    }

    public static void validateTtlExpiredInterval(TtlDefinitionInfo ttlInfo, ExecutionContext ec) {

        try {
            boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
            boolean useExpireAfter = ttlInfo.useExpireAfterTimeIntervalPolicy();
            if (useExpireOver) {
                int expireOverCnt = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();
                if (expireOverCnt <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create ttl definition because the ttl expire over value must be more than 0"));
                }

                Integer partInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
                Integer partIntervalUnitCode = ttlInfo.getTtlInfoRecord().getArcPartUnit();

                if (partInterval == null || partIntervalUnitCode == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create ttl definition because the ttl_part_interval definition no found in using expire policy"));

                } else if (partInterval != null && partIntervalUnitCode != null) {
                    if (partInterval <= 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the interval value of ttl_part_interval must be more than 0"));
                    }

                    TtlTimeUnit partIntervalUnit = TtlTimeUnit.of(partIntervalUnitCode);
                    if (!TtlMetaValidationUtil.supportedTtlUnit.contains(partIntervalUnit)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the time unit of ttl_part_interval `%s` is not supported yet",
                            partIntervalUnit.getUnitName()));
                    }
                }

            } else if (useExpireAfter) {

                Integer ttlInterval = ttlInfo.getTtlInfoRecord().getTtlInterval();
                if (ttlInterval != null && ttlInterval <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create ttl definition because the ttl interval value must be more than 0"));
                }

                Integer ttlUnitCode = ttlInfo.getTtlInfoRecord().getTtlUnit();
                if (ttlUnitCode != null) {
                    TtlTimeUnit ttlUnit = TtlTimeUnit.of(ttlUnitCode);
                    if (!TtlMetaValidationUtil.supportedTtlUnit.contains(ttlUnit)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the ttl time unit `%s` is not supported yet",
                            ttlUnit.getUnitName()));
                    }
                }

                Integer artPartUnitCode = ttlInfo.getTtlInfoRecord().getArcPartUnit();
                if (artPartUnitCode != null) {
                    TtlTimeUnit artPartUnit = TtlTimeUnit.of(artPartUnitCode);
                    if (!TtlMetaValidationUtil.supportedTtlUnit.contains(artPartUnit)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the arc partition unit `%s` is not supported yet",
                            artPartUnit.getUnitName()));
                    }
                }

                Integer partInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
                Integer partIntervalUnitCode = ttlInfo.getTtlInfoRecord().getArcPartUnit();
                if ((partInterval == null && partIntervalUnitCode != null) || (partInterval != null
                    && partIntervalUnitCode == null)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create ttl definition because the ttl_part_interval definition is invalid"));
                } else if (partInterval != null && partIntervalUnitCode != null) {
                    if (partInterval <= 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the interval value of ttl_part_interval must be more than 0"));
                    }

                    TtlTimeUnit partIntervalUnit = TtlTimeUnit.of(partIntervalUnitCode);
                    if (partIntervalUnit.isNumberUnit()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the number unit of ttl_part_interval `%s` is not supported in using expire after policy",
                            partIntervalUnit.getUnitName()));
                    }

                    if (!TtlMetaValidationUtil.supportedTtlUnit.contains(partIntervalUnit)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the time unit of ttl_part_interval `%s` is not supported yet",
                            partIntervalUnit.getUnitName()));
                    }
                }

            } else {

                /**
                 * No Expire policy
                 */

                Integer partInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
                Integer partIntervalUnitCode = ttlInfo.getTtlInfoRecord().getArcPartUnit();

                if (partInterval == null || partIntervalUnitCode == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create ttl definition because the ttl_part_interval definition no found in using no-expire policy"));

                } else if (partInterval != null && partIntervalUnitCode != null) {
                    if (partInterval <= 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the interval value of ttl_part_interval must be more than 0"));
                    }

                    TtlTimeUnit partIntervalUnit = TtlTimeUnit.of(partIntervalUnitCode);
                    if (partIntervalUnit.isNumberUnit()) {
                        /**
                         * because in no-expire policy, cannot decide the pivot partition used to perform pre-buld
                         */
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the number unit of ttl_part_interval `%s` is not supported in using no-expire policy",
                            partIntervalUnit.getUnitName()));
                    }

                    if (!TtlMetaValidationUtil.supportedTtlUnit.contains(partIntervalUnit)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create ttl definition because the time unit of ttl_part_interval `%s` is not supported in using no-expire policy",
                            partIntervalUnit.getUnitName()));
                    }
                }

            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }

    }

    public static void validateTimeZoneExpr(String tzExpr, ExecutionContext ec) {
        try {
            if (StringUtils.isEmpty(tzExpr)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                    "Failed to create ttl definition because the ttl timezone `%s` is invalid", tzExpr));
            }
            InternalTimeZone internalTz = TimeZoneUtils.convertFromMySqlTZ(tzExpr);
            if (internalTz == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                    "Failed to create ttl definition because the ttl timezone `%s` is invalid", tzExpr));
            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }
    }

    public static void validateArchiveTableInfo(TtlDefinitionInfo ttlInfo,
                                                TableMeta ttlTableMeta,
                                                ExecutionContext ec) {
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String arcTblSchema = ttlInfo.getTtlInfoRecord().getArcTblSchema();
        String arcTblName = ttlInfo.getTtlInfoRecord().getArcTblSchema();

        if (StringUtils.isEmpty(arcTblName) && StringUtils.isEmpty(arcTblSchema)) {
            /**
             * ok, ignore
             */
        } else if (StringUtils.isEmpty(arcTblName) && !StringUtils.isEmpty(arcTblSchema)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the params of the archive table schema `%s` is invalid",
                arcTblSchema));
        } else if (!StringUtils.isEmpty(arcTblName) && StringUtils.isEmpty(arcTblSchema)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the params of the archive table name `%s` is invalid",
                arcTblName));
        } else {
            if (!ttlTblSchema.equalsIgnoreCase(arcTblSchema)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                    "Failed to create ttl definition because the archive table schema `%s` is not the same as ttl table schema `%s`",
                    arcTblSchema, ttlTblSchema));
            }
        }

    }

    protected static String getFirstColumnarName(boolean isValidateForNewCreateTable,
                                                 TableMeta primTableMeta,
                                                 SqlCreateTable newCreateTableAst) {

        String firstCciNameStr = null;
        if (!isValidateForNewCreateTable) {
            Map<String, GsiMetaManager.GsiIndexMetaBean> cciBeanInfo = primTableMeta.getColumnarIndexPublished();

            if (cciBeanInfo == null || cciBeanInfo.isEmpty()) {
                return firstCciNameStr;
            }

            Collection<GsiMetaManager.GsiIndexMetaBean> cciBeans = cciBeanInfo.values();
            if (cciBeans == null || cciBeans.isEmpty()) {
                return firstCciNameStr;
            }

            Iterator<GsiMetaManager.GsiIndexMetaBean> cciItor = cciBeans.iterator();
            if (cciItor != null && cciItor.hasNext()) {
                GsiMetaManager.GsiIndexMetaBean cciInfo = cciItor.next();
                firstCciNameStr = TddlSqlToRelConverter.unwrapGsiName(cciInfo.indexName);
            }
        } else {
            firstCciNameStr = TtlUtil.getFirstColumnarIndexName(newCreateTableAst);
        }
        return firstCciNameStr;
    }

    public static void validateArchiveCciInfo(TtlDefinitionInfo ttlInfo,
                                              TableMeta tableMeta,
                                              PartitionInfo newCreatedTblPartInfo,
                                              SqlCreateTable sqlCreateTableAst,
                                              ExecutionContext ec) {

        boolean validateForNewCreateTable = newCreatedTblPartInfo != null;
        String arcTblSchema = ttlInfo.getTtlInfoRecord().getArcTblSchema();
        String arcTblName = ttlInfo.getTtlInfoRecord().getArcTblName();

        String arcCciTblSchema = ttlInfo.getTtlInfoRecord().getArcTmpTblSchema();
        String arcCciTblName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();

        if (StringUtils.isEmpty(arcTblSchema) || StringUtils.isEmpty(arcTblName)) {
            return;
        }

        if (!StringUtils.equalsIgnoreCase(arcTblSchema, arcCciTblSchema)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the archive table schema `%s` is not the same as the actual archive cci schema `%s`",
                arcTblSchema, arcCciTblSchema));
        }

        String realCciName = getFirstColumnarName(validateForNewCreateTable, tableMeta, sqlCreateTableAst);
        String tmpCciName = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);

//        TtlArchiveKind archiveKind = TtlArchiveKind.of(ttlInfo.getTtlInfoRecord().getArcKind());
        if (StringUtils.isEmpty(realCciName) && !StringUtils.isEmpty(tmpCciName)) {

            /**
             * If the prim Tbl has no any cci, then auto ignore the arc cci bound
             * <pre>
             *     Notice !!!
             *     If code come in here ,that means some wrong has gone on current polardbx-inst cluster
             *     or its replica polardbx-inst cluster,
             *     here just to handle for producing invalid meta data of ttl20
             * </pre>
             */

            /**
             * Auto reset the arcName & arcTmpName of ttlInfo
             */
            ttlInfo.getTtlInfoRecord().setArcTmpTblSchema(null);
            ttlInfo.getTtlInfoRecord().setArcTmpTblName(null);

            ttlInfo.getTtlInfoRecord().setArcTblSchema(null);
            ttlInfo.getTtlInfoRecord().setArcTblName(null);

            try {
                String ttlSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
                String ttlTbl = ttlInfo.getTtlInfoRecord().getTableName();
                String logMsg = String.format(
                    "The table `%s`.`%s` has no found any cci, so ignore bounding cci as the archive table `%s`.`%s`",
                    ttlSchema, ttlTbl, arcTblSchema, arcTblName);
                logger.warn(logMsg);
            } catch (Throwable e) {
                // ignore
            }

            return;
        }

        if (!StringUtils.equalsIgnoreCase(realCciName, tmpCciName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the real archive cci name `%s` is not the same as the auto-gen cci name `%s`",
                realCciName, tmpCciName));
        }

    }

    public static void validateTtlColExpr(TtlDefinitionInfo ttlInfo,
                                          TableMeta ttlTableMeta,
                                          ExecutionContext ec) {
        TtlMetaValidationUtil.validateTtlColExprAndPartInterval(ttlInfo, ttlTableMeta, ec);
        TtlMetaValidationUtil.validateTtlExpiredInterval(ttlInfo, ec);
        TtlMetaValidationUtil.validateTimeZoneExpr(ttlInfo.getTtlInfoRecord().getTtlTimezone(), ec);
    }

    public static void validateTtlCronExpr(TtlDefinitionInfo ttlInfo, ExecutionContext ec) {

        String ttlCronExpr = "";
        try {
            ttlCronExpr = ttlInfo.getTtlInfoRecord().getTtlCron();
            CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
            Cron cron = quartzCronParser.parse(ttlCronExpr);
            assert cron != null;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the ttl cron expression `%s` is invalid", ttlCronExpr), ex);
        }
    }

    public static void validateTtlFilterExpr(TtlDefinitionInfo ttlInfo,
                                             TableMeta ttlTableMeta,
                                             ExecutionContext ec) {
        String ttlFilterExpr = ttlInfo.getTtlInfoRecord().getTtlFilter();
        if (StringUtils.isEmpty(ttlFilterExpr)) {
            return;
        }
        boolean archiveByRow = ttlInfo.performArchiveByRow();
        if (!archiveByRow) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the ttl_filter '%s' is not supported in using partition/subpartition archive_type",
                ttlFilterExpr));
        }
        ByteString byteStrMySql = ByteString.from(ttlFilterExpr);
        MySqlExprParser mySqlExprParser = new MySqlExprParser(byteStrMySql);
        SQLExpr filterExpr = mySqlExprParser.expr();
        if (!(filterExpr instanceof SQLBinaryOpExpr)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                "Failed to create ttl definition because the ttl_filter '%s' is not a binary-op expression",
                ttlFilterExpr));
        }
    }

    public static void validateTtlArchiveType(TtlDefinitionInfo ttlInfo,
                                              TableMeta ttlTableMeta,
                                              PartitionInfo newCreatedTblPartInfo,
                                              SqlCreateTable sqlCreateTableAst,
                                              ExecutionContext ec) {
        boolean arcByRow = ttlInfo.performArchiveByRow();
        if (arcByRow) {
            /**
             * Validate local index of ttl_col ?
             */
            return;
        }

        boolean checkForNewCreateTableWithTtlDef = newCreatedTblPartInfo != null;
        boolean arcByPartOrSubPart = ttlInfo.performArchiveByPartitionOrSubPartition();
        PartitionInfo partInfo = ttlTableMeta.getPartitionInfo();
        if (partInfo == null || checkForNewCreateTableWithTtlDef) {
            partInfo = newCreatedTblPartInfo;
        }

        boolean containGsi = false;
        if (checkForNewCreateTableWithTtlDef) {
            List<Pair<SqlIdentifier, SqlIndexDefinition>> gsiKeyList = sqlCreateTableAst.getGlobalKeys();
            if (gsiKeyList != null) {
                containGsi = !gsiKeyList.isEmpty();
            }
        } else {
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiInfos = ttlTableMeta.getGsiPublished();
            if (gsiInfos != null) {
                containGsi = !gsiInfos.isEmpty();
            }
        }

        if (containGsi) {
            // add switch
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because a partition table with gsi is not supported for using partition/subpartition archive_type"));
        }

        if (arcByPartOrSubPart) {
            if (partInfo.isSingleTable() || partInfo.isBroadcastTable()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because single/broadcast table is not supported for using partition/subpartition archive_type "));
            }
        }

        boolean arcBySubPart = ttlInfo.performArchiveBySubPartition();
        PartitionByDefinition tarPartBy = null;
        PartitionByDefinition partBy = partInfo.getPartitionBy();
        if (arcBySubPart) {

            PartitionByDefinition subPartBy = partBy.getSubPartitionBy();
            if (subPartBy == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because the ttl table has no any subpartitions"));
            }
            if (!subPartBy.isUseSubPartTemplate()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because the ttl table is not templated subpartition tables"));
            }
            tarPartBy = subPartBy;

        } else {
            tarPartBy = partBy;
        }

        if (!(tarPartBy.getStrategy().isRange())) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the ttl table is not range partition/subpartition tables"));
        }

        List<ColumnMeta> partColMetas = tarPartBy.getPartitionFieldList();
        if (partColMetas.size() > 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because partition/subpartition archive_type is not allowed a partitioned table with multi part columns"));
        }
        ColumnMeta partColMeta = partColMetas.get(0);
        String partColName = partColMeta.getName();
        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        if (!ttlColName.equalsIgnoreCase(partColName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the ttl column `%s` is not a partition column in using partition/subpartition archive_type",
                    ttlColName));
        }

        PartitionIntFunction partFunc = tarPartBy.getPartIntFunc();
        if (partFunc != null) {
            SqlOperator op = partFunc.getSqlOperator();
            String funcName = op.getName();
            boolean isSupported =
                PartitionFunctionBuilder.isTimeBasedFamilyPartitionFunctionForPartitionTypeTtl(funcName);
            if (!isSupported) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because partition/subpartition archive_type is not allowed using a partition column with function expression %s",
                        funcName));
            }
        }
    }

    public static void validateTtlCleanup(TtlDefinitionInfo ttlInfo,
                                          TableMeta ttlTableMeta,
                                          SqlCreateTable sqlCreateTableAst,
                                          ExecutionContext ec) {
        // check for gsi
        boolean arcByRow = ttlInfo.performArchiveByRow();
        boolean cleanupEnabled = ttlInfo.isCleanupEnabled();

        boolean useNoExpirePolicy = ttlInfo.useUndefinedExpirePolicy();
        if (useNoExpirePolicy) {
            if (cleanupEnabled) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because undefined-expired policy is not allowed using ttl_cleanup='on'"));
            }
        }

        if (arcByRow) {
            return;
        }

        boolean checkForNewCreateTableWithTtlDef = sqlCreateTableAst != null;
        boolean containGsi = false;
        if (checkForNewCreateTableWithTtlDef) {
            List<Pair<SqlIdentifier, SqlIndexDefinition>> gsiKeyList = sqlCreateTableAst.getGlobalKeys();
            if (gsiKeyList != null) {
                containGsi = !gsiKeyList.isEmpty();
            }
        } else {
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiInfos = ttlTableMeta.getGsiPublished();
            if (gsiInfos != null) {
                containGsi = !gsiInfos.isEmpty();
            }
        }

        if (cleanupEnabled) {
            if (containGsi) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                    String.format(
                        "Failed to create ttl definition because partition/subpartition archive_type is not allowed using a partition column with function expression"));
            }
        }

    }

    public static void validateArchiveTableAllocateInfo(TtlDefinitionInfo ttlInfo,
                                                        TableMeta ttlTableMeta,
                                                        ExecutionContext ec) {
        int maxPartCntLimit = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (ec != null) {
            maxPartCntLimit = ec.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }
        int arcPreCount = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();
        if (arcPreCount <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the value %s of archive_table_pre_allocate must be more than 0",
                    arcPreCount));
        }
        if (arcPreCount > maxPartCntLimit) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the value %s of archive_table_pre_allocate is too large",
                    arcPreCount));
        }

        int arcPostCount = ttlInfo.getTtlInfoRecord().getArcPostPartCnt();
        if (arcPostCount <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the value %s of archive_table_post_allocate must be more than 0",
                    arcPostCount));
        }
        if (arcPostCount > maxPartCntLimit) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create ttl definition because the value %s of archive_table_post_allocate is too large",
                    arcPostCount));
        }
    }

    public static void validateTtlDefinition(TtlDefinitionInfo ttlInfo,
                                             TableMeta ttlTableMeta,
                                             PartitionInfo newCreatedTblPartInfo,
                                             SqlCreateTable sqlCreateTableAst,
                                             ExecutionContext ec) {
        TtlMetaValidationUtil.validateAllowedCreatingNewTtlInfo(ttlInfo, ttlTableMeta, newCreatedTblPartInfo, ec);
        TtlMetaValidationUtil.validateArchiveTableInfo(ttlInfo, ttlTableMeta, ec);
        TtlMetaValidationUtil.validateTtlArchiveType(ttlInfo, ttlTableMeta,
            newCreatedTblPartInfo, sqlCreateTableAst, ec);
        TtlMetaValidationUtil.validateArchiveCciInfo(ttlInfo, ttlTableMeta,
            newCreatedTblPartInfo, sqlCreateTableAst, ec);
        TtlMetaValidationUtil.validateAllowedBoundingArchiveTable(ttlInfo, ec,
            false, ttlInfo.getArchiveTableSchema(), ttlInfo.getArchiveTableName());
        TtlMetaValidationUtil.validateTtlColExpr(ttlInfo, ttlTableMeta, ec);
        TtlMetaValidationUtil.validateTtlCronExpr(ttlInfo, ec);
        TtlMetaValidationUtil.validateTtlCleanup(ttlInfo, ttlTableMeta, sqlCreateTableAst, ec);
        TtlMetaValidationUtil.validateTtlFilterExpr(ttlInfo, ttlTableMeta, ec);
        TtlMetaValidationUtil.validateArchiveTableAllocateInfo(ttlInfo, ttlTableMeta, ec);

    }

    public static void validateAllowedBoundingArchiveTable(TtlDefinitionInfo ttlInfo,
                                                           ExecutionContext ec,
                                                           boolean validateForCreateNewArcTable,
                                                           String newTarArcTblSchema,
                                                           String newTarArcTblName) {
        try {
            if (ttlInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                    "Failed to create table because the source table is not a ttl-defined table"));
            }

            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();

            String arcTblSchema = ttlInfo.getTtlInfoRecord().getArcTblSchema();
            String arcTblName = ttlInfo.getTtlInfoRecord().getArcTblName();

            String cciName = TtlUtil.buildArcTmpNameByArcTblName(newTarArcTblName);

            if (!StringUtils.isEmpty(arcTblSchema) || !StringUtils.isEmpty(arcTblName)) {
                if (validateForCreateNewArcTable) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create archive table because the source table `%s`.`%s` has bound the archive table `%s`.`%s`",
                        ttlTblSchema, ttlTblName, arcTblSchema, arcTblName));
                }
            }
            if (StringUtils.isEmpty(newTarArcTblSchema) || StringUtils.isEmpty(newTarArcTblName)) {
                return;
            }

            ViewManager viewMgr = OptimizerContext.getContext(newTarArcTblSchema).getViewManager();
            if (viewMgr != null) {
                SystemTableView.Row viewInfo = viewMgr.select(newTarArcTblName);
                if (viewInfo != null) {
                    String viewDefLowerCase = viewInfo.getViewDefinition().toLowerCase();
                    boolean containTtlName = viewDefLowerCase.contains(ttlTblName.toLowerCase());
                    boolean containCciName = viewDefLowerCase.contains(cciName.toLowerCase());

                    if (!(containTtlName && containCciName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                            "Failed to create archive table because the archive table view `%s`.`%s` already exists",
                            newTarArcTblSchema, newTarArcTblName));
                    }
                }
            }

            SchemaManager schemaManager = ec.getSchemaManager(newTarArcTblSchema);
            if (schemaManager != null) {
                TableMeta tableMeta = null;
                try {
                    tableMeta = schemaManager.getTable(newTarArcTblName);
                } catch (TableNotFoundException exception) {
                    tableMeta = null;
                }
                if (tableMeta != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create archive table because the archive table name `%s`.`%s` already exists",
                        newTarArcTblSchema, newTarArcTblName));
                }
            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }
    }

    public static void validateAllowedCreatingNewTtlInfo(TtlDefinitionInfo newTtlInfo,
                                                         TableMeta ttlTableMeta,
                                                         PartitionInfo newCreatedTblPartInfo,
                                                         ExecutionContext ec) {

        boolean buildTtlInfoForCreateNewTable = newCreatedTblPartInfo != null;
        try {

            String ttlTblSchema = newTtlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = newTtlInfo.getTtlInfoRecord().getTableName();

            if (!DbInfoManager.getInstance().isNewPartitionDb(ttlTblSchema)) {
                // error
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the drds-mode table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            boolean isSingleTbl = false;
            boolean isBroadcastTbl = false;
            boolean isLocalPartTbl = false;
            boolean isOssTbl = false;
            boolean isGsi = false;
            boolean isCci = false;

            if (buildTtlInfoForCreateNewTable) {
                isSingleTbl = newCreatedTblPartInfo.isSingleTable();
                isBroadcastTbl = newCreatedTblPartInfo.isBroadcastTable();
                isGsi = newCreatedTblPartInfo.isGsi();
                isCci = newCreatedTblPartInfo.isColumnar();

            } else {
                if (ttlTableMeta != null && ttlTableMeta.getPartitionInfo() != null) {
                    isSingleTbl = ttlTableMeta.getPartitionInfo().isSingleTable();
                    isBroadcastTbl = ttlTableMeta.getPartitionInfo().isBroadcastTable();
                    isLocalPartTbl = ttlTableMeta.getLocalPartitionDefinitionInfo() != null;
                    isGsi = ttlTableMeta.isGsi();
                    isCci = ttlTableMeta.isColumnar();
                }
            }
            isOssTbl = Engine.isFileStore(ttlTableMeta.getEngine());

            if (isSingleTbl) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the broadcast/single table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            if (isBroadcastTbl) {
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the broadcast/single table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            if (isGsi) {
                //err
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the global index table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            if (isCci) {
                // err
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the columnar index table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            if (isLocalPartTbl) {
                // err
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the local-partition table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }

            if (isOssTbl) {
                // err
                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
                    "Failed to create ttl definition because the oss table `%s`.`%s` is not allowed",
                    ttlTblSchema, ttlTblName));
            }
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex.getMessage(), ex);
        }

    }
}

