package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeToLiveExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.TtlEventLogUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;

import java.util.List;

public class TtlDefinitionInfo {

    /**
     * The ttl definition
     */
    protected TtlInfoRecord ttlInfoRecord = new TtlInfoRecord();

    /**
     * The func expr def for ttl-col from int-type to datetime-type
     */
    protected boolean ttlColUseFuncExpr = false;
    protected TtlColFuncExprInfo ttlColFuncExprInfo = null;

    public TtlDefinitionInfo() {
    }

    public boolean isEnableTtlSchedule() {
        return ttlInfoRecord.getTtlStatus() == TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE;
    }

    public boolean isTtlBinlogOpen() {
        return ttlInfoRecord.getTtlBinlog() == TtlInfoRecord.TTL_BINLOG_OPEN_BINLOG_DURING_CLEANING_DATA;
    }

    public boolean performArchiveByRow() {
        return ttlInfoRecord.getArcKind() == TtlInfoRecord.ARCHIVE_KIND_ROW;
    }

    public boolean performArchiveByPartition() {
        return ttlInfoRecord.getArcKind() == TtlInfoRecord.ARCHIVE_KIND_PARTITION;
    }

    public boolean performArchiveBySubPartition() {
        return ttlInfoRecord.getArcKind() == TtlInfoRecord.ARCHIVE_KIND_SUBPARTITION;
    }

    public boolean performArchiveByPartitionOrSubPartition() {
        return performArchiveByPartition() || performArchiveBySubPartition();
    }

    public boolean needPerformExpiredDataArchiving() {
        return !StringUtils.isEmpty(ttlInfoRecord.getArcTblName());
    }

    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.of(TtlTimeUnit.of(ttlInfoRecord.getTtlUnit()).getUnitName());
    }

    public ColumnMeta getTtlColMeta(ExecutionContext ec) {
        String ttlTblSchema = this.getTtlInfoRecord().getTableSchema();
        String ttlTblName = this.getTtlInfoRecord().getTableName();
        String ttlCol = this.getTtlInfoRecord().getTtlCol();
        TableMeta tableMeta = ec.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        ColumnMeta ttlCm = tableMeta.getColumn(ttlCol);
        return ttlCm;
    }

    public boolean useTimestampOnTtlCol(ExecutionContext ec) {
        return TtlUtil.useTimestampOnTtlCol(this, ec);
    }

    /**
     * Create TtlDefInfo from metadb
     */
    public static TtlDefinitionInfo createFrom(TtlInfoRecord ttlInfoRecord) {
        if (ttlInfoRecord == null) {
            return null;
        }
        TtlDefinitionInfo ttlDefinitionInfo = new TtlDefinitionInfo();
        ttlDefinitionInfo.setTtlInfoRecord(ttlInfoRecord);
        String ttlExprStr = ttlInfoRecord.getTtlExpr();
        try {
            ByteString ttlExprByteStr = ByteString.from(ttlExprStr);
            MySqlExprParser exprParser = new MySqlExprParser(ttlExprByteStr);
            SQLTimeToLiveExpr ttlExprAst =
                MySqlCreateTableParser.parseTimeToLiveExpr(exprParser, exprParser.getLexer());
            SQLExpr ttlColExpr = ttlExprAst.getColumn();

            FastSqlToCalciteNodeVisitor visitor =
                new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
            SqlNode ttlColExprAst = visitor.convertToSqlNode(ttlColExpr);

            TtlColFuncExprInfo funcExprInfo = TtlColFuncExprInfo.buildTtlColFuncExprInfoByTtlColAst(ttlColExprAst);
            ttlDefinitionInfo.setTtlColFuncExprInfo(funcExprInfo);
            ttlDefinitionInfo.setTtlColUseFuncExpr(funcExprInfo != null);

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                "Failed to init ttl func expr from ttl info record");
        }

        return ttlDefinitionInfo;
    }

    public static Integer getArcKindByEngine(String tableEngine) {
        if (StringUtils.isEmpty(tableEngine)) {
            return TtlInfoRecord.ARCHIVE_KIND_UNDEF;
        }
        if (tableEngine.equalsIgnoreCase(Engine.COLUMNAR.name())) {
            return TtlInfoRecord.ARCHIVE_KIND_ROW;
        } else {
            return TtlInfoRecord.ARCHIVE_KIND_UNDEF;
        }
    }

    public static TtlDefinitionInfo createNewTtlInfo(
        BuildTtlInfoParams buildParams,
        PartitionInfo newCreatedTblPartInfo,
        SqlCreateTable sqlCreateTableAst) {
        TtlDefinitionInfo newTtlInfo = createNewTtlInfoInner(buildParams, newCreatedTblPartInfo, sqlCreateTableAst);
        TtlInfoRecord ttlDefRec = newTtlInfo.getTtlInfoRecord();
        logEventForCreateTtlInfo(newCreatedTblPartInfo != null, ttlDefRec);
        return newTtlInfo;
    }

    public static TtlDefinitionInfo createNewTtlInfoInner(
        BuildTtlInfoParams buildParams,
        PartitionInfo newCreatedTblPartInfo,
        SqlCreateTable sqlCreateTableAst) {

        String tableSchema = buildParams.getTableSchema();
        String tableName = buildParams.getTableName();
        String ttlEnable = buildParams.getTtlEnable();
        SqlTimeToLiveExpr ttlExpr = buildParams.getTtlExpr();
        SqlTimeToLiveJobExpr ttlJob = buildParams.getTtlJob();
        String ttlFilter = buildParams.getTtlFilter();
        String ttlCleanup = buildParams.getTtlCleanup();
        SqlNode ttlPartInterval = buildParams.getTtlPartInterval();
        String archiveKind = buildParams.getArchiveKind();
        String archiveTableSchema = buildParams.getArchiveTableSchema();
        String archiveTableName = buildParams.getArchiveTableName();
        Integer arcPreAllocateCount = buildParams.getArcPreAllocateCount();
        Integer arcPostAllocateCount = buildParams.getArcPostAllocateCount();
        TableMeta ttlTableMeta = buildParams.getTtlTableMeta();
        ExecutionContext ec = buildParams.getEc();

        TtlDefinitionInfo oldTtlInfo = buildParams.getOldTtlInfo();
        boolean buildForModifyExistsTtl = oldTtlInfo != null;
        TtlInfoRecord oldTtlRec = null;
        TtlInfoRecord ttlDefRec = new TtlInfoRecord();
        if (buildForModifyExistsTtl) {
            oldTtlRec = oldTtlInfo.getTtlInfoRecord();
            ttlDefRec = oldTtlRec.copy();
        }
        //==== init ====
        ttlDefRec.setTtlBinlog(TtlInfoRecord.TTL_BINLOG_CLOSE_BINLOG_DURING_CLEANING_DATA);

        // ======== table_schema & table_name ========
        String tableSchemaVal = tableSchema;
        String tableNameVal = tableName;
        if (tableSchemaVal == null) {
            if (buildForModifyExistsTtl) {
                tableSchemaVal = oldTtlRec.getTableSchema();
            }
        }
        if (tableNameVal == null) {
            if (buildForModifyExistsTtl) {
                tableNameVal = oldTtlRec.getTableName();
            }
        }
        ttlDefRec.setTableSchema(tableSchemaVal);
        ttlDefRec.setTableName(tableNameVal);

        //====== ttl_filter =======
        String ttlFilterStrVal = null;
        if (ttlFilter != null) {
            ttlFilterStrVal = SQLUtils.normalizeNoTrim(ttlFilter.toString());
        } else {
            if (buildForModifyExistsTtl) {
                ttlFilterStrVal = oldTtlRec.getTtlFilter();
            }
        }
        ttlDefRec.setTtlFilter(ttlFilterStrVal);

        //==== archive_type ====
        boolean archiveByPart = false;
        boolean specifyPartInterval = false;
        int arcKind = TtlInfoRecord.ARCHIVE_KIND_ROW;

        TtlArchiveKind archiveKindVal = null;
        if (archiveKind != null) {
            archiveKindVal = TtlArchiveKind.of(archiveKind);
            arcKind = archiveKindVal.getArchiveKindCode();
            if (archiveKindVal.archivedByPartitions()) {
                archiveByPart = true;
            }
            if (archiveKindVal == TtlArchiveKind.UNDEFINED) {
                arcKind = TtlInfoRecord.ARCHIVE_KIND_ROW;
            }
        } else {
            if (buildForModifyExistsTtl) {
                arcKind = oldTtlRec.getArcKind();
                archiveByPart = oldTtlInfo.performArchiveByPartitionOrSubPartition();
                if (arcKind == TtlArchiveKind.UNDEFINED.getArchiveKindCode()) {
                    arcKind = TtlInfoRecord.ARCHIVE_KIND_ROW;
                }
            }
        }
        ttlDefRec.setArcKind(arcKind);

        if (ttlPartInterval != null) {
            specifyPartInterval = true;
        }

        //==== ttl_enable =====
        int ttlStatusVal = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE;
        if (!StringUtils.isEmpty(ttlEnable)) {
            if (ttlEnable.equalsIgnoreCase(TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE_STR_VAL)) {
                ttlStatusVal = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE;
            } else {
                ttlStatusVal = TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE;
            }
        } else {
            if (buildForModifyExistsTtl) {
                ttlStatusVal = oldTtlRec.getTtlStatus();
            }
        }
        ttlDefRec.setTtlStatus(ttlStatusVal);

        //==== ttl_cleanup options =====
        String ttlCleanupValStr = ttlCleanup;
        boolean enableCleanup = false;
        boolean modifiedCleanupVal = false;
        if (!StringUtils.isEmpty(ttlCleanupValStr)) {
            if (ttlCleanupValStr.equalsIgnoreCase(TtlInfoRecord.TTL_CLEANUP_ON)) {
                enableCleanup = true;
            }
            modifiedCleanupVal = true;
        }

        //======= ttl_expr & ttl_col ========
        TtlArchivePartMode expirePolicy = TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL;
        String ttlExprStrVal = null;
        Integer expireAfter = null;
        Integer expireAfterIntervalVal = TtlInfoRecord.TTL_EXPIRE_AFTER_UNDEFINED;
        String expireAfterUnit = null;
        Integer expireOver = null;
        String ttlExprTimezoneVal = null;
        Integer expireAfterTimeUnitCode = TtlTimeUnit.UNDEFINED.getUnitCode();
        String columnName = null;
        boolean ttlColUseFuncExpr = false;
        TtlColFuncExprInfo ttlColFuncExprInfo = null;

        if (ttlExpr != null) {

            ttlExprStrVal = ttlExpr.toString();

            //===== ttl_col of ttl_expr ========
            SqlNode ttlColNodeAst = ttlExpr.getColumn();
            TtlUtil.TtlColumnFinder ttlColumnFinder = new TtlUtil.TtlColumnFinder();
            SqlNode columnNodeAst = ttlExpr.getColumn();
            boolean findTtlCol = ttlColumnFinder.find(columnNodeAst);
            SqlIdentifier columnNode = null;
            if (findTtlCol) {
                columnNode = ttlColumnFinder.getTtlColumn();
                columnName = SQLUtils.normalize(columnNode.getLastName()).trim();

                boolean useFuncExprDef = ttlColumnFinder.ttlColUseFuncExpr();
                if (useFuncExprDef) {
                    SqlBasicCall ttlColFuncExpr = (SqlBasicCall) ttlColNodeAst;
                    ttlColFuncExprInfo = TtlColFuncExprInfo.buildTtlColFuncExprInfoByTtlColAst(ttlColFuncExpr);
                }
            }
            ttlColUseFuncExpr = ttlColFuncExprInfo != null;

            //===== ttl_time_zone of ttl_expr ========
            SqlNode timezone = ttlExpr.getTimezone();
            if (timezone != null) {
                SqlCharStringLiteral timezoneNode = (SqlCharStringLiteral) timezone;
                String ttlExprTimezoneValTmp = timezoneNode.toString();
                ttlExprTimezoneVal = SQLUtils.normalize(ttlExprTimezoneValTmp);
            } else {
                if (buildForModifyExistsTtl) {
                    ttlExprTimezoneVal = oldTtlRec.getTtlTimezone();
                } else {
                    ttlExprTimezoneVal = TtlInfoRecord.TTL_EXPR_EXPIRE_DEFAULT_TIME_ZONE;
                }
            }

            // ==== ttl expire interval & time unit ======
            SqlNumericLiteral expireAfterNode = (SqlNumericLiteral) ttlExpr.getExpireAfter();
            boolean useExpireAfter = false;
            boolean useExpireOver = false;
            if (expireAfterNode != null) {
                expireAfter = ((Long) expireAfterNode.getValue()).intValue();
                useExpireAfter = true;
                expireAfterIntervalVal = expireAfter;
            }
            SqlIdentifier unitNode = (SqlIdentifier) ttlExpr.getUnit();
            if (unitNode != null) {
                expireAfterUnit = SQLUtils.normalize(unitNode.getLastName());
                expireAfterTimeUnitCode = TtlTimeUnit.of(expireAfterUnit).getUnitCode();
            }

            // ==== ttl expire over ======
            SqlNumericLiteral expireOverNode = (SqlNumericLiteral) ttlExpr.getExpireOver();
            if (expireOverNode != null) {
                expireOver = ((Long) expireOverNode.getValue()).intValue();
                useExpireOver = true;
            }

            if (useExpireOver) {
                if (useExpireAfter) {
                    /**
                     * impossible come here
                     */
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS, String.format(
                        "Failed to create new ttl definition for table `%s`.`%s` because the ttl_expr definition is invalid",
                        tableSchema, tableName));
                } else {
                    /**
                     * Expired over policy
                     */
                    expirePolicy = TtlArchivePartMode.EXPIRE_OVER_PARTITION_COUNT;
                }
            } else {
                if (useExpireAfter) {
                    /**
                     * Expired after policy
                     */
                    expirePolicy = TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL;

                } else {
                    /**
                     * No define expire policy
                     */
                    expirePolicy = TtlArchivePartMode.UNDEFINED;
                }
            }

            if (expirePolicy == TtlArchivePartMode.EXPIRE_OVER_PARTITION_COUNT) {
                /**
                 * Expired over policy
                 */
                if (!archiveByPart) {
                    /**
                     * Row-Level
                     */
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                        String.format(
                            "Failed to create new ttl definition for table `%s`.`%s` because expire over policy is not supported for row-level archive_type",
                            tableSchema, tableName));
                }

                /**
                 * Partitions-level / SubPartitions-Level
                 */
                if (!specifyPartInterval) {
                    /**
                     * No found ttl_part_interval specified, so it is now allowed
                     */
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                        String.format(
                            "Failed to create new ttl definition for table `%s`.`%s` because the ttl_part_interval definition must be specified when using expire over policy",
                            tableSchema, tableName));
                }

            } else if (expirePolicy == TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL) {
                /**
                 * Expired after policy, ignore
                 */
            } else {

                /**
                 * No specify expire policy
                 */

                /**
                 * No specify expire policy, so ttl_cleanup is not allowed
                 */
                enableCleanup = false;
                modifiedCleanupVal = true;

                if (archiveByPart) {
                    /**
                     * Partitions-level / SubPartitions-Level
                     */
                    if (!specifyPartInterval) {
                        /**
                         * No found ttl_part_interval specified,
                         */
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                            String.format(
                                "Failed to create new ttl definition for table `%s`.`%s` because the ttl_part_interval definition must be specified when using no-expire policy",
                                tableSchema, tableName));
                    }
                } else {
                    /**
                     * Row-Level, ignore
                     */
                }
            }
        } else {
            if (buildForModifyExistsTtl) {
                ttlExprStrVal = oldTtlRec.getTtlExpr();
                columnName = oldTtlRec.getTtlCol();
                ttlColUseFuncExpr = oldTtlInfo.isTtlColUseFuncExpr();
                if (ttlColUseFuncExpr) {
                    ttlColFuncExprInfo = oldTtlInfo.getTtlColFuncExprInfo().copy();
                }
                ttlExprTimezoneVal = oldTtlRec.getTtlTimezone();
                expirePolicy = TtlArchivePartMode.of(oldTtlRec.getArcPartMode());
                expireAfterIntervalVal = oldTtlRec.getTtlInterval();
                expireAfterTimeUnitCode = oldTtlRec.getTtlUnit();

            } else {
                if (archiveByPart) {
                    if (specifyPartInterval) {
                        /**
                         * auto fill ttlExprStrVal by first part_col_name as
                         * ttl_expr=`part_col`
                         */
                        PartitionInfo partInfo = ttlTableMeta.getPartitionInfo();
                        PartitionByDefinition partBy = partInfo.getPartitionBy();
                        if (archiveKindVal == TtlArchiveKind.SUBPARTITION) {
                            partBy = partBy.getSubPartitionBy();
                        }
                        String partColName = partBy.getPartitionColumnNameList().get(0);
                        ttlExprStrVal = String.format("`%s`", partColName);
                        columnName = partColName;
                        enableCleanup = false;
                        modifiedCleanupVal = true;
                        ttlExprTimezoneVal = TtlInfoRecord.TTL_EXPR_EXPIRE_DEFAULT_TIME_ZONE;
                        ;
                        expirePolicy = TtlArchivePartMode.UNDEFINED;
                        expireAfterIntervalVal = TtlInfoRecord.TTL_EXPIRE_AFTER_UNDEFINED;
                        expireAfterTimeUnitCode = TtlTimeUnit.UNDEFINED.getUnitCode();
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                            String.format(
                                "Failed to create new ttl definition for table `%s`.`%s` because the ttl_part_interval of (sub)partition is not defined",
                                tableSchema, tableName));
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                        String.format(
                            "Failed to create new ttl definition for table `%s`.`%s` because the ttl_expr is not defined in row-level archive_type",
                            tableSchema, tableName));
                }
            }
        }
        ttlDefRec.setTtlExpr(ttlExprStrVal);
        ttlDefRec.setTtlCol(columnName);
        ttlDefRec.setTtlTimezone(ttlExprTimezoneVal);
        ttlDefRec.setArcPartMode(expirePolicy.getArcPartModeCode());
        ttlDefRec.setTtlUnit(expireAfterTimeUnitCode);
        ttlDefRec.setTtlInterval(expireAfterIntervalVal);

        //======= expire over policy===========
        /**
         * When using expireOver policy,
         * the arcPreAllocateCount is saved as expire over count
         */
        if (expireOver != null) {
            arcPreAllocateCount = expireOver;
        }

        //======= archive_table_pre_allocate_count / archive_table_post_allocate_count  ===========
        Integer arcPreAllocateCountVal = arcPreAllocateCount;
        if (arcPreAllocateCountVal == null) {
            if (buildForModifyExistsTtl) {
                arcPreAllocateCountVal = oldTtlRec.getArcPrePartCnt();
            } else {
                arcPreAllocateCountVal =
                    ec.getParamManager().getInt(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT);
            }
        }
        Integer arcPostAllocateCountVal = arcPostAllocateCount;
        if (arcPostAllocateCountVal == null) {
            if (buildForModifyExistsTtl) {
                arcPostAllocateCountVal = oldTtlRec.getArcPrePartCnt();
            } else {
                arcPostAllocateCountVal =
                    ec.getParamManager().getInt(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT);
            }
        }
        ttlDefRec.setArcPrePartCnt(arcPreAllocateCountVal);
        ttlDefRec.setArcPostPartCnt(arcPostAllocateCountVal);

        // ==== ttl_job ====
        String cronVal = ConnectionParams.DEFAULT_TTL_SCHEDULE_CRON_EXPR.getDefault();
        if (ttlJob != null) {
            SqlNode cronNode = ttlJob.getCron();
            cronVal = SQLUtils.normalize((cronNode).toString());
        } else {
            if (buildForModifyExistsTtl) {
                cronVal = oldTtlRec.getTtlCron();
            }
        }
        ttlDefRec.setTtlCron(cronVal);

        //========== ttl_cleanup status ============
        //==== arc_status =====
        int arcStatus = TtlInfoRecord.ARCHIVE_STATUS_UNDEF;
        if (buildForModifyExistsTtl) {
            arcStatus = oldTtlRec.getArcStatus();
        }
        ttlDefRec.setArcStatus(arcStatus);
        // some policy will force close ttl_cleanup, like no-expire policy
        boolean allowCleanupVal = enableCleanup;
        if (modifiedCleanupVal) {
            if (allowCleanupVal) {
                ttlDefRec.setBitValIntoArchiveStatus(
                    TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL, true);
            } else {
                ttlDefRec.setBitValIntoArchiveStatus(
                    TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL, false);
            }
        }

        // ==== ttl_part_interval =====
        TtlTimeUnit arcPartUnit = TtlTimeUnit.MONTH;
        Integer arcPartInterval = TtlConfigUtil.getIntervalOfPreBuildPartOfArcTbl();
        if (ttlPartInterval != null) {
            Pair<Integer, TtlTimeUnit> arcPartIntervalResult = fetchPartInterval(ttlPartInterval);
            if (arcPartIntervalResult != null) {
                arcPartInterval = arcPartIntervalResult.getKey();
                arcPartUnit = arcPartIntervalResult.getValue();
            }
        } else {
            if (buildForModifyExistsTtl) {
                arcPartInterval = oldTtlRec.getArcPartInterval();
                arcPartUnit = TtlTimeUnit.of(oldTtlRec.getArcPartUnit());
            } else {
                Pair<Integer, TtlTimeUnit> defaultArcPartIntervalResult =
                    TtlDefinitionInfo.calculateDefaultPartInterValAndTimeUnit(expireAfterIntervalVal,
                        expireAfterTimeUnitCode);
                arcPartInterval = defaultArcPartIntervalResult.getKey();
                arcPartUnit = defaultArcPartIntervalResult.getValue();
            }
        }
        ttlDefRec.setArcPartUnit(arcPartUnit.getUnitCode());
        ttlDefRec.setArcPartInterval(arcPartInterval);

        //====== archive_table_schema & archive_table_name / archive_tmp_table_schema & archive_tmp_table_name ======
        boolean isPerformUnbindingArcTblName = false;
        boolean isPerformBindingArcTblName = false;
        String arcTmpTblNameVal = null;
        String archiveTableNameVal = null;
        String oldArcTblName = "";
        if (buildForModifyExistsTtl) {
            oldArcTblName = oldTtlRec.getArcTblName();
        }
        if (archiveTableName != null) {
            /**
             * Come here , that means that alterModifyTtl specify a archiveTableName
             */
            if (buildForModifyExistsTtl) {
                /**
                 * Come here , that means that alterModifyTtl specify a archiveTableName on a exists ttl_info
                 */
                if (StringUtils.isEmpty(archiveTableName)) {
                    /**
                     * arcTblName has been specified as an empty str but original arcTableName is not empty,
                     * that means curr alterModifyTtl is unbinding the arcTableName
                     */
                    isPerformUnbindingArcTblName = !StringUtils.isEmpty(oldArcTblName);
                } else {
                    /**
                     * arcTblName has been specified as an non-empty str but original is empty,
                     * that means curr alterModifyTtl is binding arcTableName
                     */
                    isPerformBindingArcTblName = StringUtils.isEmpty(oldArcTblName);
                }
            } else {
                /**
                 * Come here , that means that alterModifyTtl specify a archiveTableName to add a new ttl_info
                 */
                if (StringUtils.isEmpty(archiveTableName)) {
                    /**
                     * arcTblName has been specified as an empty str,
                     * that means that curr alterModifyTtl is unbinding the arcTableName
                     */
                    isPerformUnbindingArcTblName = true;
                } else {
                    /**
                     * arcTblName has been specified as an non-empty str,
                     * that means that curr alterModifyTtl is binding arcTableName
                     */
                    isPerformBindingArcTblName = true;
                }
            }
            archiveTableNameVal = archiveTableName;
            if (isPerformBindingArcTblName) {
                arcTmpTblNameVal = TtlUtil.buildArcTmpNameByArcTblName(archiveTableName);
            } else {
                arcTmpTblNameVal = "";
            }
        } else {
            if (buildForModifyExistsTtl) {
                archiveTableNameVal = oldTtlRec.getArcTblName();
                arcTmpTblNameVal = oldTtlRec.getArcTmpTblName();
            }
        }

        String arcTmpTblSchemaVal = null;
        String archiveTableSchemaVal = null;
        if (archiveTableSchema != null) {
            if (buildForModifyExistsTtl) {
                /**
                 * Come here , that means that alterModifyTtl specify a archiveTableSchema
                 */
                if (isPerformBindingArcTblName) {
                    /**
                     * curr alterModifyTtl is binding arcTableName
                     */
                    if (StringUtils.isEmpty(archiveTableSchema)) {
                        /**
                         * arcTblName has been specified, but arcTblSchema has NOT been specified,
                         * so arcTblSchema use the ttlSchema as default value which is the same as tableSchema of ttl-tbl
                         */
                        archiveTableSchemaVal = tableSchema;
                    } else {
                        /**
                         * arcTblName has been specified, and the arcTblSchema has been specified,
                         */
                        archiveTableSchemaVal = archiveTableSchema;
                    }
                } else {
                    /**
                     * curr alterModifyTtl is unbinding a arcTableName
                     */
                    if (StringUtils.isEmpty(archiveTableSchema)) {
                        /**
                         * arcTblName is assigned to empty
                         * so arcTblSchema should be force empty
                         */
                        archiveTableSchemaVal = "";
                    } else {
                        /**
                         * arcTblName has been specified, and the arcTblSchema has been specified,
                         */
                        /**
                         * arcTblName is assigned to empty
                         * but the arcTblSchema is not empty,
                         * should NOT come here!!!,
                         * here the arcTblSchema should be force empty
                         */
                        archiveTableSchemaVal = "";
                    }
                }
            } else {
                /**
                 * Come here , that means that alterModifyTtl specify a archiveTableSchema to add a new ttl_info
                 */
                if (isPerformBindingArcTblName) {
                    if (StringUtils.isEmpty(archiveTableSchema)) {
                        /**
                         * arcTblName has been specified, but arcTblSchema has NOT been specified,
                         * so arcTblSchema use the ttlSchema as default value which is the same as tableSchema of ttl-tbl
                         */
                        archiveTableSchemaVal = tableSchema;
                    } else {
                        archiveTableSchemaVal = archiveTableSchema;
                    }
                } else {
                    /**
                     * arcTblName is assigned to empty
                     * so arcTblSchema should be force empty
                     */
                    archiveTableSchemaVal = "";
                }
            }
            arcTmpTblSchemaVal = archiveTableSchemaVal;
        } else {
            if (buildForModifyExistsTtl) {
                archiveTableSchemaVal = oldTtlRec.getArcTblSchema();
                arcTmpTblSchemaVal = oldTtlRec.getArcTmpTblSchema();
            } else {
                if (isPerformBindingArcTblName) {
                    archiveTableSchemaVal = tableSchema;
                    arcTmpTblSchemaVal = tableSchema;
                }
            }
        }
        ttlDefRec.setArcTblName(archiveTableNameVal);
        ttlDefRec.setArcTmpTblName(arcTmpTblNameVal);
        ttlDefRec.setArcTblSchema(archiveTableSchemaVal);
        ttlDefRec.setArcTmpTblSchema(arcTmpTblSchemaVal);

        ExtraFieldJSON extraJsonFld = new ExtraFieldJSON();
        ttlDefRec.setExtra(extraJsonFld);

        TtlDefinitionInfo newTtlInfoToBeReturn = null;
        newTtlInfoToBeReturn = new TtlDefinitionInfo();
        newTtlInfoToBeReturn.setTtlInfoRecord(ttlDefRec);
        newTtlInfoToBeReturn.setTtlColUseFuncExpr(ttlColUseFuncExpr);
        newTtlInfoToBeReturn.setTtlColFuncExprInfo(ttlColFuncExprInfo);

        TtlMetaValidationUtil.validateTtlDefinition(newTtlInfoToBeReturn, ttlTableMeta, newCreatedTblPartInfo,
            sqlCreateTableAst, ec);

        return newTtlInfoToBeReturn;
    }

    public static TtlDefinitionInfo buildModifiedTtlInfo(
        TtlDefinitionInfo oldTtlInfo,
        BuildTtlInfoParams modifyTtlInfoParams) {

        String tableSchema = modifyTtlInfoParams.getTableSchema();
        String tableName = modifyTtlInfoParams.getTableName();
        String ttlEnable = modifyTtlInfoParams.getTtlEnable();
        SqlTimeToLiveExpr ttlExpr = modifyTtlInfoParams.getTtlExpr();
        SqlTimeToLiveJobExpr ttlJob = modifyTtlInfoParams.getTtlJob();
        String ttlFilter = modifyTtlInfoParams.getTtlFilter();
        String ttlCleanup = modifyTtlInfoParams.getTtlCleanup();
        SqlNode ttlPartInterval = modifyTtlInfoParams.getTtlPartInterval();
        String archiveKind = modifyTtlInfoParams.getArchiveKind();
        String archiveTableSchema = modifyTtlInfoParams.getArchiveTableSchema();
        String archiveTableName = modifyTtlInfoParams.getArchiveTableName();
        Integer arcPreAllocateCount = modifyTtlInfoParams.getArcPreAllocateCount();
        Integer arcPostAllocateCount = modifyTtlInfoParams.getArcPostAllocateCount();
        TableMeta ttlTableMeta = modifyTtlInfoParams.getTtlTableMeta();
        ExecutionContext ec = modifyTtlInfoParams.getEc();

        BuildTtlInfoParams buildParams = new BuildTtlInfoParams();
        buildParams.setTableSchema(tableSchema);
        buildParams.setTableName(tableName);
        buildParams.setTtlTableMeta(ttlTableMeta);
        buildParams.setEc(ec);
        buildParams.setTtlEnable(ttlEnable);
        buildParams.setTtlExpr(ttlExpr);
        buildParams.setTtlJob(ttlJob);
        buildParams.setTtlFilter(ttlFilter);
        buildParams.setTtlCleanup(ttlCleanup);
        buildParams.setTtlPartInterval(ttlPartInterval);
        buildParams.setArchiveKind(archiveKind);
        buildParams.setArchiveTableSchema(archiveTableSchema);
        buildParams.setArchiveTableName(archiveTableName);
        buildParams.setArcPreAllocateCount(arcPreAllocateCount);
        buildParams.setArcPostAllocateCount(arcPostAllocateCount);
        buildParams.setOldTtlInfo(oldTtlInfo);

        TtlDefinitionInfo newTtlInfo = createNewTtlInfoInner(buildParams, null, null);
        logEventForModifyTtlInfo(oldTtlInfo, newTtlInfo);

        return newTtlInfo;
    }

    private static void logEventForCreateTtlInfo(boolean isFromCreateTbl, TtlInfoRecord ttlDefRec) {
        if (StringUtils.isEmpty(ttlDefRec.getArcTblName())) {
            TtlEventLogUtil.logCreateTtlDefinitionEvent(ttlDefRec.getTableSchema(), ttlDefRec.getTableName());
        } else {
            if (isFromCreateTbl) {
                TtlEventLogUtil.logCreateCciArchiveTableEvent(ttlDefRec.getTableSchema(), ttlDefRec.getTableName(),
                    ttlDefRec.getArcTblName());
            } else {
                TtlEventLogUtil.logCreateTtlDefinitionWithArchiveTableEvent(ttlDefRec.getTableSchema(),
                    ttlDefRec.getTableName(), ttlDefRec.getArcTblName());
            }
        }
    }

    private static void logEventForModifyTtlInfo(TtlDefinitionInfo oldTtlInfo, TtlDefinitionInfo newTtlInfoToBeReturn) {
        if (!StringUtils.isEmpty(newTtlInfoToBeReturn.getTtlInfoRecord().getArcTblName())) {
            if (StringUtils.isEmpty(oldTtlInfo.getTtlInfoRecord().getArcTblName())) {
                /**
                 * ArcTblName change from null to non-null
                 */
                TtlEventLogUtil.logCreateCciArchiveTableEvent(newTtlInfoToBeReturn.getTtlInfoRecord().getTableSchema(),
                    newTtlInfoToBeReturn.getTtlInfoRecord().getTableName(),
                    newTtlInfoToBeReturn.getTtlInfoRecord().getArcTblName());
            }
        } else {
            if (!StringUtils.isEmpty(oldTtlInfo.getTtlInfoRecord().getArcTblName())) {
                /**
                 * ArcTblName change from non-null to null
                 */
                TtlEventLogUtil.logDropCciArchiveTableEvent(newTtlInfoToBeReturn.getTtlInfoRecord().getTableSchema(),
                    newTtlInfoToBeReturn.getTtlInfoRecord().getTableName(),
                    oldTtlInfo.getTtlInfoRecord().getArcTblName());
            }
        }
    }

    /**
     * Fetch ttl_part_interval & time_unit from ttl_part_interval ast
     */
    protected static Pair<Integer, TtlTimeUnit> fetchPartInterval(SqlNode ttlPartInterval) {

        Integer arcPartInterval = null;
        TtlTimeUnit arcPartIntervalUnit = null;
        if (ttlPartInterval != null) {
            if (ttlPartInterval instanceof SqlCall) {
                SqlCall intervalCall = (SqlCall) ttlPartInterval;
                List<SqlNode> opList = intervalCall.getOperandList();
                if (opList.size() == 2) {
                    SqlNode opVal = opList.get(0);
                    SqlNode opUnit = opList.get(1);
                    if (opVal instanceof SqlNumericLiteral) {
                        SqlNumericLiteral partIntervalNumLiteral = (SqlNumericLiteral) opVal;
                        arcPartInterval = ((Long) partIntervalNumLiteral.getValue()).intValue();
                    }
                    if (opUnit instanceof SqlIntervalQualifier) {
                        SqlIntervalQualifier unitCode = (SqlIntervalQualifier) opUnit;
                        TimeUnit startUnit = unitCode.getStartUnit();
                        arcPartIntervalUnit = TtlTimeUnit.of(startUnit.name());
                    } else if (opUnit instanceof SqlIdentifier) {
                        SqlIdentifier unitCode = (SqlIdentifier) opUnit;
                        arcPartIntervalUnit = TtlTimeUnit.of(SQLUtils.normalizeNoTrim(unitCode.getLastName()));
                    }
                }
            }
        }
        if (arcPartInterval == null || arcPartIntervalUnit == null) {
            return null;
        }
        Pair<Integer, TtlTimeUnit> partIntervalResult = new Pair<>(arcPartInterval, arcPartIntervalUnit);
        return partIntervalResult;
    }

    public static Pair<Integer, TtlTimeUnit> calculateDefaultPartInterValAndTimeUnit(Integer expireAfterInterval,
                                                                                     Integer expireAfterUnitCode) {
        Integer arcPartInterval = 1;
        /**
         * The default artPartIntervalUnit is month,
         * if the expired after unit is day, then use day as default unit
         */
        TtlTimeUnit arcPartIntervalUnit = TtlTimeUnit.MONTH;
        if (expireAfterInterval != null && expireAfterUnitCode != null) {
            TtlTimeUnit expireAfterUnit = TtlTimeUnit.of(expireAfterUnitCode);
            if (expireAfterUnit == TtlTimeUnit.DAY) {
                arcPartIntervalUnit = expireAfterUnit;
            }
        }
        Pair<Integer, TtlTimeUnit> partIntervalResult = new Pair<>(arcPartInterval, arcPartIntervalUnit);
        return partIntervalResult;
    }

    public String buildShowCreateTableOptions() {
//
//        TTL_ENABLE = 'ON'
//        TTL_EXPR = `b` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00'
//        TTL_JOB = CRON '* * */1 * * ?' TIMEZONE '+00:00'
        /**
         * <pre>
         * TTL =
         * TTL_DEFINITION (
         *  TTL_ENABLE='ON',
         *  TTL_EXPR = `b` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00',
         *  TTL_JOB = CRON '* * * * * *' TIMEZONE '+00:00',
         *  ...
         *  )
         * </pre>
         */
        String options = "";
        if (ttlInfoRecord != null) {
            options += String.format("\nTTL = TTL_DEFINITION (");
            options += String.format("TTL_ENABLE = '%s', ",
                ttlInfoRecord.getTtlStatus() == TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE ? "ON" : "OFF");
            options += String.format("TTL_EXPR = %s, ", ttlInfoRecord.getTtlExpr());
            options += String.format("TTL_JOB = CRON '%s', ", ttlInfoRecord.getTtlCron(),
                ttlInfoRecord.getTtlTimezone());
            if (!StringUtils.isEmpty(ttlInfoRecord.getTtlFilter())) {
                options += String.format("TTL_FILTER = COND_EXPR(%s), ", ttlInfoRecord.getTtlFilter());
            }
            if (isCleanupEnabled()) {
                options += String.format("TTL_CLEANUP = 'ON', ");
            } else {
                options += String.format("TTL_CLEANUP = 'OFF', ");
            }
//            if (isTtlPartIntervalSpecified()) {
//                options += String.format("TTL_PART_INTERVAL = INTERVAL(%s,%s), ", ttlInfoRecord.getArcPartInterval(),
//                    TtlTimeUnit.of(ttlInfoRecord.getArcPartUnit()).getUnitName());
//            }
            options += String.format("TTL_PART_INTERVAL = INTERVAL(%s,%s), ", ttlInfoRecord.getArcPartInterval(),
                TtlTimeUnit.of(ttlInfoRecord.getArcPartUnit()).getUnitName());

            options += String.format("ARCHIVE_TYPE = '%s', ",
                TtlArchiveKind.of(ttlInfoRecord.getArcKind()).getArchiveKindStr());
            String ttlTblSchema = ttlInfoRecord.getTableSchema();
            String arcTblSchema = ttlInfoRecord.getArcTblSchema();
            String archTblName = ttlInfoRecord.getArcTblName();
            if (!StringUtils.isEmpty(archTblName) && !StringUtils.isEmpty(arcTblSchema)
                && !arcTblSchema.equalsIgnoreCase(ttlTblSchema)) {
                options += String.format("ARCHIVE_TABLE_SCHEMA = '%s', ",
                    ttlInfoRecord.getArcTblSchema() == null ? "" : ttlInfoRecord.getArcTblSchema());
            }
            options += String.format("ARCHIVE_TABLE_NAME = '%s', ",
                ttlInfoRecord.getArcTblName() == null ? "" : ttlInfoRecord.getArcTblName());
            options += String.format("ARCHIVE_TABLE_PRE_ALLOCATE = %s, ", ttlInfoRecord.getArcPrePartCnt());
            options += String.format("ARCHIVE_TABLE_POST_ALLOCATE = %s", ttlInfoRecord.getArcPostPartCnt());

            options += String.format(")");
        }
        return options;
    }

    public boolean alreadyBoundArchiveTable() {
        boolean res = false;
        if (!StringUtils.isEmpty(this.getTtlInfoRecord().getArcTblSchema())
            || !StringUtils.isEmpty(this.getTtlInfoRecord().getArcTblName())) {
            return false;
        }
        return res;
    }

    public TtlDefinitionInfo copy() {
        TtlInfoRecord newTtlRec = this.ttlInfoRecord.copy();

        TtlDefinitionInfo newTtlInfo = new TtlDefinitionInfo();
        newTtlInfo.setTtlInfoRecord(newTtlRec);

        newTtlInfo.setTtlColUseFuncExpr(this.ttlColUseFuncExpr);
        if (ttlColFuncExprInfo != null) {
            newTtlInfo.setTtlColFuncExprInfo(this.ttlColFuncExprInfo.copy());
        }

        return newTtlInfo;
    }

    public boolean isCleanupEnabled() {

        if (!isExpireIntervalSpecified()) {
            /**
             * If no define the expireAfterInterval,
             * then check if use expire by over
             */
            if (!useExpireOverPartitionsPolicy()) {
                return false;
            }
        }
        return this.getTtlInfoRecord()
            .getBitValFromArchiveStatus(TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL);
    }

    public boolean isTtlPartIntervalSpecified() {
        if (this.ttlInfoRecord.getArcPartInterval() >= 1L) {
            return true;
        }
        if (this.ttlInfoRecord.getTtlUnit() != this.ttlInfoRecord.getArcPartUnit()) {
            return true;
        }
        return false;
    }

    public boolean useExpireOverPartitionsPolicy() {
        TtlArchivePartMode arcPartMode = TtlArchivePartMode.of(this.ttlInfoRecord.getArcPartMode());
        return arcPartMode.isExpiredOverPartitionCount();
    }

    public boolean useExpireAfterTimeIntervalPolicy() {
        TtlArchivePartMode arcPartMode = TtlArchivePartMode.of(this.ttlInfoRecord.getArcPartMode());
        return arcPartMode.isExpiredAfterTimeInterval();
    }

    public boolean useUndefinedExpirePolicy() {
        TtlArchivePartMode arcPartMode = TtlArchivePartMode.of(this.ttlInfoRecord.getArcPartMode());
        return arcPartMode.isUndefinedExpirePolicy();
    }

    public boolean useNumberAsPartIntervalUnit() {
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(this.ttlInfoRecord.getArcPartUnit());
        return arcPartUnit == TtlTimeUnit.NUMBER;
    }

    public boolean isExpireIntervalSpecified() {
        if (TtlTimeUnit.of(this.ttlInfoRecord.getTtlUnit()) == TtlTimeUnit.UNDEFINED
            || this.ttlInfoRecord.getTtlInterval() == TtlInfoRecord.TTL_EXPIRE_AFTER_UNDEFINED) {
            return false;
        }
        return true;
    }

    public String getTmpTableName() {
        return ttlInfoRecord.getArcTmpTblName();
    }

    public String getTmpTableSchema() {
        return ttlInfoRecord.getArcTmpTblSchema();
    }

    public String getArchiveTableName() {
        return ttlInfoRecord.getArcTblName();
    }

    public String getArchiveTableSchema() {
        return ttlInfoRecord.getArcTblSchema();
    }

    public TtlInfoRecord getTtlInfoRecord() {
        return ttlInfoRecord;
    }

    public void setTtlInfoRecord(TtlInfoRecord ttlInfoRecord) {
        this.ttlInfoRecord = ttlInfoRecord;
    }

    public boolean isTtlColUseFuncExpr() {
        return ttlColUseFuncExpr;
    }

    public void setTtlColUseFuncExpr(boolean ttlColUseFuncExpr) {
        this.ttlColUseFuncExpr = ttlColUseFuncExpr;
    }

    public TtlColFuncExprInfo getTtlColFuncExprInfo() {
        return ttlColFuncExprInfo;
    }

    public void setTtlColFuncExprInfo(TtlColFuncExprInfo ttlColFuncExprInfo) {
        this.ttlColFuncExprInfo = ttlColFuncExprInfo;
    }
}

