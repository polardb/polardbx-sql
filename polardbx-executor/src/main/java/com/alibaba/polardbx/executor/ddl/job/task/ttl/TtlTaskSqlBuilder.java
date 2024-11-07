package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import org.apache.commons.lang.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TtlTaskSqlBuilder {

    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_YEAR = "'%Y-01-01 00:00:00'";
    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_MONTH = "'%Y-%m-01 00:00:00'";
    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_DAY = "'%Y-%m-%d 00:00:00'";
    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_HOUR = "'%Y-%m-%d %H:00:00'";
    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_MINUTE = "'%Y-%m-%d %H:%i:00'";
    protected static final String ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_SECOND = "'%Y-%m-%d %H:%i:%s'";

    public static final DateTimeFormatter ISO_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected static final String PART_NAME_FORMAT_ON_UNIT_YEAR = "yyyy";
    protected static final String PART_NAME_FORMAT_ON_UNIT_MONTH = "yyyyMM";
    protected static final String PART_NAME_FORMAT_ON_UNIT_DAY = "yyyyMMdd";
    protected static final String PART_NAME_FORMAT_ON_UNIT_HOUR = "yyyyMMddtHH";
    protected static final String PART_NAME_FORMAT_ON_UNIT_MINUTE = "yyyyMMddtHHmm";
    protected static final String PART_NAME_FORMAT_ON_UNIT_SECOND = "yyyyMMddtHHmmss";

    protected static final String SUBJOB_STMT_TEMP = "SElECT '%s';";

    public static final String COL_NAME_FOR_SELECT_TTL_COL_LOWER_BOUND = "expired_lower_bound";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_UPPER_BOUND = "expired_upper_bound";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE = "min_value";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_NULL = "is_null";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_ZERO = "is_zero";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_CURRENT_DATETIME = "current_datetime";
    public static final String COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME = "formated_current_datetime";

    public static final String COL_NAME_FOR_SELECT_PARTITION_DATA_FREE = "all_data_free";
    public static final String COL_NAME_FOR_SELECT_PARTITION_DATA_LENGTH = "all_data_length";
    public static final String COL_NAME_FOR_SELECT_PARTITION_TABLE_ROWS = "all_table_rows";
    public static final String COL_NAME_FOR_SELECT_UNIX_TIMESTAMP_FOR_BOUND = "bound_ts";

    public static final String COL_NAME_FOR_DDL_JOB_ID = "job_id";
    public static final String DDL_FLAG_OF_TTL_JOB = "from ttl job";

    public static final String ARC_TBL_FIRST_PART_NAME = "pstart";
    public static final String ARC_TBL_MAXVAL_PART_NAME = "pmax";
    public static final String ARC_TBL_FIRST_PART_BOUND = "1970-01-02 00:00:00";
    public static final String ARC_TBL_FIRST_PART_BOUND_FOR_TS =
        String.format("UNIX_TIMESTAMP('%s')", ARC_TBL_FIRST_PART_BOUND);
    public static final String ARC_TBL_MAXVALUE_BOUND = "MAXVALUE";

    /**
     * The default lower bound for null value or zero value of ttl_col_min_val
     */
    public static final String TTL_COL_MIN_VAL_DEFAULT_LOWER_BOUND = "1971-01-01 00:00:00";

    public static final int MAX_TTL_TMP_NAME_PREFIX_LENGTH = 44;
    /**
     * arc_tmp name format (maxlen 60)：arctmp_(7) + %s(ttl_tbl_prefix,max 44) + _%s(hashvalstr, 9)
     */
    public static final String ARC_TMP_NAME_TEMPLATE_WITH_HASHCODE = "arctmp_%s_%s";
    public static final String ARC_TMP_NAME_TEMPLATE_WITHOUT_HASHCODE = "arctmp_%s";

    /**
     * the name format of tg name of arc_tmp (maxlen: 63)：tg_(3) + arctmp_%s(60)
     */
    private static final String ARC_TMP_TG_NAME_TEMPLATEE = "tg_%s";

    public static final String ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE =
        "alter /* from ttl scheduled job */ table `%s`.`%s` cleanup expired data;";

    public static final String ASYNC_ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE =
        "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/ alter /* from ttl scheduled job */ table `%s`.`%s` cleanup expired data;";

    public static final String ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE_KEYWORD = "cleanup expired data";

    public static final String ALTER_TABLE_MODIFY_TTL_FOR_BIND_ARCHIVE_TABLE =
        "alter table `%s`.`%s` modify ttl set archive_type = '%s', archive_table_schema = '%s', archive_table_name = '%s';";

    public static final String SELECT_TTL_COL_MIN_VAL_AND_LOWER_BOUND_SQL_TEMP_FORMAT =
        "SELECT ttl_col AS min_value, DATE_FORMAT(ttl_col,%s) AS expired_lower_bound, ttl_col is null is_null, ttl_col = '0000-00-00 00:00:00.000000' is_zero FROM (SELECT %s as ttl_col FROM %s.%s %s ORDER BY %s ASC LIMIT 1) as ttl_tbl;";

    public static final String SELECT_TTL_COL_UPPER_BOUND_SQL_TEMP_FORMAT =
        "SELECT %s as current_datetime, DATE_FORMAT( %s, %s ) as formated_current_datetime, DATE_FORMAT( DATE_SUB(%s, INTERVAL %s %s), %s ) as expired_upper_bound;";

    /**
     * <pre>
     * SELECT
     *  DATE_FORMAT(ttl_col,'%Y-%m-%d') as day,
     *  COUNT(1) rows_cnt
     * FROM tbl partition(?)
     * WHERE ttl_col < ?
     * GROUP BY day
     * ORDER BY day asc;
     * </pre>
     */
    public static String buildSelectForStatTemplate(TableMeta tableMeta) {
        LocalPartitionDefinitionInfo ttlDefInfo = tableMeta.getLocalPartitionDefinitionInfo();
        String ttlCol = ttlDefInfo.getColumnName();
        String tblName = tableMeta.getTableName();
        String dateFormatFuncTemp = "%Y-%m-%d";
        String statSqlProjFormat = String.format("DATE_FORMAT(`%s`,'%s') as day", ttlCol, dateFormatFuncTemp);

        String statSqlTemplateFormat =
            "SELECT %s FROM `%s` partition(?) WHERE `%s` < '?' GROUP BY day ORDER BY day ASC";
        String statSqlTemplate = String.format(statSqlTemplateFormat, statSqlProjFormat, tblName, ttlCol);
        statSqlTemplate = statSqlTemplate.replace("?", "%s");
        return statSqlTemplate;
    }

    public static String buildSelectForStatSql(TableMeta tableMeta, String partName, String expireAfterExprStr) {
        LocalPartitionDefinitionInfo ttlDefInfo = tableMeta.getLocalPartitionDefinitionInfo();
        String ttlCol = ttlDefInfo.getColumnName();
        String tblName = tableMeta.getTableName();
        String dateFormatFuncTemp = "%Y-%m-%d";
        String statSqlProjFormat = String.format("DATE_FORMAT(`%s`,'%s') as day", ttlCol, dateFormatFuncTemp);
        String statSqlTemplateFormat =
            "SELECT %s, count(1) rows_cnt FROM `%s` partition(%s) WHERE `%s` <= '%s' GROUP BY day ORDER BY day ASC";
        String statSqlTemplate =
            String.format(statSqlTemplateFormat, statSqlProjFormat, tblName, partName, ttlCol, expireAfterExprStr);

        return statSqlTemplate;
    }

    /**
     * <pre>
     * SELECT {all_columns_of_pk_and_sk}, ttl_col
     * FROM {physical_primary_table} FORCE INDEX (xxx)
     * WHERE (ttl_col) <= (?)
     *      [ AND  (ttl_col) >= (?) ]
     * ORDER BY ttl_col ASC
     * </pre>
     */
    public static String buildSelectForFetchPkAndSkTemplate(TableMeta tableMeta) {

        LocalPartitionDefinitionInfo ttlDefInfo = tableMeta.getLocalPartitionDefinitionInfo();
        String ttlCol = ttlDefInfo.getColumnName();
        Set<String> pkColSet = tableMeta.getPrimaryKeyMap().keySet();
        List<String> selectCols = new ArrayList<>();
        selectCols.addAll(pkColSet);
        selectCols.add(ttlCol);
        String tblName = tableMeta.getTableName();
        StringBuilder projectPart = new StringBuilder("");
        for (int i = 0; i < selectCols.size(); i++) {
            if (i > 0) {
                projectPart.append(",");
            }
            projectPart.append(selectCols.get(i));
        }
        String filterPart = String.format(" %s <= ? AND %s >= ? ", ttlCol, ttlCol);

        String selectSqlTemp =
            String.format("SELECT %s FROM %s PARTITION(?) WHERE %s ORDER BY %s ASC LIMIT ?,?;", projectPart, tblName,
                filterPart, ttlCol);
        selectSqlTemp.replace("?", "%s");
        return selectSqlTemp;
    }

    /**
     * Select the round-downed upper bound of expired value of ttl_col
     * <pre>
     * set TIME_ZONE='xxx';
     * SELECT
     *      NOW() as current_datetime,
     *      DATE_FORMAT( NOW(), formatter ) as formated_current_datetime,
     *      DATE_FORMAT( DATE_SUB(NOW(), INTERVAL %s %s), formatter ) as expired_upper_bound,
     * ;
     * formatter is like %Y-%m-%d 00:00:00.000000
     * </pre>
     */
    public static String buildSelectExpiredUpperBoundValueSqlTemplate(TtlDefinitionInfo ttlDefInfo,
                                                                      ExecutionContext ec) {

        String ttlTblSchema = ttlDefInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlDefInfo.getTtlInfoRecord().getTableName();
        String ttlCol = ttlDefInfo.getTtlInfoRecord().getTtlCol();
        String ttlUnit = TtlTimeUnit.of(ttlDefInfo.getTtlInfoRecord().getTtlUnit()).getTimeUnitName();
        Integer ttlInterval = ttlDefInfo.getTtlInfoRecord().getTtlInterval();
        String formatter = fetchRoundDownFormatter(ec, ttlTblSchema, ttlTblName, ttlCol, ttlUnit);
        String nowFunExpr = "NOW()";
        String debugCurrDateTime = ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_CURRENT_DATETIME);
        if (!StringUtils.isEmpty(debugCurrDateTime)) {
            nowFunExpr = String.format("'%s'", debugCurrDateTime);
        }
        String selectMinTtlColValueSqlTemp = String.format(
            TtlTaskSqlBuilder.SELECT_TTL_COL_UPPER_BOUND_SQL_TEMP_FORMAT,
            nowFunExpr, nowFunExpr, formatter, nowFunExpr, ttlInterval, ttlUnit, formatter);
        return selectMinTtlColValueSqlTemp;
    }

    protected static String fetchRoundDownFormatter(ExecutionContext ec,
                                                    String ttlTblSchema,
                                                    String ttlTblName,
                                                    String ttlCol,
                                                    String ttlUnit) {
        TableMeta tableMeta = ec.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        ColumnMeta ttlCm = tableMeta.getColumn(ttlCol);
        DataType dt = ttlCm.getDataType();

        boolean containTimeInfo = DataTypeUtil.isFractionalTimeType(dt);
//        boolean timezoneDependent = DataTypeUtil.isTimezoneDependentType(dt);
        boolean isDateType = DataTypeUtil.isDateType(dt);
        boolean isIntType = DataTypeUtil.isUnderBigintUnsignedType(dt);
        boolean isStrType = DataTypeUtil.isStringSqlType(dt);

        String formatter = "";
        if (isDateType) {
            if (containTimeInfo) {
                if (ttlUnit.equalsIgnoreCase(TtlInfoRecord.TTL_UNIT_YEAR)) {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_YEAR;
                } else if (ttlUnit.equalsIgnoreCase(TtlInfoRecord.TTL_UNIT_MONTH)) {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_MONTH;
                } else if (ttlUnit.equalsIgnoreCase(TtlInfoRecord.TTL_UNIT_DAY)) {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_DAY;
                } else if (ttlUnit.equalsIgnoreCase(TtlInfoRecord.TTL_UNIT_HOUR)) {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_HOUR;
                } else if (ttlUnit.equalsIgnoreCase(TtlInfoRecord.TTL_UNIT_MINUTE)) {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_MINUTE;
                } else {
                    formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_SECOND;
                }
            } else {
                formatter = TtlTaskSqlBuilder.ROUND_DOWN_DATETIME_FORMAT_ON_UNIT_DAY;
            }
        } else {
            if (isIntType || isStrType) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
            }
        }
        return formatter;
    }

    /**
     * Select the min value & lower bound value of ttl_col of ttl_tbl
     * <pre>
     * SELECT ttl_col AS min_value, DATE_FORMAT(ttl_col,formatter) AS expired_lower_bound
     *  FROM (SELECT %s as ttl_col
     *         FROM [db_name.]ttl_tbl [FORCE INDEX(xxx_ttl_col_idx)]
     *         ORDER BY %s ASC
     *         LIMIT 1
     *       ) as ttl_tbl
     * </pre>
     */
    public static String buildSelectExpiredLowerBoundValueBySqlTemplateWithoutConcurrent(TtlDefinitionInfo ttlDefInfo,
                                                                                         ExecutionContext ec,
                                                                                         int mergeUnionSize,
                                                                                         String forceIndexExpr) {
        String ttlTblSchema = ttlDefInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlDefInfo.getTtlInfoRecord().getTableName();
        String ttlCol = ttlDefInfo.getTtlInfoRecord().getTtlCol();
        String ttlUnit = TtlTimeUnit.of(ttlDefInfo.getTtlInfoRecord().getTtlUnit()).getTimeUnitName();
        String formatter = fetchRoundDownFormatter(ec, ttlTblSchema, ttlTblName, ttlCol, ttlUnit);
        String queryHint =
            "/*+TDDL:cmd_extra(MERGE_UNION=true, MERGE_UNION_SIZE=%s, MERGE_CONCURRENT=false, PREFETCH_SHARDS=1, SEQUENTIAL_CONCURRENT_POLICY=true)*/";
        String selectExpiredLownerBoundValueSqlTemp = String.format(
            queryHint + TtlTaskSqlBuilder.SELECT_TTL_COL_MIN_VAL_AND_LOWER_BOUND_SQL_TEMP_FORMAT,
            mergeUnionSize, formatter, ttlCol, ttlTblSchema, ttlTblName, forceIndexExpr, ttlCol);
        return selectExpiredLownerBoundValueSqlTemp;
    }

    public static String buildSelectExpiredLowerBoundValueSqlTemplate(TtlDefinitionInfo ttlDefInfo,
                                                                      ExecutionContext ec,
                                                                      String queryHint,
                                                                      String forceIndexExpr) {
        String ttlTblSchema = ttlDefInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlDefInfo.getTtlInfoRecord().getTableName();
        String ttlCol = ttlDefInfo.getTtlInfoRecord().getTtlCol();
        String ttlUnit = TtlTimeUnit.of(ttlDefInfo.getTtlInfoRecord().getTtlUnit()).getTimeUnitName();
        String formatter = fetchRoundDownFormatter(ec, ttlTblSchema, ttlTblName, ttlCol, ttlUnit);
        String queryHintFormatPart = "%s ";
        String selectExpiredLownerBoundValueSqlTemp = String.format(
            queryHintFormatPart + TtlTaskSqlBuilder.SELECT_TTL_COL_MIN_VAL_AND_LOWER_BOUND_SQL_TEMP_FORMAT,
            queryHint, formatter, ttlCol, ttlTblSchema, ttlTblName, forceIndexExpr, ttlCol);
        return selectExpiredLownerBoundValueSqlTemp;
    }

    public static String buildInsertSelectTemplate(TtlDefinitionInfo ttlDefInfo,
                                                   boolean needAddIntervalLowerBound,
                                                   String queryHint) {
        String ttlTblSchema = ttlDefInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlDefInfo.getTtlInfoRecord().getTableName();

        String ttlTmpTblSchema = ttlDefInfo.getTtlInfoRecord().getArcTmpTblSchema();
        String ttlTmpTblName = ttlDefInfo.getTtlInfoRecord().getArcTmpTblName();

        String ttlCol = ttlDefInfo.getTtlInfoRecord().getTtlCol();
        String filterPart = "";
        if (needAddIntervalLowerBound) {
            filterPart = String.format(" %s < '?' and %s >= '?' ", ttlCol, ttlCol);
        } else {
            filterPart = String.format("( %s < '?' or %s is null)", ttlCol, ttlCol);
        }

        String pkColsStr = "";
        List<String> pkColumns = ttlDefInfo.getPkColumns();
        for (int i = 0; i < pkColumns.size(); i++) {
            String pkCol = pkColumns.get(i).toLowerCase();
            if (!pkColsStr.isEmpty()) {
                pkColsStr += ",";
            }
            pkColsStr += pkCol;
        }

        String insertSelectSqlTemp = String.format(
            "%s INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s` PARTITION(?) WHERE %s ORDER BY %s,%s ASC LIMIT ?;",
            queryHint, ttlTmpTblSchema, ttlTmpTblName, ttlTblSchema, ttlTblName, filterPart, ttlCol, pkColsStr);
        String finalInsertSelectSqlTemp = insertSelectSqlTemp.replace("?", "%s");
        return finalInsertSelectSqlTemp;
    }

    public static String buildDeleteTemplate(TtlDefinitionInfo ttlDefInfo,
                                             boolean needAddIntervalLowerBound,
                                             String queryHint,
                                             String forceIndexExpr) {
        String ttlTblSchema = ttlDefInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlDefInfo.getTtlInfoRecord().getTableName();

        String ttlCol = ttlDefInfo.getTtlInfoRecord().getTtlCol();
        String filterPart = "";

        if (needAddIntervalLowerBound) {
            filterPart = String.format(" %s < '?' and %s >= '?' ", ttlCol, ttlCol);
        } else {
            filterPart = String.format(" (%s < '?' or %s is null) ", ttlCol, ttlCol);
        }

//        String pkColsStr = "";
//        List<String> pkColumns = ttlDefInfo.getPkColumns();
//        for (int i = 0; i < pkColumns.size(); i++) {
//            String pkCol = pkColumns.get(i).toLowerCase();
//            if (!pkColsStr.isEmpty()) {
//                pkColsStr += ",";
//            }
//            pkColsStr += pkCol;
//        }

        String deleteSqlTemp =
            String.format("%s DELETE FROM `%s`.`%s` PARTITION(?) %s WHERE %s ORDER BY %s ASC LIMIT ?;",
                queryHint, ttlTblSchema, ttlTblName, forceIndexExpr, filterPart, ttlCol);
        String finalDeleteSqlTemp = deleteSqlTemp.replace("?", "%s");
        return finalDeleteSqlTemp;
    }

    public static String buildAddPartitionsSqlTemplate(TtlDefinitionInfo ttlInfo,
                                                       List<String> newAddRangeBounds,
                                                       List<String> newAddRangePartNames,
                                                       String maxValPartName,
                                                       String queryHint,
                                                       ExecutionContext executionContext) {

        String partBoundDefs = "";
        List<String> normalizedNewAddPartBoundList =
            normalizedRangePartBoundValueList(ttlInfo, newAddRangeBounds, executionContext);
        for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
            String bndStr = normalizedNewAddPartBoundList.get(i);
            String partNameStr = newAddRangePartNames.get(i);
            String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
            String part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
            if (!partBoundDefs.isEmpty()) {
                partBoundDefs += ",\n";
            }
            partBoundDefs += part;
        }
        String addPartSpecSql = "";
        if (StringUtils.isEmpty(maxValPartName)) {
            addPartSpecSql = String.format("ADD PARTITION ( \n%s );",
                partBoundDefs);
        } else {
            addPartSpecSql = String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) );",
                maxValPartName, partBoundDefs, maxValPartName);
        }

        String skipDdlTasks =
            executionContext.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
        if (!StringUtils.isEmpty(skipDdlTasks)) {
            queryHint = addCciHint(queryHint, String.format("SKIP_DDL_TASKS=\"%s\"", skipDdlTasks));
        }

//        String addPartSqlTemp = queryHint + " ALTER TABLE %s " + addPartSpecSql;
        String addPartSqlTemp = queryHint + " ALTER INDEX %s ON TABLE %s " + addPartSpecSql;
        return addPartSqlTemp;
    }

    public static String addCciHint(String originalString, String... newParams) {
        StringBuilder paramBuilder = new StringBuilder();
        for (String param : newParams) {
            if (paramBuilder.length() > 0) {
                paramBuilder.append(",");
            }
            paramBuilder.append(param);
        }
        String newParam = paramBuilder.toString();

        // 使用正则表达式替换原始字符串
        Pattern pattern = Pattern.compile("/\\*\\+TDDL:cmd_extra\\((.*?)\\)\\*/");
        Matcher matcher = pattern.matcher(originalString);
        if (matcher.find()) {
            String existingParams = matcher.group(1);
            String modifiedString = originalString.replace(matcher.group(),
                "/*+TDDL:cmd_extra(" + existingParams + (existingParams.isEmpty() ? "" : ",") + newParam + ")*/");
            return modifiedString;
        } else {
            return originalString;
        }
    }

    public static String buildAddPartitionsByTemplateAndTableName(String ttlSchemaName,
                                                                  String ttlTblName,
                                                                  String arcCciIndexName,
                                                                  String addPartSqlTemp) {

        String dbNameStr = SqlIdentifierUtil.escapeIdentifierString(ttlSchemaName);
        String tbNameStr = SqlIdentifierUtil.escapeIdentifierString(ttlTblName);
        String cciNameStr = SqlIdentifierUtil.escapeIdentifierString(arcCciIndexName);
        String dbAndTb = String.format("%s.%s", dbNameStr, tbNameStr);
        String addPartsSql = String.format(addPartSqlTemp, cciNameStr, dbAndTb);
        return addPartsSql;
    }

    public static String buildPartBoundsSqlTemplateForColumnarIndex(TtlDefinitionInfo ttlInfo,
                                                                    List<String> newAddRangeBounds,
                                                                    List<String> newAddRangePartNames,
                                                                    ExecutionContext executionContext) {

        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        String partBoundDefs = "";

        List<String> newTmpAddRangeBounds = new ArrayList<>();
        newTmpAddRangeBounds.add(TtlTaskSqlBuilder.ARC_TBL_FIRST_PART_BOUND);
        newTmpAddRangeBounds.addAll(newAddRangeBounds);

        List<String> newTmpAddRangePartNames = new ArrayList<>();
        newTmpAddRangePartNames.add(TtlTaskSqlBuilder.ARC_TBL_FIRST_PART_NAME);
        newTmpAddRangePartNames.addAll(newAddRangePartNames);

        List<String> normalizedNewAddPartBoundList =
            normalizedRangePartBoundValueList(ttlInfo, newTmpAddRangeBounds, executionContext);
        for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
            String bndStr = normalizedNewAddPartBoundList.get(i);
            String partNameStr = newTmpAddRangePartNames.get(i);
            String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
            String part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
            if (!partBoundDefs.isEmpty()) {
                partBoundDefs += ",\n";
            }
            partBoundDefs += part;
        }
        String partByDefStr = buildPartByDef(ttlInfo, ttlColName, executionContext);
        String partBySql = String.format("%s ( %s )", partByDefStr, partBoundDefs);
        return partBySql;
    }

    protected static String buildPartByDef(TtlDefinitionInfo ttlInfo, String ttlColName,
                                           ExecutionContext executionContext) {
        boolean useTsOnTtlCol = ttlInfo.useTimestampOnTtlCol(executionContext);
        String partByDefStr = "";
        if (useTsOnTtlCol) {
            partByDefStr = String.format("PARTITION BY RANGE (UNIX_TIMESTAMP(`%s`))", ttlColName);
        } else {
            partByDefStr = String.format("PARTITION BY RANGE COLUMNS(`%s`)", ttlColName);
        }
        return partByDefStr;
    }

    protected static List<String> normalizedRangePartBoundValueList(TtlDefinitionInfo ttlInfo,
                                                                    List<String> rangeBounds,
                                                                    ExecutionContext executionContext) {
        boolean useTsOnTtlCol = ttlInfo.useTimestampOnTtlCol(executionContext);
        String ttlTzStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        List<String> result = new ArrayList<>();
        for (int i = 0; i < rangeBounds.size(); i++) {
            String bndStr = rangeBounds.get(i);
            boolean isMaxValBnd = bndStr.toUpperCase().contains(TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
            boolean partBndNeedUseFuncExpr = useTsOnTtlCol;
            String normalizedPartBndVal = "";
            if (!isMaxValBnd) {
                if (!partBndNeedUseFuncExpr) {
                    normalizedPartBndVal = String.format("'%s'", bndStr);
                } else {
                    normalizedPartBndVal = convertToUnixTimestampLongStr(bndStr, ttlInfo);
                }
            } else {
                normalizedPartBndVal = bndStr;
            }
            result.add(normalizedPartBndVal);
        }
        return result;
    }

    public static String convertToUnixTimestampLongStr(String bndStr,
                                                       TtlDefinitionInfo ttlInfo) {
        String timezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        ZoneId zoneId = ZoneId.of(timezoneStr);
        LocalDateTime localDateTime = LocalDateTime.parse(bndStr, ISO_DATETIME_FORMATTER);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
        Long unixTs = zonedDateTime.toEpochSecond();
        String unixTsStr = String.valueOf(unixTs);
        return unixTsStr;
    }

    public static String getPartNameFormatterPatternByTtlUnit(TtlTimeUnit ttlUnit) {
        String formatter = null;

        switch (ttlUnit) {
        case YEAR:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_YEAR;
            break;
        case MONTH:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_MONTH;
            break;
        case DAY:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_DAY;
            break;
        case HOUR:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_HOUR;
            break;
        case MINUTE:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_MINUTE;
            break;
        case SECOND:
            formatter = TtlTaskSqlBuilder.PART_NAME_FORMAT_ON_UNIT_SECOND;
            break;
        }
        return formatter;
    }

    public static String buildArchivePartitionSql(String tableSchema,
                                                  String tableName,
                                                  List<String> targetPartNames,
                                                  int ossParallelism) {
        String sql = "";
        String dbNameStr = SqlIdentifierUtil.escapeIdentifierString(tableSchema);
        String tbNameStr = SqlIdentifierUtil.escapeIdentifierString(tableName);
        String partNameListStr = "";
        for (int i = 0; i < targetPartNames.size(); i++) {
            if (i > 0) {
                partNameListStr += ",";
            }
            partNameListStr += SqlIdentifierUtil.escapeIdentifierString(targetPartNames.get(i));
        }
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true, OSS_BACKFILL_PARALLELISM=%d)*/ALTER TABLE %s.%s ARCHIVE PARTITIONS %s",
            ossParallelism, dbNameStr, tbNameStr, partNameListStr);
        return sql;
    }

    public static final String SUBJOB_NAME_FOR_ADD_PARTS_OF_ARC_TMP_TBL = "SUBJOB_NAME_FOR_ADD_PARTS_OF_ARC_TMP_TBL";
    public static final String SUBJOB_NAME_FOR_ADD_PARTS_OF_OSS_TBL = "SUBJOB_NAME_FOR_ADD_PARTS_OF_OSS_TBL";
    public static final String SUBJOB_NAME_FOR_CREATE_CCI_OF_ARC_TMP = "SUBJOB_NAME_FOR_CREATE_CCI_OF_ARC_TMP";
    public static final String SUBJOB_NAME_FOR_ADD_PARTS_FOR_ARC_TMP_CCI = "SUBJOB_NAME_FOR_ADD_PARTS_FOR_ARC_TMP_CCI";

    /**
     * The SubJobTaskName is used by the pervious tasks of the subjobTask to dynamic modify subjobTask sql
     */
    public static String buildSubJobTaskNameForAddPartsBySpecifySubjobStmt(boolean isBuildForArcTable) {
        String stmt = "";
        if (isBuildForArcTable) {
            stmt = String.format(SUBJOB_STMT_TEMP, SUBJOB_NAME_FOR_ADD_PARTS_OF_OSS_TBL);
        } else {
            stmt = String.format(SUBJOB_STMT_TEMP, SUBJOB_NAME_FOR_ADD_PARTS_OF_ARC_TMP_TBL);
        }
        return stmt;
    }

    public static String buildSubJobTaskNameForCreateColumnarIndexBySpecifySubjobStmt() {
        String stmt = "";
        stmt = String.format(SUBJOB_STMT_TEMP, SUBJOB_NAME_FOR_CREATE_CCI_OF_ARC_TMP);
        return stmt;
    }

    public static String buildSubJobTaskNameForAddPartsFroActTmpCciBySpecifySubjobStmt() {
        String stmt = "";
        stmt = String.format(SUBJOB_STMT_TEMP, SUBJOB_NAME_FOR_ADD_PARTS_FOR_ARC_TMP_CCI);
        return stmt;
    }

    public static final String STATS_PHY_TTL_TMP_TBL_DATA_LENGTH_BY_GROUP =
        "/*TDDL: node='%s'*/select sum(table_rows) all_table_rows, sum(data_length + index_length) all_data_length from information_schema.tables where table_schema='%s' and table_name in (%s)";

    public static String buildQueryPhyInfoSchemaTablesByGroup(String groupKey,
                                                              String phyDb,
                                                              List<String> phyTbList) {
        StringBuilder phyTbListSb = new StringBuilder("");
        for (int i = 0; i < phyTbList.size(); i++) {
            if (i > 0) {
                phyTbListSb.append(",");
            }
            phyTbListSb.append("'");
            phyTbListSb.append(phyTbList.get(i));
            phyTbListSb.append("'");
        }
        String phyTbListStr = phyTbListSb.toString();
        String querySql = String.format(STATS_PHY_TTL_TMP_TBL_DATA_LENGTH_BY_GROUP, groupKey, phyDb, phyTbListStr);
        return querySql;
    }

    public static final String STATS_PHY_TTL_TBL_DATA_FREE_BY_GROUP =
        "/*TDDL: node='%s'*/select sum(table_rows) all_table_rows, sum(data_length + index_length) all_data_length, sum(data_free) all_data_free from information_schema.tables where table_schema='%s' and table_name in (%s)";

    public static String buildQueryPhyDataFreeInfoSchemaTablesByGroup(String groupKey,
                                                                      String phyDb,
                                                                      List<String> phyTbList) {
        StringBuilder phyTbListSb = new StringBuilder("");
        for (int i = 0; i < phyTbList.size(); i++) {
            if (i > 0) {
                phyTbListSb.append(",");
            }
            phyTbListSb.append("'");
            phyTbListSb.append(phyTbList.get(i));
            phyTbListSb.append("'");
        }
        String phyTbListStr = phyTbListSb.toString();
        String querySql = String.format(STATS_PHY_TTL_TBL_DATA_FREE_BY_GROUP, groupKey, phyDb, phyTbListStr);
        return querySql;
    }

    public static String buildAsyncOptimizeTableSql(String tableSchema,
                                                    String tableName,
                                                    String queryHint) {
        String sql = "";
        String dbNameStr = SqlIdentifierUtil.escapeIdentifierString(tableSchema);
        String tbNameStr = SqlIdentifierUtil.escapeIdentifierString(tableName);
        sql = String.format(
            "%s OPTIMIZE TABLE /* from ttl job */ %s.%s",
            queryHint, dbNameStr, tbNameStr);
        return sql;
    }

    public static String buildPauseDdlSql(Long jobId) {
        String sql = "";
        sql = String.format(
            "PAUSE DDL /* from ttl job */ %s", jobId);
        return sql;
    }

    public static String buildAsyncContinueDdlSql(Long jobId, int optiTblParallelism) {
        String sql = "";
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true, RETURN_JOB_ID_ON_ASYNC_DDL_MODE=true, OPTIMIZE_TABLE_PARALLELISM=%s)*/ CONTINUE DDL /* from ttl job */ %s",
            optiTblParallelism, jobId);
        return sql;
    }

    public static String buildContinueDdlSql(Long jobId) {
        String sql = "";
        sql = String.format(
            "CONTINUE DDL /* from scheduled ttl job */ %s", jobId);
        return sql;
    }

    public static String buildAsyncContinueDdlSql(Long jobId) {
        String sql = "";
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true, RETURN_JOB_ID_ON_ASYNC_DDL_MODE=true)*/ CONTINUE DDL /* from scheduled ttl job */ %s",
            jobId);
        return sql;
    }

    public static String buildCreateColumnarIndexSqlForArcTbl(TtlDefinitionInfo ttlInfo,
                                                              String arcTblSchema,
                                                              String arcTblName,
                                                              List<String> newAddRangeBounds,
                                                              List<String> newAddRangePartNames,
                                                              ExecutionContext executionContext) {

        if (TtlConfigUtil.isUseGsiInsteadOfCciForCreateColumnarArcTbl(executionContext)) {
            return buildCreateNormalGlobalIndexSqlForArcTbl(ttlInfo, arcTblSchema, arcTblName, newAddRangeBounds,
                newAddRangePartNames, executionContext);
        }

        String sql = "";
        String ciNameOfArcTbl = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        String ciPartByStr =
            buildPartBoundsSqlTemplateForColumnarIndex(ttlInfo, newAddRangeBounds, newAddRangePartNames,
                executionContext);

        String cciHint = "";
        String skipDdlTasks =
            executionContext.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
        if (!StringUtils.isEmpty(skipDdlTasks)) {
            cciHint = String.format("/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"%s\")*/", skipDdlTasks);
        }

        sql = String.format("%s create clustered columnar index `%s` on `%s`(`%s`) %s;",
            cciHint, ciNameOfArcTbl, ttlTblName, ttlColName, ciPartByStr);
        return sql;
    }

    public static String buildCreateNormalGlobalIndexSqlForArcTbl(TtlDefinitionInfo ttlInfo,
                                                                  String arcTblSchema,
                                                                  String arcTblName,
                                                                  List<String> newAddRangeBounds,
                                                                  List<String> newAddRangePartNames,
                                                                  ExecutionContext executionContext) {
        String sql = "";
        String ciNameOfArcTbl = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        String ciPartByStr =
            buildPartBoundsSqlTemplateForColumnarIndex(ttlInfo, newAddRangeBounds, newAddRangePartNames,
                executionContext);
//        String arcTmpTgName = buildArcTmpTgName(ttlInfo);
//        sql = String.format("create clustered index `%s` on `%s`(`%s`) %s tablegroup=%s;",
//            ciNameOfArcTbl, ttlTblName, ttlColName, ciPartByStr, arcTmpTgName);
        sql = String.format("create clustered index `%s` on `%s`(`%s`) %s;",
            ciNameOfArcTbl, ttlTblName, ttlColName, ciPartByStr);
        return sql;
    }

    public static String buildDropColumnarIndexSqlForArcTbl(TtlDefinitionInfo ttlInfo, String arcTblSchema,
                                                            String arcTblName) {
        String sql = "";
        String ciNameOfArcTbl = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        sql = String.format("alter table `%s`.`%s` drop index `%s`;",
            ttlTblSchema, ttlTblName, ciNameOfArcTbl);
        return sql;
    }

    public static String buildCreateViewSqlForArcTbl(String arcTblSchema,
                                                     String arcTblName,
                                                     TtlDefinitionInfo ttlInfo) {
        String sql = "";
        String viewSchema = arcTblSchema;
        String viewName = arcTblName;
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ciIndexName = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);
        sql = String.format("create or replace view `%s`.`%s` as select * from `%s`.`%s` force index (`%s`);",
            viewSchema, viewName, ttlTblSchema, ttlTblName, ciIndexName);
        return sql;
    }

    public static String buildSqlForFullScanArcTbl(String ttlTblSchema,
                                                   String ttlTblName,
                                                   String cciIndexName,
                                                   List<ColumnMeta> columnMetaList
    ) {
        String sql = "";
        String colListStr = "";
        for (int i = 0; i < columnMetaList.size(); i++) {
            if (i > 0) {
                colListStr += ",";
            }
            ColumnMeta cm = columnMetaList.get(i);
            String col = cm.getName();
            String fullColName = String.format("`%s`.`%s`", ttlTblName, col);
            colListStr += fullColName;
        }
        sql = String.format("select %s from `%s`.`%s` force index (`%s`);",
            colListStr, ttlTblSchema, ttlTblName, cciIndexName);
        return sql;
    }

    public static String buildDropViewSqlFroArcTbl(String arcTblSchema,
                                                   String arcTblName,
                                                   TtlDefinitionInfo ttlInfo) {
        String sql = "";
        String viewSchema = arcTblSchema;
        String viewName = arcTblName;
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ciIndexName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
        sql = String.format("drop view if exists `%s`.`%s`;",
            viewSchema, viewName, ttlTblSchema, ttlTblName, ciIndexName);
        return sql;
    }

    public static String buildModifyTtlSqlForBindArcTbl(String ttlTblSchema,
                                                        String ttlTblName,
                                                        String arcTblSchema,
                                                        String arcTblName,
                                                        TtlArchiveKind archiveKind
    ) {

        String alterModifyTtlSql =
            String.format(ALTER_TABLE_MODIFY_TTL_FOR_BIND_ARCHIVE_TABLE, ttlTblSchema, ttlTblName,
                archiveKind.getArchiveKindStr(),
                arcTblSchema, arcTblName);
        return alterModifyTtlSql;
    }

//    public static String buildArcTmpTgName(TtlDefinitionInfo ttlInfo) {
//        String tgName = "";
//        String arcTmpTblName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
//        tgName = String.format(ARC_TMP_TG_NAME_TEMPLATEE, arcTmpTblName);
//        return tgName;
//    }
//
//    public static String buildColumnarTgName(TtlDefinitionInfo ttlInfo) {
//        String tgName = "";
////        tgName = TableGroupNameUtil.autoBuildTableGroupName(tableGroupId, tgType);
//        return tgName;
//    }
//
//    public static String buildCreateArcTmpTgSqlForArcTmp(TtlDefinitionInfo ttlInfo) {
//        String sql = "";
//        String arcTmpTgName = buildArcTmpTgName(ttlInfo);
//        sql = String.format("create tablegroup if not exists `%s`;", arcTmpTgName);
//        return sql;
//    }
//
//    public static String buildDropArcTmpTgSqlForArcTmp(TtlDefinitionInfo ttlInfo) {
//        String sql = "";
//        String arcTmpTgName = buildArcTmpTgName(ttlInfo);
//        sql = String.format("drop tablegroup if exists `%s`;", arcTmpTgName);
//        return sql;
//    }
//    public static String buildArcTmpNameByArcTblName(String arcTblName) {
//        String finalArcTmpName = "";
//        if (StringUtils.isEmpty(arcTblName)) {
//            return finalArcTmpName;
//        }
//        int length = arcTblName.length();
//        String ttlTmpNamePrefixStr = arcTblName;
//        String ttlTblNameHashCodeHexStr = "";
//        if (length > TtlUtil.MAX_TTL_TMP_NAME_PREFIX_LENGTH) {
//            ttlTmpNamePrefixStr = arcTblName.substring(0, TtlUtil.MAX_TTL_TMP_NAME_PREFIX_LENGTH);
//            ttlTblNameHashCodeHexStr = GroupInfoUtil.doMurmur3Hash32(arcTblName);
//            finalArcTmpName =
//                String.format(ARC_TMP_NAME_TEMPLATE_WITH_HASHCODE, ttlTmpNamePrefixStr, ttlTblNameHashCodeHexStr);
//        } else {
//            finalArcTmpName = String.format(ARC_TMP_NAME_TEMPLATE_WITHOUT_HASHCODE, ttlTmpNamePrefixStr);
//        }
//        finalArcTmpName = finalArcTmpName.toLowerCase();
//        return finalArcTmpName;
//    }

    public static String buildTtTableCleanupExpiredDataSql(String ttlTblSchema, String ttlTableName) {
        String alterTableCleanupExpiredDataSql =
            String.format(ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE, ttlTblSchema, ttlTableName);
        return alterTableCleanupExpiredDataSql;
    }

    public static String buildAsyncTtTableCleanupExpiredDataSql(String ttlTblSchema, String ttlTableName) {
        String alterTableCleanupExpiredDataSql =
            String.format(ASYNC_ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE, ttlTblSchema, ttlTableName);
        return alterTableCleanupExpiredDataSql;
    }

}
