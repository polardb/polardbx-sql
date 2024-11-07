package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.TtlEventLogUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Data;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@Data
public class TtlDefinitionInfo {

    /**
     * The ttl definition
     */
    protected TtlInfoRecord ttlInfoRecord = new TtlInfoRecord();

    /**
     * The pk columns of ttl table
     */
    protected List<String> pkColumns = new ArrayList<>();

    public TtlDefinitionInfo() {
    }

    public boolean isEnableTtlSchedule() {
        return ttlInfoRecord.getTtlStatus() == TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE;
    }

    public boolean isTtlBinlogOpen() {
        return ttlInfoRecord.getTtlBinlog() == TtlInfoRecord.TTL_BINLOG_OPEN_BINLOG_DURING_CLEANING_DATA;
    }

    public boolean isEnableArchivedTableSchedule() {
        return ttlInfoRecord.getArcStatus() == TtlInfoRecord.ARCHIVE_STATUS_ENABLE_OSS_ARCHIVE;
    }

    public boolean performArchiveByColumnarIndex() {
        return ttlInfoRecord.getArcKind() == TtlInfoRecord.ARCHIVE_KIND_COLUMNAR;
    }

    public boolean needPerformExpiredDataArchivingByOssTbl() {
        if (performArchiveByColumnarIndex()) {
            return false;
        }
        return !StringUtils.isEmpty(ttlInfoRecord.getArcTblName());
    }

    public boolean needPerformExpiredDataArchivingByCci() {
        if (!performArchiveByColumnarIndex()) {
            return false;
        }
        return !StringUtils.isEmpty(ttlInfoRecord.getArcTblName());
    }

    public boolean needPerformExpiredDataArchiving() {
        return !StringUtils.isEmpty(ttlInfoRecord.getArcTblName());
    }

    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.of(TtlTimeUnit.of(ttlInfoRecord.getTtlUnit()).getTimeUnitName());
    }

    public boolean useTimestampOnTtlCol(ExecutionContext ec) {
        return TtlUtil.useTimestampOnTtlCol(this, ec);
    }

    /**
     * Create TtlDefInfo from metadb
     */
    public static TtlDefinitionInfo createFrom(TtlInfoRecord ttlInfoRecord,
                                               List<String> pkColumns) {
        if (ttlInfoRecord == null) {
            return null;
        }
        TtlDefinitionInfo ttlDefinitionInfo = new TtlDefinitionInfo();
        ttlDefinitionInfo.setTtlInfoRecord(ttlInfoRecord);
        ttlDefinitionInfo.setPkColumns(pkColumns);
        return ttlDefinitionInfo;
    }

    public static Integer getArcKindByEngine(String tableEngine) {
        if (StringUtils.isEmpty(tableEngine)) {
            return TtlInfoRecord.ARCHIVE_KIND_UNDEF;
        }
        if (tableEngine.equalsIgnoreCase(Engine.COLUMNAR.name())) {
            return TtlInfoRecord.ARCHIVE_KIND_COLUMNAR;
        } else {
            return TtlInfoRecord.ARCHIVE_KIND_UNDEF;
        }
    }

    public static TtlDefinitionInfo createNewTtlInfo(
        String tableSchema,
        String tableName,
        String ttlEnable,
        SqlTimeToLiveExpr ttlExpr,
        SqlTimeToLiveJobExpr ttlJob,
        String archiveKind,
        String archiveTableSchema,
        String archiveTableName,
        Integer arcPreAllocateCount,
        Integer arcPostAllocateCount,
        List<String> pkColumns,
        TableMeta ttlTableMeta,
        PartitionInfo newCreatedTblPartInfo,
        SqlCreateTable sqlCreateTableAst,
        ExecutionContext ec) {

        if (ttlExpr == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL_PARAMS,
                String.format(
                    "Failed to create new ttl definition for table `%s`.`%s` because the ttl_expr is not defined",
                    tableSchema, tableName));
        }

        String ttlExprStrVal = ttlExpr.toString();

        SqlIdentifier columnNode = (SqlIdentifier) ttlExpr.getColumn();
        String columnName = SQLUtils.normalize(columnNode.getLastName()).trim();

        SqlNumericLiteral expireAfterNode = (SqlNumericLiteral) ttlExpr.getExpireAfter();
        Integer expireAfter = ((Long) expireAfterNode.getValue()).intValue();

        SqlIdentifier unitNode = (SqlIdentifier) ttlExpr.getUnit();
        String expireAfterUnit = SQLUtils.normalize(unitNode.getLastName());

        SqlNode timezone = ttlExpr.getTimezone();
        String ttlExprTimezoneVal = null;
        if (timezone != null) {
            SqlCharStringLiteral timezoneNode = (SqlCharStringLiteral) timezone;
            String ttlExprTimezoneValTmp = timezoneNode.toString();
            ttlExprTimezoneVal = SQLUtils.normalize(ttlExprTimezoneValTmp);
        }

        String cronVal = ConnectionParams.DEFAULT_TTL_SCHEDULE_CRON_EXPR.getDefault();
        if (ttlJob != null) {
            SqlNode cronNode = ttlJob.getCron();
            cronVal = SQLUtils.normalize((cronNode).toString());
        }

        int ttlStatusVal = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE;
        if (!StringUtils.isEmpty(ttlEnable)) {
            if (ttlEnable.equalsIgnoreCase(TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE_STR_VAL)) {
                ttlStatusVal = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE;
            }
        }

        int arcKind = TtlInfoRecord.ARCHIVE_KIND_UNDEF;
        if (archiveKind != null) {
            arcKind = TtlArchiveKind.of(archiveKind).getArchiveKindCode();
        }

        String archiveTableNameVal = null;
        if (!StringUtils.isEmpty(archiveTableName)) {
            /**
             * arcTblName has NOT been specified
             */
            archiveTableNameVal = archiveTableName;
        }

        String archiveTableSchemaVal = null;
        if (!StringUtils.isEmpty(archiveTableSchema)) {
            archiveTableSchemaVal = archiveTableSchema;
        } else {
            if (!StringUtils.isEmpty(archiveTableNameVal)) {
                /**
                 * arcTblName has been specified, but arcTblSchema has NOT been specified,
                 * so arcTblSchema use the ttlSchema as default value
                 */
                archiveTableSchemaVal = tableSchema;
            }
        }

        String arcTmpTblSchema = null;
        if (!StringUtils.isEmpty(archiveTableSchemaVal)) {
            arcTmpTblSchema = archiveTableSchemaVal;
        }

        String arcTmpTblName = null;
        if (!StringUtils.isEmpty(archiveTableNameVal)) {
            arcTmpTblName = TtlUtil.buildArcTmpNameByArcTblName(archiveTableNameVal);
        }

        int arcStatus = TtlInfoRecord.ARCHIVE_STATUS_DISABLE_OSS_ARCHIVE;

        TtlDefinitionInfo newTtlInfoToBeReturn = null;
        newTtlInfoToBeReturn = new TtlDefinitionInfo();
        TtlInfoRecord ttlDefRec = new TtlInfoRecord();
        ttlDefRec.setTableSchema(tableSchema);
        ttlDefRec.setTableName(tableName);

        ttlDefRec.setTtlStatus(ttlStatusVal);
        ttlDefRec.setTtlExpr(ttlExprStrVal);
        ttlDefRec.setTtlFilter("");
        ttlDefRec.setTtlCol(columnName);
        ttlDefRec.setTtlInterval(expireAfter);
        ttlDefRec.setTtlUnit(TtlTimeUnit.of(expireAfterUnit).getTimeUnitCode());
        ttlDefRec.setTtlTimezone(ttlExprTimezoneVal);
        ttlDefRec.setTtlCron(cronVal);
        ttlDefRec.setTtlBinlog(TtlInfoRecord.TTL_BINLOG_CLOSE_BINLOG_DURING_CLEANING_DATA);

        ttlDefRec.setArcKind(arcKind);
        ttlDefRec.setArcStatus(arcStatus);
        ttlDefRec.setArcTmpTblSchema(arcTmpTblSchema);
        ttlDefRec.setArcTmpTblName(arcTmpTblName);
        ttlDefRec.setArcTblSchema(archiveTableSchemaVal);
        ttlDefRec.setArcTblName(archiveTableNameVal);

        ttlDefRec.setArcPartMode(TtlInfoRecord.ARC_PART_MODE_NORMAL);
        ttlDefRec.setArcPartUnit(ttlDefRec.getTtlUnit());
        ttlDefRec.setArcPartInterval(TtlConfigUtil.getIntervalOfPreBuildPartOfArcTbl());
        ttlDefRec.setArcPrePartCnt(arcPreAllocateCount);
        ttlDefRec.setArcPostPartCnt(arcPostAllocateCount);

        ExtraFieldJSON extraJsonFld = new ExtraFieldJSON();
        ttlDefRec.setExtra(extraJsonFld);

        newTtlInfoToBeReturn.setTtlInfoRecord(ttlDefRec);
        newTtlInfoToBeReturn.setPkColumns(pkColumns);

        TtlMetaValidationUtil.validateTtlDefinition(newTtlInfoToBeReturn, ttlTableMeta, newCreatedTblPartInfo, sqlCreateTableAst, ec);
        logEventForCreateTtlInfo(newCreatedTblPartInfo != null, ttlDefRec);

        return newTtlInfoToBeReturn;
    }

    public static TtlDefinitionInfo buildModifiedTtlInfo(TtlDefinitionInfo oldTtlInfo,
                                                         String tableSchema,
                                                         String tableName,
                                                         String ttlEnable,
                                                         SqlTimeToLiveExpr ttlExpr,
                                                         SqlTimeToLiveJobExpr ttlJob,
                                                         String archiveKind,
                                                         String archiveTableSchema,
                                                         String archiveTableName,
                                                         Integer arcPreAllocateCount,
                                                         Integer arcPostAllocateCount,
                                                         List<String> pkColumns,
                                                         TableMeta ttlTableMeta,
                                                         ExecutionContext ec) {
        TtlDefinitionInfo newTtlInfoToBeReturn = null;
        newTtlInfoToBeReturn = oldTtlInfo.copy();

        newTtlInfoToBeReturn.getTtlInfoRecord().setTableSchema(tableSchema);
        newTtlInfoToBeReturn.getTtlInfoRecord().setTableName(tableName);
        newTtlInfoToBeReturn.setPkColumns(pkColumns);

        if (ttlEnable != null) {
            int ttlStatusVal = TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE;
            if (ttlEnable.equalsIgnoreCase(TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE_STR_VAL)) {
                ttlStatusVal = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE;
            }
            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlStatus(ttlStatusVal);
        }

        if (ttlExpr != null) {
            String ttlExprStrVal = ttlExpr.toString();
            SqlIdentifier columnNode = (SqlIdentifier) ttlExpr.getColumn();

            String ttlCol = SQLUtils.normalize(columnNode.getLastName()).trim();

            SqlNumericLiteral expireAfterNode = (SqlNumericLiteral) ttlExpr.getExpireAfter();
            Integer expireAfter = ((Long) expireAfterNode.getValue()).intValue();

            SqlIdentifier unitNode = (SqlIdentifier) ttlExpr.getUnit();
            String expireAfterUnit = SQLUtils.normalize(unitNode.getLastName());

            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlCol(ttlCol);
            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlExpr(ttlExprStrVal);
            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlInterval(expireAfter);

            int newTimeUnitCode = TtlTimeUnit.of(expireAfterUnit).getTimeUnitCode();
            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlUnit(newTimeUnitCode);
            newTtlInfoToBeReturn.getTtlInfoRecord().setArcPartUnit(newTimeUnitCode);

            SqlNode timezone = ttlExpr.getTimezone();
            String ttlExprTimezoneVal = null;
            if (timezone != null) {
                SqlCharStringLiteral timezoneNode = (SqlCharStringLiteral) timezone;
                String ttlExprTimezoneValTmp = timezoneNode.toString();
                ttlExprTimezoneVal = SQLUtils.normalize(ttlExprTimezoneValTmp);
                newTtlInfoToBeReturn.getTtlInfoRecord().setTtlTimezone(ttlExprTimezoneVal);
            }
        }

        if (ttlJob != null) {
            SqlNode cronNode = ttlJob.getCron();
            String cronVal = SQLUtils.normalize(((SqlCharStringLiteral) cronNode).toString());

//        SqlNode cronTimezone = ttlJob.getTimezone();
//        String cronTimezoneVal = null;
//        if (cronTimezone != null) {
//            SqlCharStringLiteral cronTimezoneNode = (SqlCharStringLiteral) cronTimezone;
//            cronTimezoneVal = cronTimezoneNode.toString();
//        }
            newTtlInfoToBeReturn.getTtlInfoRecord().setTtlCron(cronVal);
        }

        if (archiveTableName != null) {
            if (StringUtils.isEmpty(archiveTableName)) {
                /**
                 * arcTblName has been specified as an empty str, means unbinding arcTableName
                 */
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTblName(null);
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTmpTblName(null);
            } else {
                /**
                 * arcTblName has been specified as an non-empty str, means binding arcTableName
                 */
                String arcTmpTblName = TtlUtil.buildArcTmpNameByArcTblName(archiveTableName);
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTblName(archiveTableName);
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTmpTblName(arcTmpTblName);
            }
        }

        if (archiveTableSchema != null) {
            String arcTblNameToBeReturn = newTtlInfoToBeReturn.getTtlInfoRecord().getArcTblName();
            if (StringUtils.isEmpty(arcTblNameToBeReturn)) {
                /**
                 * arcTblName has NOT been specified, arcTblSchema must be null;
                 */
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTblSchema(null);
                newTtlInfoToBeReturn.getTtlInfoRecord().setArcTmpTblSchema(null);
            } else {
                /**
                 * arcTblName has been specified
                 */
                if (StringUtils.isEmpty(archiveTableSchema)) {
                    /**
                     * arcTblName has been specified, but arcTblSchema has NOT been specified,
                     * so arcTblSchema use the ttlSchema as default value
                     */
                    newTtlInfoToBeReturn.getTtlInfoRecord().setArcTblSchema(tableSchema);
                    newTtlInfoToBeReturn.getTtlInfoRecord().setArcTmpTblSchema(tableSchema);
                } else {
                    newTtlInfoToBeReturn.getTtlInfoRecord().setArcTblSchema(archiveTableSchema);
                    newTtlInfoToBeReturn.getTtlInfoRecord().setArcTmpTblSchema(archiveTableSchema);
                }
            }
        }

        if (archiveKind != null) {
            newTtlInfoToBeReturn.getTtlInfoRecord().setArcKind(TtlArchiveKind.of(archiveKind).getArchiveKindCode());
        }

        if (arcPreAllocateCount != null) {
            newTtlInfoToBeReturn.getTtlInfoRecord().setArcPrePartCnt(arcPreAllocateCount);
        }

        if (arcPostAllocateCount != null) {
            newTtlInfoToBeReturn.getTtlInfoRecord().setArcPostPartCnt(arcPostAllocateCount);
        }

        TtlMetaValidationUtil.validateTtlDefinition(newTtlInfoToBeReturn, ttlTableMeta, null, null, ec);
        logEventForModifyTtlInfo(oldTtlInfo, newTtlInfoToBeReturn);

        return newTtlInfoToBeReturn;
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
            options += String.format("TTL_JOB = CRON '%s' TIMEZONE '%s', ", ttlInfoRecord.getTtlCron(),
                ttlInfoRecord.getTtlTimezone());
            options += String.format("ARCHIVE_TYPE = '%s', ",
                TtlArchiveKind.of(ttlInfoRecord.getArcKind()).getArchiveKindStr());
            String ttlTblSchema = ttlInfoRecord.getTableSchema();
            String arcTblSchema = ttlInfoRecord.getArcTblSchema();
            String archTblName = ttlInfoRecord.getArcTblName();
            if (!StringUtils.isEmpty(archTblName) && !arcTblSchema.equalsIgnoreCase(ttlTblSchema)) {
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
        List<String> newPkColumns = new ArrayList<>();
        newPkColumns.addAll(this.pkColumns);

        TtlDefinitionInfo newTtlInfo = new TtlDefinitionInfo();
        newTtlInfo.setTtlInfoRecord(newTtlRec);
        newTtlInfo.setPkColumns(newPkColumns);

        return newTtlInfo;
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

    public List<String> getPkColumns() {
        return pkColumns;
    }

    public void setPkColumns(List<String> pkColumns) {
        this.pkColumns = pkColumns;
    }
}
