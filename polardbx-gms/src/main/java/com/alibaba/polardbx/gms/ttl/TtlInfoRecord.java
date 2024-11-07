package com.alibaba.polardbx.gms.ttl;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author chenghui.lch
 */
public class TtlInfoRecord implements SystemTableRecord {

    public static final String TTL_STATUS_DISABLE_SCHEDULE_STR_VAL = "OFF";
    public static final String TTL_STATUS_ENABLE_SCHEDULE_STR_VAL = "ON";

    public static final int TTL_STATUS_DISABLE_SCHEDULE = 0;
    public static final int TTL_STATUS_ENABLE_SCHEDULE = 1;
    public static final int TTL_BINLOG_CLOSE_BINLOG_DURING_CLEANING_DATA = 0;
    public static final int TTL_BINLOG_OPEN_BINLOG_DURING_CLEANING_DATA = 1;

    public static final int ARCHIVE_KIND_UNDEF = 0;// ' ARCHIVE_KIND='' '
    public static final int ARCHIVE_KIND_COLUMNAR = 1;// ' ARCHIVE_KIND='COLUMNAR' '
    public static final int ARCHIVE_KIND_PARTITION = 2;// ' ARCHIVE_KIND='PARTITION' '
    public static final int ARCHIVE_KIND_SUBPARTITION = 3;// ' ARCHIVE_KIND='SUBPARTITION' '

    public static final int ARCHIVE_STATUS_DISABLE_OSS_ARCHIVE = 0;
    public static final int ARCHIVE_STATUS_ENABLE_OSS_ARCHIVE = 1;

    public static final String TTL_UNIT_YEAR = "YEAR";
    public static final String TTL_UNIT_MONTH = "MONTH";
    public static final String TTL_UNIT_DAY = "DAY";
    public static final String TTL_UNIT_HOUR = "HOUR";
    public static final String TTL_UNIT_MINUTE = "MINUTE";
    public static final String TTL_UNIT_SECOND = "SECOND";

    public static final Integer ARC_PART_MODE_NORMAL = 0;

    private Long id;
    private Date gmtCreated;
    private Date gmtModified;

    private String tableSchema;
    private String tableName;

    /**
     * 0-disable ttl schedule,1-enable ttl schedule
     */
    private Integer ttlStatus;
    private String ttlExpr;
    private String ttlFilter;
    private String ttlCol;
    private Integer ttlInterval;
    /**
     * The time unit code of ttl task
     * <pre>
     *     0:year
     *     1:month
     *     2:day
     *     3:hour
     *     4:minute
     *     5:second
     * </pre>
     */
    private Integer ttlUnit;
    private String ttlTimezone;
    private String ttlCron;
    /**
     * 0-disable gen binlog, 1-enable gen binlog
     */
    private Integer ttlBinlog;

    /**
     * <pre>
     *  0-arc by oss tbl;
     *  1-arc by columnar index;
     *  2-arc by time-based range-part;
     *  3-arc by time-based range-subpart;
     *  4-arc by self-def-range-part.
     * </pre>
     */
    private Integer arcKind;

    /**
     * The mode of arc part generation
     */
    private Integer arcPartMode = 0;
    /**
     * The interval of arc part generation
     */
    private Integer arcPartInterval = 1;
    /**
     * The time unit code of arc part generation
     * <pre>
     *     0:year
     *     1:month
     *     2:day
     *     3:hour
     *     4:minute
     *     5:second
     * </pre>
     */
    private Integer arcPartUnit = 1;
    /**
     * The previous allocated partition counts of ttl-tmp for future
     */
    private Integer arcPrePartCnt = 0;
    /**
     * The post allocated partition counts of ttl-tmp base on ttl-col min value for past
     */
    private Integer arcPostPartCnt = 0;

    /**
     * 0-enable oss arc schedule,
     * 1-disable oss arc schedule
     */
    private Integer arcStatus;
    private String arcTmpTblSchema;
    private String arcTmpTblName;

    private String arcTblSchema;
    private String arcTblName;

    private ExtraFieldJSON extra;

    public TtlInfoRecord() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getGmtCreated() {
        return gmtCreated;
    }

    public void setGmtCreated(Date gmtCreated) {
        this.gmtCreated = gmtCreated;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getTtlStatus() {
        return ttlStatus;
    }

    public void setTtlStatus(Integer ttlStatus) {
        this.ttlStatus = ttlStatus;
    }

    public String getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(String ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public String getTtlCol() {
        return ttlCol;
    }

    public void setTtlCol(String ttlCol) {
        this.ttlCol = ttlCol;
    }

    public Integer getTtlInterval() {
        return ttlInterval;
    }

    public void setTtlInterval(Integer ttlInterval) {
        this.ttlInterval = ttlInterval;
    }

    public Integer getTtlUnit() {
        return ttlUnit;
    }

    public void setTtlUnit(Integer ttlUnit) {
        this.ttlUnit = ttlUnit;
    }

    public String getTtlTimezone() {
        return ttlTimezone;
    }

    public void setTtlTimezone(String ttlTimezone) {
        this.ttlTimezone = ttlTimezone;
    }

    public String getTtlCron() {
        return ttlCron;
    }

    public void setTtlCron(String ttlCron) {
        this.ttlCron = ttlCron;
    }

    public Integer getTtlBinlog() {
        return ttlBinlog;
    }

    public void setTtlBinlog(Integer ttlBinlog) {
        this.ttlBinlog = ttlBinlog;
    }

    public Integer getArcStatus() {
        return arcStatus;
    }

    public void setArcStatus(Integer arcStatus) {
        this.arcStatus = arcStatus;
    }

    public String getArcTmpTblSchema() {
        return arcTmpTblSchema;
    }

    public void setArcTmpTblSchema(String arcTmpTblSchema) {
        this.arcTmpTblSchema = arcTmpTblSchema;
    }

    public String getArcTmpTblName() {
        return arcTmpTblName;
    }

    public void setArcTmpTblName(String arcTmpTblName) {
        this.arcTmpTblName = arcTmpTblName;
    }

    public Integer getArcPrePartCnt() {
        return arcPrePartCnt;
    }

    public void setArcPrePartCnt(Integer arcPrePartCnt) {
        this.arcPrePartCnt = arcPrePartCnt;
    }

    public String getArcTblSchema() {
        return arcTblSchema;
    }

    public void setArcTblSchema(String arcTblSchema) {
        this.arcTblSchema = arcTblSchema;
    }

    public String getArcTblName() {
        return arcTblName;
    }

    public void setArcTblName(String arcTblName) {
        this.arcTblName = arcTblName;
    }

    public ExtraFieldJSON getExtra() {
        return extra;
    }

    public void setExtra(ExtraFieldJSON extra) {
        this.extra = extra;
    }

    public Integer getArcPostPartCnt() {
        return arcPostPartCnt;
    }

    public void setArcPostPartCnt(Integer arcPostPartCnt) {
        this.arcPostPartCnt = arcPostPartCnt;
    }

    public Integer getArcKind() {
        return arcKind;
    }

    public void setArcKind(Integer arcKind) {
        this.arcKind = arcKind;
    }

    public String getTtlFilter() {
        return ttlFilter;
    }

    public void setTtlFilter(String ttlFilter) {
        this.ttlFilter = ttlFilter;
    }

    public Integer getArcPartMode() {
        return arcPartMode;
    }

    public void setArcPartMode(Integer arcPartMode) {
        this.arcPartMode = arcPartMode;
    }

    public Integer getArcPartInterval() {
        return arcPartInterval;
    }

    public void setArcPartInterval(Integer arcPartInterval) {
        this.arcPartInterval = arcPartInterval;
    }

    public Integer getArcPartUnit() {
        return arcPartUnit;
    }

    public void setArcPartUnit(Integer arcPartUnit) {
        this.arcPartUnit = arcPartUnit;
    }

    @Override
    public TtlInfoRecord fill(ResultSet rs) throws SQLException {

        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");

        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");

        this.ttlStatus = rs.getInt("ttl_status");
        this.ttlExpr = rs.getString("ttl_expr");
        this.ttlFilter = rs.getString("ttl_filter");
        this.ttlInterval = rs.getInt("ttl_interval");
        this.ttlUnit = rs.getInt("ttl_unit");
        this.ttlCol = rs.getString("ttl_col");
        this.ttlTimezone = rs.getString("ttl_tz");
        this.ttlCron = rs.getString("ttl_cron");
        this.ttlBinlog = rs.getInt("ttl_binlog");

        this.arcKind = rs.getInt("arc_kind");
        this.arcStatus = rs.getInt("arc_status");
        this.arcTmpTblSchema = rs.getString("arc_tmp_tbl_schema");
        this.arcTmpTblName = rs.getString("arc_tmp_tbl_name");
        this.arcTblSchema = rs.getString("arc_tbl_schema");
        this.arcTblName = rs.getString("arc_tbl_name");

        this.arcPartMode = rs.getInt("arc_part_mode");
        this.arcPartInterval = rs.getInt("arc_part_interval");
        this.arcPartUnit = rs.getInt("arc_part_unit");
        this.arcPrePartCnt = rs.getInt("arc_pre_part_cnt");
        this.arcPostPartCnt = rs.getInt("arc_post_part_cnt");

        this.extra = ExtraFieldJSON.fromJson(rs.getString("extra"));
        return this;
    }

    public TtlInfoRecord copy() {
        TtlInfoRecord newRec = new TtlInfoRecord();

        newRec.setId(null);
        newRec.setGmtCreated(this.gmtCreated);
        newRec.setGmtModified(this.gmtModified);

        newRec.setTableSchema(this.tableSchema);
        newRec.setTableName(this.tableName);

        newRec.setTtlStatus(this.ttlStatus);
        newRec.setTtlExpr(this.ttlExpr);
        newRec.setTtlFilter(this.ttlFilter);
        newRec.setTtlInterval(this.ttlInterval);
        newRec.setTtlUnit(this.ttlUnit);
        newRec.setTtlCol(this.ttlCol);
        newRec.setTtlTimezone(this.ttlTimezone);
        newRec.setTtlCron(this.ttlCron);
        newRec.setTtlBinlog(this.ttlBinlog);

        newRec.setArcKind(this.arcKind);
        newRec.setArcStatus(this.arcStatus);
        newRec.setArcTmpTblSchema(this.arcTmpTblSchema);
        newRec.setArcTmpTblName(this.arcTmpTblName);
        newRec.setArcTblSchema(this.arcTblSchema);
        newRec.setArcTblName(this.arcTblName);

        newRec.setArcPartMode(this.arcPartMode);
        newRec.setArcPartInterval(this.arcPartInterval);
        newRec.setArcPartUnit(this.arcPartUnit);
        newRec.setArcPrePartCnt(this.arcPrePartCnt);
        newRec.setArcPostPartCnt(this.arcPostPartCnt);

        ExtraFieldJSON newJsonField = ExtraFieldJSON.fromJson(this.extra.toString());
        newRec.setExtra(newJsonField);

        return newRec;
    }
}
