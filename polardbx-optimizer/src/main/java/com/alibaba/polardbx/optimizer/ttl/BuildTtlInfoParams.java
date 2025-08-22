package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;

/**
 * @author chenghui.lch
 */
public class BuildTtlInfoParams {

    protected TtlDefinitionInfo oldTtlInfo;

    protected String tableSchema;
    protected String tableName;
    protected String ttlEnable;
    protected SqlTimeToLiveExpr ttlExpr;
    protected SqlTimeToLiveJobExpr ttlJob;
    protected String ttlFilter;
    protected String ttlCleanup;
    protected SqlNode ttlPartInterval;
    protected String archiveKind;
    protected String archiveTableSchema;
    protected String archiveTableName;
    protected Integer arcPreAllocateCount;
    protected Integer arcPostAllocateCount;
    protected TableMeta ttlTableMeta;
    protected ExecutionContext ec;

    public BuildTtlInfoParams() {

    }

    public TtlDefinitionInfo getOldTtlInfo() {
        return oldTtlInfo;
    }

    public void setOldTtlInfo(TtlDefinitionInfo oldTtlInfo) {
        this.oldTtlInfo = oldTtlInfo;
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

    public String getTtlEnable() {
        return ttlEnable;
    }

    public void setTtlEnable(String ttlEnable) {
        this.ttlEnable = ttlEnable;
    }

    public SqlTimeToLiveExpr getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SqlTimeToLiveExpr ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SqlTimeToLiveJobExpr getTtlJob() {
        return ttlJob;
    }

    public void setTtlJob(SqlTimeToLiveJobExpr ttlJob) {
        this.ttlJob = ttlJob;
    }

    public String getTtlFilter() {
        return ttlFilter;
    }

    public void setTtlFilter(String ttlFilter) {
        this.ttlFilter = ttlFilter;
    }

    public String getTtlCleanup() {
        return ttlCleanup;
    }

    public void setTtlCleanup(String ttlCleanup) {
        this.ttlCleanup = ttlCleanup;
    }

    public SqlNode getTtlPartInterval() {
        return ttlPartInterval;
    }

    public void setTtlPartInterval(SqlNode ttlPartInterval) {
        this.ttlPartInterval = ttlPartInterval;
    }

    public String getArchiveKind() {
        return archiveKind;
    }

    public void setArchiveKind(String archiveKind) {
        this.archiveKind = archiveKind;
    }

    public String getArchiveTableSchema() {
        return archiveTableSchema;
    }

    public void setArchiveTableSchema(String archiveTableSchema) {
        this.archiveTableSchema = archiveTableSchema;
    }

    public String getArchiveTableName() {
        return archiveTableName;
    }

    public void setArchiveTableName(String archiveTableName) {
        this.archiveTableName = archiveTableName;
    }

    public Integer getArcPreAllocateCount() {
        return arcPreAllocateCount;
    }

    public void setArcPreAllocateCount(Integer arcPreAllocateCount) {
        this.arcPreAllocateCount = arcPreAllocateCount;
    }

    public Integer getArcPostAllocateCount() {
        return arcPostAllocateCount;
    }

    public void setArcPostAllocateCount(Integer arcPostAllocateCount) {
        this.arcPostAllocateCount = arcPostAllocateCount;
    }

    public TableMeta getTtlTableMeta() {
        return ttlTableMeta;
    }

    public void setTtlTableMeta(TableMeta ttlTableMeta) {
        this.ttlTableMeta = ttlTableMeta;
    }

    public ExecutionContext getEc() {
        return ec;
    }

    public void setEc(ExecutionContext ec) {
        this.ec = ec;
    }
}
