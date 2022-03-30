package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TableLocalPartitionRecord implements SystemTableRecord {

    public Long id;
    public Date createTime;
    public Date updateTime;
    public String tableSchema;
    public String tableName;
    public String columnName;

    public int intervalCount;
    public String intervalUnit;
    public int expireAfterCount;
    public int preAllocateCount;
    public String pivotDateExpr;

    @Override
    public TableLocalPartitionRecord fill(ResultSet rs) throws SQLException {

        this.id = rs.getLong("id");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.columnName = rs.getString("column_name");

        this.intervalCount = rs.getInt("interval_count");
        this.intervalUnit = rs.getString("interval_unit");
        this.expireAfterCount = rs.getInt("expire_after_count");
        this.preAllocateCount = rs.getInt("pre_allocate_count");
        this.pivotDateExpr = rs.getString("pivot_date_expr");

        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.id);
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, this.createTime);
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, this.updateTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.intervalCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.intervalUnit);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.expireAfterCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.preAllocateCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.pivotDateExpr);
        return params;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(final Long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(final Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(final Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getTableSchema() {
        return this.tableSchema;
    }

    public void setTableSchema(final String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public int getIntervalCount() {
        return this.intervalCount;
    }

    public void setIntervalCount(final int intervalCount) {
        this.intervalCount = intervalCount;
    }

    public String getIntervalUnit() {
        return this.intervalUnit;
    }

    public void setIntervalUnit(final String intervalUnit) {
        this.intervalUnit = intervalUnit;
    }

    public int getExpireAfterCount() {
        return this.expireAfterCount;
    }

    public void setExpireAfterCount(final int expireAfterCount) {
        this.expireAfterCount = expireAfterCount;
    }

    public int getPreAllocateCount() {
        return this.preAllocateCount;
    }

    public void setPreAllocateCount(final int preAllocateCount) {
        this.preAllocateCount = preAllocateCount;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public String getPivotDateExpr() {
        return this.pivotDateExpr;
    }

    public void setPivotDateExpr(final String pivotDateExpr) {
        this.pivotDateExpr = pivotDateExpr;
    }

}