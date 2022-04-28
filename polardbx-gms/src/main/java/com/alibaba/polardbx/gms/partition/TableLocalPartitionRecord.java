/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    public String archiveTableSchema;
    public String archiveTableName;

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

        this.archiveTableSchema = rs.getString("archive_table_schema");
        this.archiveTableName = rs.getString("archive_table_name");

        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.intervalCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.intervalUnit);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.expireAfterCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.preAllocateCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.pivotDateExpr);

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.archiveTableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.archiveTableName);
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

    public String getArchiveTableName() {
        return archiveTableName;
    }

    public void setArchiveTableName(String archiveTableName) {
        this.archiveTableName = archiveTableName;
    }

    public String getArchiveTableSchema() {
        return archiveTableSchema;
    }

    public void setArchiveTableSchema(String archiveTableSchema) {
        this.archiveTableSchema = archiveTableSchema;
    }
}