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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnMetasRecord implements SystemTableRecord {
    public long columnMetaId;

    // file info
    public String tableFileName;
    public String tableName;
    public String tableSchema;

    // stripe info
    public long stripeIndex;
    public long stripeOffset;
    public long stripeLength;

    // column info
    public String columnName;
    public long columnIndex;
    public String bloomFilterPath;
    public long bloomFilterOffset;
    public long bloomFilterLength;
    public long isMerged;

    public String createTime;
    public String updateTime;
    public long taskId;
    public long lifeCycle;

    public String engine;

    public String logicalSchemaName;
    public String logicalTableName;

    @Override
    public ColumnMetasRecord fill(ResultSet rs) throws SQLException {
        this.columnMetaId = rs.getLong("column_meta_id");
        this.tableFileName = rs.getString("table_file_name");
        this.tableName = rs.getString("table_name");
        this.tableSchema = rs.getString("table_schema");
        this.stripeIndex = rs.getLong("stripe_index");
        this.stripeOffset = rs.getLong("stripe_offset");
        this.stripeLength = rs.getLong("stripe_length");
        this.columnName = rs.getString("column_name");
        this.columnIndex = rs.getLong("column_index");
        this.bloomFilterPath = rs.getString("bloom_filter_path");
        this.bloomFilterOffset = rs.getLong("bloom_filter_offset");
        this.bloomFilterLength = rs.getLong("bloom_filter_length");
        this.isMerged = rs.getLong("is_merged");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        this.taskId = rs.getInt("task_id");
        this.lifeCycle = rs.getLong("life_cycle");
        this.engine = rs.getString("engine");
        this.logicalSchemaName = rs.getString("logical_schema_name");
        this.logicalTableName = rs.getString("logical_table_name");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(9);
        int index = 0;

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableFileName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.stripeIndex);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.stripeOffset);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.stripeLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.columnIndex);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.bloomFilterPath);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.bloomFilterOffset);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.bloomFilterLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.isMerged);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.lifeCycle);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalSchemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalTableName);
        return params;
    }
}
