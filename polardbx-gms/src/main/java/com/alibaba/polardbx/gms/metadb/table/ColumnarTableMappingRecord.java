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

public class ColumnarTableMappingRecord implements SystemTableRecord {
    public long tableId;
    public String tableSchema;
    public String tableName;
    public String indexName;
    public long latestVersionId;
    public String status;
    public String extra;
    public String type;

    public ColumnarTableMappingRecord() {
    }

    public ColumnarTableMappingRecord(String tableSchema, String tableName, String indexName, long latestVersionId,
                                      String status) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.indexName = indexName;
        this.latestVersionId = latestVersionId;
        this.status = status;
    }

    @Override
    public ColumnarTableMappingRecord fill(ResultSet rs) throws SQLException {
        this.tableId = rs.getLong("table_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.latestVersionId = rs.getLong("latest_version_id");
        this.status = rs.getString("status");
        this.extra = rs.getString("extra");
        this.type = rs.getString("type");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.latestVersionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.type);
        return params;
    }
}
