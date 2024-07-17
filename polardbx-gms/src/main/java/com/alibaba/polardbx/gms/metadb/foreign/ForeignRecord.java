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

package com.alibaba.polardbx.gms.metadb.foreign;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wenki
 */
public class ForeignRecord implements SystemTableRecord {
    public long id;
    public String schemaName;
    public String tableName;
    public String indexName;
    public String constraintName;
    public String refSchemaName;
    public String refTableName;
    public String refIndexName;
    public long nCols;
    public String updateRule;
    public String deleteRule;
    public long pushDown;

    @Override
    public ForeignRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.constraintName = rs.getString("constraint_name");
        this.refSchemaName = rs.getString("ref_schema_name");
        this.refTableName = rs.getString("ref_table_name");
        this.refIndexName = rs.getString("ref_index_name");
        this.nCols = rs.getLong("n_cols");
        this.updateRule = rs.getString("update_rule");
        this.deleteRule = rs.getString("delete_rule");
        this.pushDown = rs.getLong("push_down");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(12);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.constraintName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.refSchemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.refTableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.refIndexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.nCols);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.updateRule);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.deleteRule);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.pushDown);
        return params;
    }
}
