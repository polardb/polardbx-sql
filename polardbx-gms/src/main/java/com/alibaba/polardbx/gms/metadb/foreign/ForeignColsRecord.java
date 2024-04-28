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

public class ForeignColsRecord implements SystemTableRecord {
    public long id;
    public String schemaName;
    public String tableName;
    public String indexName;
    public String forColName;
    public String refColName;
    public long pos;

    public ForeignColsRecord() {
    }

    public ForeignColsRecord(String schemaName, String tableName, String indexName, String forColName,
                             String refColName, long pos) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        this.forColName = forColName;
        this.refColName = refColName;
        this.pos = pos;
    }

    @Override
    public ForeignColsRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.forColName = rs.getString("for_col_name");
        this.refColName = rs.getString("ref_col_name");
        this.pos = rs.getLong("pos");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(7);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.forColName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.refColName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.pos);
        return params;
    }
}
