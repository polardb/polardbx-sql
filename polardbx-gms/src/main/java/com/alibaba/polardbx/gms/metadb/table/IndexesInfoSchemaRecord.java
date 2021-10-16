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

public class IndexesInfoSchemaRecord implements SystemTableRecord {

    public String tableSchema;
    public String tableName;
    public long nonUnique;
    public String indexSchema;
    public String indexName;
    public long seqInIndex;
    public String columnName;
    public String collation;
    public long cardinality;
    public long subPart;
    public String packed;
    public String nullable;
    public String indexType;
    public String comment;
    public String indexComment;

    @Override
    public IndexesInfoSchemaRecord fill(ResultSet rs) throws SQLException {
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.nonUnique = rs.getLong("non_unique");
        this.indexSchema = rs.getString("index_schema");
        this.indexName = rs.getString("index_name");
        this.seqInIndex = rs.getLong("seq_in_index");
        this.columnName = rs.getString("column_name");
        this.collation = rs.getString("collation");
        this.cardinality = rs.getLong("cardinality");
        this.subPart = rs.getLong("sub_part");
        this.packed = rs.getString("packed");
        this.nullable = rs.getString("nullable");
        this.indexType = rs.getString("index_type");
        this.comment = rs.getString("comment");
        this.indexComment = rs.getString("index_comment");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(15);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.nonUnique);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.seqInIndex);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.collation);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.cardinality);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.subPart);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.packed);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.nullable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.comment);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexComment);
        return params;
    }

}
