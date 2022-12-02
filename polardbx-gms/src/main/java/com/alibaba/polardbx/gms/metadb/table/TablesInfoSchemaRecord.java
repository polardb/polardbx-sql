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

public class TablesInfoSchemaRecord implements SystemTableRecord {

    public long id;
    public String tableSchema;
    public String tableName;
    public String tableType;
    public String engine;
    public long version;
    public String rowFormat;
    public long tableRows;
    public long avgRowLength;
    public long dataLength;
    public long maxDataLength;
    public long indexLength;
    public long dataFree;
    public long autoIncrement;
    public String tableCollation;
    public long checkSum;
    public String createOptions;
    public String tableComment;

    @Override
    public TablesInfoSchemaRecord fill(ResultSet rs) throws SQLException {
        try {
            this.id = rs.getLong("id");
        } catch (Exception ignored) {
        }
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.tableType = rs.getString("table_type");
        this.engine = rs.getString("engine");
        this.version = rs.getLong("version");
        this.rowFormat = rs.getString("row_format");
        this.tableRows = rs.getLong("table_rows");
        this.avgRowLength = rs.getLong("avg_row_length");
        this.dataLength = rs.getLong("data_length");
        this.maxDataLength = rs.getLong("max_data_length");
        this.indexLength = rs.getLong("index_length");
        this.dataFree = rs.getLong("data_free");
        this.autoIncrement = rs.getLong("auto_increment");
        this.tableCollation = rs.getString("table_collation");
        this.checkSum = rs.getLong("checksum");
        this.createOptions = rs.getString("create_options");
        this.tableComment = rs.getString("table_comment");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(17);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        setCommonParams(params, index);
        return params;
    }

    protected Map<Integer, ParameterContext> buildUpdateParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        setCommonParams(params, index);
        return params;
    }

    private void setCommonParams(Map<Integer, ParameterContext> params, int index) {
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.rowFormat);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableRows);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.avgRowLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.dataLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxDataLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.indexLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.dataFree);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.autoIncrement);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableCollation);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.checkSum);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.createOptions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableComment);
    }

    @Override
    public String toString() {
        return "TablesInfoSchemaRecord{" +
            "id=" + id +
            ", tableSchema='" + tableSchema + '\'' +
            ", tableName='" + tableName + '\'' +
            ", tableType='" + tableType + '\'' +
            ", engine='" + engine + '\'' +
            ", version=" + version +
            ", rowFormat='" + rowFormat + '\'' +
            ", tableRows=" + tableRows +
            ", avgRowLength=" + avgRowLength +
            ", dataLength=" + dataLength +
            ", maxDataLength=" + maxDataLength +
            ", indexLength=" + indexLength +
            ", dataFree=" + dataFree +
            ", autoIncrement=" + autoIncrement +
            ", tableCollation='" + tableCollation + '\'' +
            ", checkSum=" + checkSum +
            ", createOptions='" + createOptions + '\'' +
            ", tableComment='" + tableComment + '\'' +
            '}';
    }
}
