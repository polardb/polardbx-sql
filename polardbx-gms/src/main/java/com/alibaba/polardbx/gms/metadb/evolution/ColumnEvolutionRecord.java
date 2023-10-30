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

package com.alibaba.polardbx.gms.metadb.evolution;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.util.FileStorageUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.eclipse.jetty.util.StringUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ColumnEvolutionRecord implements SystemTableRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnEvolutionRecord.class);
    Long fieldId;
    Long ts;

    Timestamp create;
    ColumnsRecord columnRecord;

    public ColumnEvolutionRecord() {
    }

    public ColumnEvolutionRecord(Long fieldId, Long ts, ColumnsRecord columnsRecord) {
        this.fieldId = fieldId;
        this.ts = ts;
        this.columnRecord = columnsRecord;
    }

    @Override
    public ColumnEvolutionRecord fill(ResultSet rs) throws SQLException {
        this.fieldId = rs.getLong("field_id");
        this.ts = rs.getLong("ts");
        this.columnRecord = ColumnEvolutionRecord.deserializeFromJson(rs.getString("column_record"));
        this.create = rs.getTimestamp("gmt_created");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.fieldId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ts);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,
            ColumnEvolutionRecord.serializeToJson(this.columnRecord));
        return params;
    }

    public Long getFieldId() {
        return fieldId;
    }

    public String getFieldIdString() {
        return FileStorageUtil.addIdSuffix(fieldId);
    }

    public Long getTs() {
        return ts;
    }

    public Timestamp getCreate() {
        return create;
    }

    public ColumnsRecord getColumnRecord() {
        return columnRecord;
    }

    public static String serializeToJson(ColumnsRecord columnsRecord) {
        if (columnsRecord == null) {
            return "";
        }

        JSONObject columnRecordJson = new JSONObject();
        columnRecordJson.put("tableSchema", columnsRecord.tableSchema);
        columnRecordJson.put("tableName", columnsRecord.tableName);
        columnRecordJson.put("columnName", columnsRecord.columnName);
        columnRecordJson.put("columnDefault", columnsRecord.columnDefault);
        columnRecordJson.put("isNullable", columnsRecord.isNullable);
        columnRecordJson.put("dataType", columnsRecord.dataType);
        columnRecordJson.put("characterMaximumLength", columnsRecord.characterMaximumLength);
        columnRecordJson.put("characterOctetLength", columnsRecord.characterOctetLength);
        columnRecordJson.put("numericPrecision", columnsRecord.numericPrecision);
        columnRecordJson.put("numericScale", columnsRecord.numericScale);
        columnRecordJson.put("datetimePrecision", columnsRecord.datetimePrecision);
        columnRecordJson.put("characterSetName", columnsRecord.characterSetName);
        columnRecordJson.put("collationName", columnsRecord.collationName);
        columnRecordJson.put("columnType", columnsRecord.columnType);

        columnRecordJson.put("jdbcType", columnsRecord.jdbcType);
        columnRecordJson.put("jdbcTypeName", columnsRecord.jdbcTypeName);
        columnRecordJson.put("fieldLength", columnsRecord.fieldLength);
        return columnRecordJson.toJSONString();
    }

    public static ColumnsRecord deserializeFromJson(String json) {
        try {
            if (StringUtil.isEmpty(json)) {
                return null;
            }
            ColumnsRecord columnsRecord = new ColumnsRecord();
            JSONObject columnRecordJson = JSON.parseObject(json);
            columnsRecord.tableSchema = columnRecordJson.getString("tableSchema");
            columnsRecord.tableName = columnRecordJson.getString("tableName");
            columnsRecord.columnName = columnRecordJson.getString("columnName");
            columnsRecord.columnDefault = columnRecordJson.getString("columnDefault");
            columnsRecord.isNullable = columnRecordJson.getString("isNullable");
            columnsRecord.dataType = columnRecordJson.getString("dataType");
            columnsRecord.characterMaximumLength = columnRecordJson.getLongValue("characterMaximumLength");
            columnsRecord.characterOctetLength = columnRecordJson.getLongValue("characterOctetLength");
            columnsRecord.numericPrecision = columnRecordJson.getLongValue("numericPrecision");
            columnsRecord.numericScale = columnRecordJson.getLongValue("numericScale");
            columnsRecord.datetimePrecision = columnRecordJson.getLongValue("datetimePrecision");
            columnsRecord.collationName = columnRecordJson.getString("collationName");
            columnsRecord.characterSetName = columnRecordJson.getString("characterSetName");
            columnsRecord.columnType = columnRecordJson.getString("columnType");
            columnsRecord.jdbcType = columnRecordJson.getIntValue("jdbcType");
            columnsRecord.jdbcTypeName = columnRecordJson.getString("jdbcTypeName");
            columnsRecord.fieldLength = columnRecordJson.getLongValue("fieldLength");

            columnsRecord.ordinalPosition = 0L;
            columnsRecord.columnKey = "";
            columnsRecord.extra = "";
            columnsRecord.privileges = "";
            columnsRecord.columnComment = "";
            columnsRecord.generationExpression = "";
            columnsRecord.version = 0L;
            columnsRecord.status = 1;
            columnsRecord.flag = 0L;

            return columnsRecord;
        } catch (Throwable e) {
            LOGGER.error("deserializeFromJson error ", e);
            return null;
        }
    }
}
