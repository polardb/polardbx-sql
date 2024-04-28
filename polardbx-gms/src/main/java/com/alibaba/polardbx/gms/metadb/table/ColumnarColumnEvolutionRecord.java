package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ColumnarColumnEvolutionRecord implements SystemTableRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    public long id;
    public long fieldId;
    public long tableId;
    public String columnName;
    public long versionId;
    public long ddlJobId;

    public Timestamp create;
    public ColumnsRecord columnsRecord;

    public ColumnarColumnEvolutionRecord() {
    }

    public ColumnarColumnEvolutionRecord(long tableId, String columnName, long versionId, long ddlJobId,
                                         ColumnsRecord columnsRecord) {
        this.tableId = tableId;
        this.columnName = columnName;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.columnsRecord = columnsRecord;
    }

    public ColumnarColumnEvolutionRecord(long tableId, long fieldId, String columnName, long versionId, long ddlJobId,
                                         ColumnsRecord columnsRecord) {
        this.tableId = tableId;
        this.fieldId = fieldId;
        this.columnName = columnName;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.columnsRecord = columnsRecord;
    }

    public static String serializeToJson(ColumnsRecord columnsRecord) {
        JSONObject columnRecordJson = new JSONObject();

        columnRecordJson.put("tableSchema", columnsRecord.tableSchema);
        columnRecordJson.put("tableName", columnsRecord.tableName);
        columnRecordJson.put("columnName", columnsRecord.columnName);
        columnRecordJson.put("ordinalPosition", columnsRecord.ordinalPosition);
        columnRecordJson.put("columnDefault", columnsRecord.columnDefault);
        columnRecordJson.put("isNullable", columnsRecord.isNullable);
        columnRecordJson.put("dataType", columnsRecord.dataType);
        columnRecordJson.put("characterMaximumLength", columnsRecord.characterMaximumLength);
        columnRecordJson.put("characterOctetLength", columnsRecord.characterOctetLength);
        columnRecordJson.put("numericPrecision", columnsRecord.numericPrecision);
        columnRecordJson.put("numericScale", columnsRecord.numericScale);
        columnRecordJson.put("datetimePrecision", columnsRecord.datetimePrecision);
        columnRecordJson.put("collationName", columnsRecord.collationName);
        columnRecordJson.put("characterSetName", columnsRecord.characterSetName);
        columnRecordJson.put("columnType", columnsRecord.columnType);
        columnRecordJson.put("columnKey", columnsRecord.columnKey);
        columnRecordJson.put("extra", columnsRecord.extra);
        columnRecordJson.put("privileges", columnsRecord.privileges);
        columnRecordJson.put("columnComment", columnsRecord.columnComment);
        columnRecordJson.put("generationExpression", columnsRecord.generationExpression);
        columnRecordJson.put("jdbcType", columnsRecord.jdbcType);
        columnRecordJson.put("jdbcTypeName", columnsRecord.jdbcTypeName);
        columnRecordJson.put("fieldLength", columnsRecord.fieldLength);
        columnRecordJson.put("version", columnsRecord.version);
        columnRecordJson.put("status", columnsRecord.status);
        columnRecordJson.put("flag", columnsRecord.flag);

        return columnRecordJson.toJSONString();
    }

    public static ColumnsRecord deserializeFromJson(String json) {
        ColumnsRecord columnsRecord = new ColumnsRecord();
        JSONObject columnRecordJson = JSON.parseObject(json);

        columnsRecord.tableSchema = columnRecordJson.getString("tableSchema");
        columnsRecord.tableName = columnRecordJson.getString("tableName");
        columnsRecord.columnName = columnRecordJson.getString("columnName");
        columnsRecord.ordinalPosition = columnRecordJson.getLongValue("ordinalPosition");
        columnsRecord.columnDefault = columnRecordJson.getString("columnDefault");
        columnsRecord.isNullable = columnRecordJson.getString("isNullable");
        columnsRecord.dataType = columnRecordJson.getString("dataType");
        columnsRecord.characterMaximumLength = columnRecordJson.getLongValue("characterMaximumLength");
        columnsRecord.characterOctetLength = columnRecordJson.getLongValue("characterOctetLength");
        columnsRecord.numericPrecision = columnRecordJson.getLongValue("numericPrecision");
        columnsRecord.numericScale = columnRecordJson.getLongValue("numericScale");
        columnsRecord.datetimePrecision = columnRecordJson.getLongValue("datetimePrecision");
        columnsRecord.characterSetName = columnRecordJson.getString("characterSetName");
        columnsRecord.collationName = columnRecordJson.getString("collationName");
        columnsRecord.columnType = columnRecordJson.getString("columnType");
        columnsRecord.columnKey = columnRecordJson.getString("columnKey");
        columnsRecord.extra = columnRecordJson.getString("extra");
        columnsRecord.privileges = columnRecordJson.getString("privileges");
        columnsRecord.columnComment = columnRecordJson.getString("columnComment");
        columnsRecord.generationExpression = columnRecordJson.getString("generationExpression");
        columnsRecord.jdbcType = columnRecordJson.getIntValue("jdbcType");
        columnsRecord.jdbcTypeName = columnRecordJson.getString("jdbcTypeName");
        columnsRecord.fieldLength = columnRecordJson.getLongValue("fieldLength");
        columnsRecord.version = columnRecordJson.getLongValue("version");
        columnsRecord.status = columnRecordJson.getIntValue("status");
        columnsRecord.flag = columnRecordJson.getLongValue("flag");

        return columnsRecord;
    }

    @Override
    public ColumnarColumnEvolutionRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.fieldId = rs.getLong("field_id");
        this.tableId = rs.getLong("table_id");
        this.columnName = rs.getString("column_name");
        this.versionId = rs.getLong("version_id");
        this.ddlJobId = rs.getLong("ddl_job_id");
        this.columnsRecord = deserializeFromJson(rs.getString("columns_record"));
        this.create = rs.getTimestamp("gmt_created");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.fieldId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.versionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ddlJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.columnsRecord));
        return params;
    }

    public Map<Integer, ParameterContext> buildNewInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.columnsRecord));
        return params;
    }
}
