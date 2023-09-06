package com.alibaba.polardbx.gms.metadb.evolution;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.util.FileStorageUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnMappingRecord implements SystemTableRecord {
    long fieldId;
    public String tableSchema;
    public String tableName;
    public String columnName;

    public ColumnMappingRecord() {
    }

    public ColumnMappingRecord(String tableSchema, String tableName, String columnName) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public ColumnMappingRecord fill(ResultSet rs) throws SQLException {
        this.fieldId = rs.getLong("field_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.columnName = rs.getString("column_name");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        return params;
    }

    public static List<ColumnMappingRecord> fromColumnRecord(List<ColumnsRecord> columnsRecord) {
        return columnsRecord.stream()
            .map(x -> new ColumnMappingRecord(x.tableSchema, x.tableName, x.columnName))
            .collect(Collectors.toList());
    }

    public String getColumnName() {
        return columnName.toLowerCase();
    }

    public String getFieldIdString() {
        return FileStorageUtil.addIdSuffix(fieldId);
    }

    public Long getFieldId() {
        return fieldId;
    }
}
