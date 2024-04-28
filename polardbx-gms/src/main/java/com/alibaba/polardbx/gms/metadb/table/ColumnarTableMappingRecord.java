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
        return params;
    }
}
