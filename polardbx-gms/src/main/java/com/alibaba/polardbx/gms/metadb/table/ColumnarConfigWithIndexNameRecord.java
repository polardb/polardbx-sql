package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarConfigWithIndexNameRecord implements SystemTableRecord {
    public String indexName;
    public long tableId;
    public String configValue;
    public String configKey;
    @Override
    public ColumnarConfigWithIndexNameRecord fill(ResultSet rs) throws SQLException {
        this.indexName = rs.getString(1);
        this.tableId = rs.getLong("table_id");
        this.configValue = rs.getString("config_value");
        this.configKey = rs.getString("config_key");
        return this;
    }
}
