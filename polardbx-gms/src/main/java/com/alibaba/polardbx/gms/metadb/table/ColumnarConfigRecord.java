package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ColumnarConfigRecord implements SystemTableRecord {
    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public long tableId;
    public String configKey;
    public String configValue;

    @Override
    public ColumnarConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.tableId = rs.getLong("table_id");
        this.configKey = rs.getString("config_key");
        this.configValue = rs.getString("config_value");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.configKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.configValue);

        return params;
    }
}
