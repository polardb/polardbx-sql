package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Data
public class ColumnarFileIdInfoRecord implements SystemTableRecord {
    private long id;
    private String type;
    private long maxId;
    private int step;
    private long version;

    private Timestamp createTime;
    private Timestamp updateTime;

    @Override
    public ColumnarFileIdInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.type = rs.getString("type");
        this.maxId = rs.getLong("max_id");
        this.step = rs.getInt("step");
        this.version = rs.getLong("version");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(7);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.type);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.step);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        // skip automatically updated column: create_time and update_time
        return params;
    }

    public long getMaxId() {
        return this.maxId;
    }

    public int getStep() {
        return this.step;
    }

    public long getId() {
        return this.id;
    }

    public long getVersion() {
        return this.version;
    }

    public String getType() {
        return this.type;
    }
}
