package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wenki
 */
public class ColumnarTaskRecord implements SystemTableRecord {
    public String clusterId;
    public String taskName;
    public String ip;
    public long port;
    public String role;

    public ColumnarTaskRecord() {
    }

    public ColumnarTaskRecord(String clusterId, String taskName, String ip, long port, String role) {
        this.clusterId = clusterId;
        this.taskName = taskName;
        this.ip = ip;
        this.port = port;
        this.role = role;
    }

    @Override
    public ColumnarTaskRecord fill(ResultSet rs) throws SQLException {
        this.clusterId = rs.getString("cluster_id");
        this.taskName = rs.getString("task_name");
        this.ip = rs.getString("ip");
        this.port = rs.getLong("port");
        this.role = rs.getString("role");

        return this;
    }
}
