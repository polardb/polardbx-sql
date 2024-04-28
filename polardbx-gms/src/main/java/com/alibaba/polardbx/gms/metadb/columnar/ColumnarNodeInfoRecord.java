package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarNodeInfoRecord implements SystemTableRecord {
    private String ip;
    private int daemonPort;

    @SuppressWarnings("unchecked")
    @Override
    public ColumnarNodeInfoRecord fill(ResultSet rs) throws SQLException {
        this.ip = rs.getString("ip");
        this.daemonPort = rs.getInt("daemon_port");
        return this;
    }

    public String getIp() {
        return ip;
    }

    public int getDaemonPort() {
        return daemonPort;
    }
}
