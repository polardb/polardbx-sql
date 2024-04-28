package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarLeaseRecord implements SystemTableRecord {
    public int id;
    public String owner;
    public long lease;

    @Override
    public ColumnarLeaseRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getInt("id");
        this.owner = rs.getString("owner");
        this.lease = rs.getLong("lease");
        return this;
    }
}
