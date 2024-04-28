package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ChecksumRecord implements SystemTableRecord {
    public String table;
    public long checksum;

    @Override
    public ChecksumRecord fill(ResultSet rs) throws SQLException {
        this.table = rs.getString("Table");
        this.checksum = rs.getLong("Checksum");
        return this;
    }
}
