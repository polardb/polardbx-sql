package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EncdbKey implements SystemTableRecord {

    private long id;

    private String key;

    private String type;

    @Override
    public EncdbKey fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.key = rs.getString("key");
        this.type = KeyType.valueOf(rs.getString("type")).name();
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getType() {
        return type;
    }

    public long getId() {
        return id;
    }

    public static enum KeyType {
        MEK_HASH, MEK_ENC;
    }
}
