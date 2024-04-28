package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author yudong
 * @since 2024/1/8 14:11
 **/
public class CdcConfigRecord implements SystemTableRecord {
    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String configKey;
    public String configValue;

    @Override
    public CdcConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.configKey = rs.getString("config_key");
        this.configValue = rs.getString("config_value");
        return this;
    }
}
