package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author chenghui.lch
 */
public class ConfigListenerDataIdRecord implements SystemTableRecord {

    public String dataId;

    @Override
    public ConfigListenerDataIdRecord fill(ResultSet rs) throws SQLException {
        this.dataId = rs.getString("data_id");
        return this;
    }
}
