package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yaozhili
 */
public class SyncPointMetaRecord implements SystemTableRecord {
    public Integer id;
    public Integer participants;
    public Long tso;

    @Override
    public SyncPointMetaRecord fill(ResultSet rs) throws SQLException {
        SyncPointMetaRecord record = new SyncPointMetaRecord();
        record.id = rs.getInt("id");
        record.participants = rs.getInt("participants");
        record.tso = rs.getLong("tso");
        return record;
    }
}
