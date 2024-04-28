package com.alibaba.polardbx.gms.metadb.trx;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author yaozhili
 */
public class TrxLogStatusRecord implements SystemTableRecord {

    public int status;
    public String currentTableName;
    public Timestamp gmtModified;
    public int flag;
    public Timestamp now;

    @Override
    public TrxLogStatusRecord fill(ResultSet rs) throws SQLException {
        this.status = rs.getInt("status");
        this.currentTableName = rs.getString("current_table_name");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.flag = rs.getInt("flag");
        this.now = rs.getTimestamp("now");
        return this;
    }
}
