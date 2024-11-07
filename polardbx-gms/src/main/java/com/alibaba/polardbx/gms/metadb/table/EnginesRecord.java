package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EnginesRecord implements SystemTableRecord {

    public String engine;
    public String support;
    public String comment;
    public String transcations;
    public String XA;
    public String savePoints;

    @Override
    public EnginesRecord fill(ResultSet rs) throws SQLException {
        this.engine = rs.getString("engine");
        this.support = rs.getString("support");
        this.comment = rs.getString("comment");
        this.transcations = rs.getString("transactions");
        this.XA = rs.getString("xa");
        this.savePoints = rs.getString("Savepoints");
        return this;
    }
}
