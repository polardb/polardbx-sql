package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DBVariableRecord implements SystemTableRecord {
    public String variableName;
    public String value;

    @Override
    public DBVariableRecord fill(ResultSet rs) throws SQLException {
        this.variableName = rs.getString("variable_name");
        this.value = rs.getString("value");
        return this;
    }
}
