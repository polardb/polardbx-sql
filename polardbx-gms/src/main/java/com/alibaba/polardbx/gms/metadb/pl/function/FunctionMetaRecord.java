package com.alibaba.polardbx.gms.metadb.pl.function;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class FunctionMetaRecord implements SystemTableRecord {
    public String name;
    public String routineMeta;
    public boolean canPush;

    @Override
    public FunctionMetaRecord fill(ResultSet rs) throws SQLException {
        FunctionMetaRecord record = new FunctionMetaRecord();
        record.name = rs.getString("ROUTINE_NAME");
        record.routineMeta = rs.getString("ROUTINE_META");
        record.canPush = Optional.ofNullable(rs.getString("SQL_DATA_ACCESS")).map(String::trim)
            .map(t -> t.equalsIgnoreCase("NO SQL")).orElse(false);
        return record;
    }
}
