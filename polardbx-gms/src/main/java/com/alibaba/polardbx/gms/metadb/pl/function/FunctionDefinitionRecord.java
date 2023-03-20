package com.alibaba.polardbx.gms.metadb.pl.function;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class FunctionDefinitionRecord extends FunctionMetaRecord {
    public String definition;

    @Override
    public FunctionDefinitionRecord fill(ResultSet rs) throws SQLException {
        FunctionDefinitionRecord record = new FunctionDefinitionRecord();
        record.name = rs.getString("ROUTINE_NAME");
        record.routineMeta = rs.getString("ROUTINE_META");
        record.canPush = Optional.ofNullable(rs.getString("SQL_DATA_ACCESS")).map(String::trim)
            .map(t -> t.equalsIgnoreCase("NO SQL")).orElse(false);
        record.definition = rs.getString("ROUTINE_DEFINITION");
        return record;
    }
}
