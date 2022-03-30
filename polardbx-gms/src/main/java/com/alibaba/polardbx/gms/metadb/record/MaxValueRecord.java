package com.alibaba.polardbx.gms.metadb.record;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxValueRecord implements SystemTableRecord {

    public long maxValue;

    @Override
    public MaxValueRecord fill(ResultSet rs) throws SQLException {
        this.maxValue = rs.getLong(1);
        return this;
    }

}
