package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class CommonIntegerRecord implements SystemTableRecord {
    public int value;

    @Override
    public CommonIntegerRecord fill(ResultSet rs) throws SQLException {
        value = rs.getInt(1);
        return this;
    }
}
