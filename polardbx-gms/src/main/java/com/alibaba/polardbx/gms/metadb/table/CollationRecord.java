package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CollationRecord implements SystemTableRecord {
    public String collationName;
    public String characterSetName;
    public Long id;
    public String isDefault;
    public String isCompiled;
    public Long sortLen;
    public String padAttribute;

    @Override
    public CollationRecord fill(ResultSet rs) throws SQLException {
        this.collationName = rs.getString("collation_name");
        this.characterSetName = rs.getString("character_set_name");
        this.id = rs.getLong("id");
        this.isDefault = rs.getString("is_default");
        this.isCompiled = rs.getString("is_compiled");
        this.sortLen = rs.getLong("sortlen");
        this.padAttribute = rs.getString("pad_attribute");
        return this;
    }
}
