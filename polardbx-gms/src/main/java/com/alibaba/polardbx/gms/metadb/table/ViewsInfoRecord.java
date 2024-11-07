package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ViewsInfoRecord implements SystemTableRecord {
    public long id;
    public String catalogName;
    public String schemaName;
    public String viewName;
    public String viewDefinition;

    @Override
    public ViewsInfoRecord fill(ResultSet rs) throws SQLException {
        try {
            this.id = rs.getLong("id");
        } catch (Exception ignored) {

        }
        this.catalogName = rs.getString("catalog_name");
        this.schemaName = rs.getString("schema_name");
        this.viewName = rs.getString("view_name");
        this.viewDefinition = rs.getString("view_definition");
        return this;
    }
}
