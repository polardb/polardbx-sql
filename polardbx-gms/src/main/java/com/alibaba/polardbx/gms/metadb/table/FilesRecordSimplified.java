package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FilesRecordSimplified implements SystemTableRecord {

    public String fileName;
    public String partitionName;
    public Long commitTs;
    public Long removeTs;
    public Long schemaTs;

    @Override
    public FilesRecordSimplified fill(ResultSet rs) throws SQLException {
        this.fileName = rs.getString("file_name");
        this.commitTs = rs.getLong("commit_ts");
        if (rs.wasNull()) {
            this.commitTs = null;
        }
        this.removeTs = rs.getLong("remove_ts");
        if (rs.wasNull()) {
            this.removeTs = null;
        }
        this.partitionName = rs.getString("partition_name");
        this.schemaTs = rs.getLong("schema_ts");
        if (rs.wasNull()) {
            this.schemaTs = null;
        }

        return this;
    }
}
