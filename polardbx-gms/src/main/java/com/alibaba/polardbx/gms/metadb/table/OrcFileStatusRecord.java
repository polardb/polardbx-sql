package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

@Data
public class OrcFileStatusRecord implements SystemTableRecord {

    public long fileCounts;
    public long rowCounts;
    public long fileSizes;

    @Override
    public OrcFileStatusRecord fill(ResultSet rs) throws SQLException {
        this.fileCounts = rs.getLong("file_counts");
        this.rowCounts = rs.getLong("row_counts");
        this.fileSizes = rs.getLong("file_sizes");
        return this;
    }
}
