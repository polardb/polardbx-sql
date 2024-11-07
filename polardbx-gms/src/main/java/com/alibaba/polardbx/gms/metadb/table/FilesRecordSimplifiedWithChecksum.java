package com.alibaba.polardbx.gms.metadb.table;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FilesRecordSimplifiedWithChecksum extends FilesRecordSimplified {
    public Long checksum;
    public Long deletedChecksum;

    @Override
    public FilesRecordSimplified fill(ResultSet rs) throws SQLException {
        super.fill(rs);
        this.checksum = rs.getLong("checksum");
        this.deletedChecksum = rs.getLong("deleted_checksum");
        return this;
    }
}
