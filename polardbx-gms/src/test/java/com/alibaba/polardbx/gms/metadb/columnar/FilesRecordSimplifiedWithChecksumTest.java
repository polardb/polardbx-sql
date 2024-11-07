package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilesRecordSimplifiedWithChecksumTest {
    @Test
    public void testFill() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("file_name")).thenReturn("test_file");
        when(rs.getLong("commit_ts")).thenReturn(100L);
        when(rs.getLong("remove_ts")).thenReturn(100L);
        when(rs.getString("partition_name")).thenReturn("test_partition");
        when(rs.getLong("schema_ts")).thenReturn(100L);
        when(rs.getLong("checksum")).thenReturn(1024L);
        when(rs.getLong("deleted_checksum")).thenReturn(4096L);
        when(rs.wasNull()).thenReturn(false);

        FilesRecordSimplifiedWithChecksum filesRecordSimplifiedWithChecksum = new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fill(rs);
        Assert.assertEquals(Long.valueOf(1024L), filesRecordSimplifiedWithChecksum.checksum);
        Assert.assertEquals(Long.valueOf(4096L), filesRecordSimplifiedWithChecksum.deletedChecksum);
    }
}
