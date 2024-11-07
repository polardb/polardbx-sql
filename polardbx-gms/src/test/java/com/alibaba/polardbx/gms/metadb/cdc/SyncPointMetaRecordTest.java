package com.alibaba.polardbx.gms.metadb.cdc;

import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class SyncPointMetaRecordTest {
    @Test
    public void testFill() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);

        Mockito.when(rs.getInt("id")).thenReturn(1);
        Mockito.when(rs.getInt("participants")).thenReturn(3);
        Mockito.when(rs.getLong("tso")).thenReturn(123456789L);

        SyncPointMetaRecord record = new SyncPointMetaRecord();

        SyncPointMetaRecord result = record.fill(rs);

        assertEquals(Integer.valueOf(1), result.id);
        assertEquals(Integer.valueOf(3), result.participants);
        assertEquals(Long.valueOf(123456789L), result.tso);
    }
}
