package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class SyncPointMetaAccessorTest {
    @Test
    public void testDeleteSuccess() {
        int expectedRows = 1;
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() -> MetaDbUtil.delete(anyString(), Mockito.any(Connection.class)))
                .thenReturn(expectedRows);
            SyncPointMetaAccessor syncPointMetaAccessor = new SyncPointMetaAccessor();
            syncPointMetaAccessor.setConnection(mock(Connection.class));
            int result = syncPointMetaAccessor.delete();

            assertEquals(expectedRows, result);
        }
    }

    @Test
    public void testDeleteFailure() {
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() -> MetaDbUtil.delete(anyString(), Mockito.any(Connection.class)))
                .thenThrow(new SQLException("test"));
            SyncPointMetaAccessor syncPointMetaAccessor = new SyncPointMetaAccessor();
            syncPointMetaAccessor.setConnection(mock(Connection.class));
            try {
                syncPointMetaAccessor.delete();
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage()
                    .contains("Failed to delete the system table cdc_sync_point_meta. Caused by: test"));
            }

        }
    }

    @Test
    public void testInsertSuccess() {
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() ->
                    MetaDbUtil.insert(anyString(), Mockito.any(Map.class), Mockito.any(Connection.class)))
                .thenReturn(1);
            SyncPointMetaAccessor syncPointMetaAccessor = new SyncPointMetaAccessor();
            syncPointMetaAccessor.setConnection(mock(Connection.class));
            int result = syncPointMetaAccessor.insert("abc", 1, 1L);

            assertEquals(1, result);
        }
    }

    @Test
    public void testInsertFailure() {
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() ->
                    MetaDbUtil.insert(anyString(), Mockito.any(Map.class), Mockito.any(Connection.class)))
                .thenThrow(new SQLException("test"));
            SyncPointMetaAccessor syncPointMetaAccessor = new SyncPointMetaAccessor();
            syncPointMetaAccessor.setConnection(mock(Connection.class));

            try {
                syncPointMetaAccessor.insert("abc", 1, 1);
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage()
                    .contains("Failed to insert the system table cdc_sync_point_meta. Caused by: test"));
            }
        }
    }
}
