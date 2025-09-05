package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.manager.handler.SyncHandler;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author chenghui.lch
 * Test sync handler
 */
public class SyncHandlerTest {

    @Test
    public void testSyncHandler() {
        try {

            SchemaConfig sc = mock(SchemaConfig.class);
            when(sc.isDropped()).thenReturn(false);

            TDataSource ds = mock(TDataSource.class);
            ds.setSchemaName("mymockdb");
            when(sc.getDataSource()).thenReturn(ds);
            when(ds.isInited()).thenReturn(true);

            SyncHandler.initDataSourceForSyncIfNeed(sc, "mymockdb");

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testSyncHandler2() {
        try {

            SchemaConfig sc = mock(SchemaConfig.class);
            when(sc.isDropped()).thenReturn(false);

            TDataSource ds = mock(TDataSource.class);
            ds.setSchemaName("mymockdb");
            when(sc.getDataSource()).thenReturn(ds);
            when(ds.isInited()).thenReturn(true);

            try (MockedStatic<TDataSourceInitUtils> utilsStatic = mockStatic(TDataSourceInitUtils.class)) {
                SQLException myEx = new SQLException("reason", "08S01");
                utilsStatic.when(() -> TDataSourceInitUtils.initDataSource(ds)).thenReturn(myEx);
                SyncHandler.initDataSourceForSyncIfNeed(sc, "mymockdb");
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testSyncHandler3() {
        try {

            SchemaConfig sc = mock(SchemaConfig.class);
            when(sc.isDropped()).thenReturn(false);

            TDataSource ds = mock(TDataSource.class);
            ds.setSchemaName("mymockdb");
            when(sc.getDataSource()).thenReturn(null);
            when(ds.isInited()).thenReturn(true);

            try (MockedStatic<TDataSourceInitUtils> utilsStatic = mockStatic(TDataSourceInitUtils.class)) {
                Throwable myEx = mock(SQLException.class);
                utilsStatic.when(() -> TDataSourceInitUtils.initDataSource(ds)).thenReturn(null);
                SyncHandler.initDataSourceForSyncIfNeed(sc, "mymockdb");
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testSyncHandler4() {
        try {

            SchemaConfig sc = mock(SchemaConfig.class);
            when(sc.isDropped()).thenReturn(true);

            TDataSource ds = mock(TDataSource.class);
            ds.setSchemaName("mymockdb");
            when(sc.getDataSource()).thenReturn(ds);
            when(ds.isInited()).thenReturn(true);

            try (MockedStatic<TDataSourceInitUtils> utilsStatic = mockStatic(TDataSourceInitUtils.class)) {
                Throwable myEx = mock(SQLException.class);
                utilsStatic.when(() -> TDataSourceInitUtils.initDataSource(ds)).thenReturn(null);
                SyncHandler.initDataSourceForSyncIfNeed(sc, "mymockdb");
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}
