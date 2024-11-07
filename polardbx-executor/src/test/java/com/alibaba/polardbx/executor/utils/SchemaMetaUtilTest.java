package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import org.apache.calcite.avatica.Meta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class SchemaMetaUtilTest {
    @Mock
    private Connection mockMetaDatabaseConnection;

    @Mock
    private BaselineInfoAccessor mockBaselineInfoAccessor;

    @Before
    public void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        MockitoAnnotations.openMocks(this);
    }

    /*
     * Design rationale:
     *   Simulate successful database connection and 'BaselineInfoAccessor' operation to ensure that when no exceptions are thrown,
     *   the method calls the 'deleteBySchema' method correctly.
     */
    @Test
    public void testDeleteBaselineInformationSuccess() {
        // Preparation
        String schemaName = "test_schema";
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
            when(mockMetaDbDataSource.getConnection()).thenReturn(mockMetaDatabaseConnection);

            doNothing().when(mockBaselineInfoAccessor).deleteBySchema(schemaName);

            // Execution
            SchemaMetaUtil.deleteBaselineInformation(schemaName, mockBaselineInfoAccessor);
        }

        // Verification
        verify(mockBaselineInfoAccessor, times(1)).setConnection(mockMetaDatabaseConnection);
        verify(mockBaselineInfoAccessor, times(1)).deleteBySchema(schemaName);
        verify(mockBaselineInfoAccessor, times(1)).setConnection(null);
    }

    /*
     * Design rationale:
     *   When a database connection or 'BaselineInfoAccessor' operation throws an exception, ensure that the log recording is triggered correctly
     *   and resources are cleaned up.
     */
    @Test
    public void testDeleteBaselineInformationFailure() {
        // Preparation
        String schemaName = "error_schema";
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);

        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
            when(mockMetaDbDataSource.getConnection()).thenThrow(new RuntimeException("Simulated DB error"));
            doNothing().when(mockBaselineInfoAccessor).deleteBySchema(schemaName);

            // Execution & Verification
            SchemaMetaUtil.deleteBaselineInformation(schemaName, mockBaselineInfoAccessor);
        }

        // Verify log recording and resource cleanup
        verify(mockBaselineInfoAccessor, never()).deleteBySchema(
            schemaName); // Ensure it does not attempt to delete if connection fails
        verify(mockBaselineInfoAccessor, times(1)).setConnection(null);
    }
}
