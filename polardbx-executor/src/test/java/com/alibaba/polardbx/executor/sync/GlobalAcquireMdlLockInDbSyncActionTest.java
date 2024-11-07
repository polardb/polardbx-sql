package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlobalAcquireMdlLockInDbSyncActionTest {

    @InjectMocks
    private GlobalAcquireMdlLockInDbSyncAction action =
        new GlobalAcquireMdlLockInDbSyncAction(ImmutableSet.of("testSchema"));

    @Mock
    private SchemaManager schemaManager;

    @Before
    public void setUp() {
    }

    @Test
    public void testGetAllTablesInDatabase() {
        String schemaName = "testSchema";
        List<Map<String, Object>> mockResult = ImmutableList.of(
            ImmutableMap.of("TABLE_TYPE", "base table", "TABLES_IN_TESTSCHEMA", "tbl1"),
            ImmutableMap.of("TABLE_TYPE", "base table", "TABLES_IN_TESTSCHEMA1", "tbl2"),
            ImmutableMap.of("TABLE_TYPE", "view", "TABLES_IN_TESTSCHEMA", "view1")
        );
        IServerConfigManager iServerConfigManager = mock(IServerConfigManager.class);
        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);) {
            mockedDdlHelper.when(() -> DdlHelper.getServerConfigManager()).thenReturn(iServerConfigManager);
            when(iServerConfigManager.executeQuerySql(anyString(), anyString(), any())).thenReturn(mockResult);

            Set<String> expectedTables = ImmutableSet.of("tbl1");
            Set<String> actualTables = action.getAllTablesInDatabase(schemaName);

            assertEquals(expectedTables, actualTables);
        }
    }

}
