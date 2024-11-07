package com.alibaba.polardbx.executor.statistics.ndv;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.utils.Assert.assertNotNull;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.common.utils.Assert.fail;
import static com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch.handleException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class NDVShardSketchTest {
    @Test
    public void testBuildShardPartsValidInput() {
        String schemaName = "test_schema";
        String tableName = "test_table";
        String columnNames = "col1,col2";
        OptimizerContext opMock = mock(OptimizerContext.class);
        when(opMock.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));

        TableMeta tableMeta = mock(TableMeta.class);
        IndexMeta indexMeta = mock(IndexMeta.class);
        ColumnMeta col1 = mock(ColumnMeta.class);
        ColumnMeta col2 = mock(ColumnMeta.class);
        when(col1.getName()).thenReturn("col1");
        when(col2.getName()).thenReturn("col2");
        when(tableMeta.getIndexes()).thenReturn(Collections.singletonList(indexMeta));
        when(indexMeta.getPhysicalIndexName()).thenReturn("physical_index");
        when(indexMeta.getKeyColumns()).thenReturn(ImmutableList.of(col1, col2));
        when(opMock.getLatestSchemaManager().getTable(tableName)).thenReturn(tableMeta);

        Map<String, Set<String>> topology = new HashMap<>();
        topology.put("group1", Collections.singleton("testTable1"));
        topology.put("group2", ImmutableSet.of("testTable1", "testTable2"));

        try (MockedStatic<OptimizerContext> optimizerContext = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<NDVShardSketch> ndvShardSketch = Mockito.mockStatic(NDVShardSketch.class)) {
            ndvShardSketch.when(() -> NDVShardSketch.getTopology(schemaName, tableName, opMock)).thenReturn(topology);
            ndvShardSketch.when(() -> NDVShardSketch.buildShardParts(schemaName, tableName, columnNames))
                .thenCallRealMethod();
            ndvShardSketch.when(() -> NDVShardSketch.topologyPartToShard(any())).thenCallRealMethod();
            optimizerContext.when(() -> OptimizerContext.getContext(schemaName)).thenReturn(opMock);

            Pair<String, String[]> result = NDVShardSketch.buildShardParts(schemaName, tableName, columnNames);

            assertNotNull(result);
            assertEquals("physical_index", result.getKey());
            assertTrue(Arrays.asList(result.getValue())
                .containsAll(ImmutableList.of("group1:testtable1", "group2:testtable1", "group2:testtable2")));
        }
    }

    @Test
    public void testBuildShardPartsGsiValidInput() {
        String schemaName = "test_schema";
        String tableName = "test_table";
        String indexTableName = "index_test_table";
        String columnNames = "col1,col2";
        OptimizerContext opMock = mock(OptimizerContext.class);
        when(opMock.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));

        final Map<String, GsiMetaManager.GsiIndexMetaBean> gsiMeta = getStringGsiIndexMetaBeanMap();
        TableMeta tableMeta = mock(TableMeta.class);
        TableMeta tableMetaGsi = mock(TableMeta.class);
        IndexMeta indexMeta = mock(IndexMeta.class);
        ColumnMeta col1 = mock(ColumnMeta.class);
        ColumnMeta col2 = mock(ColumnMeta.class);
        when(col1.getName()).thenReturn("col1");
        when(col2.getName()).thenReturn("col2");
        when(tableMeta.getIndexes()).thenReturn(Collections.emptyList());
        when(tableMeta.getGsiPublished()).thenReturn(gsiMeta);
        when(tableMetaGsi.getIndexes()).thenReturn(Collections.singletonList(indexMeta));
        when(indexMeta.getPhysicalIndexName()).thenReturn("physical_index");
        when(indexMeta.getKeyColumns()).thenReturn(ImmutableList.of(col1, col2));
        when(opMock.getLatestSchemaManager().getTable(tableName)).thenReturn(tableMeta);
        when(opMock.getLatestSchemaManager().getTable(indexTableName)).thenReturn(tableMetaGsi);

        Map<String, Set<String>> topology = new HashMap<>();
        topology.put("group1", Collections.singleton("testTable1"));
        topology.put("group2", ImmutableSet.of("testTable1", "testTable2"));

        try (MockedStatic<OptimizerContext> optimizerContext = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<NDVShardSketch> ndvShardSketch = Mockito.mockStatic(NDVShardSketch.class)) {
            ndvShardSketch.when(() -> NDVShardSketch.getTopology(schemaName, indexTableName, opMock))
                .thenReturn(topology);
            ndvShardSketch.when(() -> NDVShardSketch.buildShardParts(schemaName, tableName, columnNames))
                .thenCallRealMethod();
            ndvShardSketch.when(() -> NDVShardSketch.topologyPartToShard(any())).thenCallRealMethod();
            optimizerContext.when(() -> OptimizerContext.getContext(schemaName)).thenReturn(opMock);

            Pair<String, String[]> result = NDVShardSketch.buildShardParts(schemaName, tableName, columnNames);

            assertNotNull(result);
            assertEquals("physical_index", result.getKey());
            assertTrue(Arrays.asList(result.getValue())
                .containsAll(ImmutableList.of("group1:testtable1", "group2:testtable1", "group2:testtable2")));
        }
    }

    @Test
    public void testHandleExceptionOtherSqlError() {
        // test error code that is not 1146
        String shardKey = "shardKey";
        String shardPart = "shardPart";
        SQLException e = mock(SQLException.class);
        when(e.getErrorCode()).thenReturn(1000); // 假设这是另一个错误码
        when(e.getSQLState()).thenReturn("42S02");
        try (MockedStatic<StatisticManager> mockedStatic = mockStatic(StatisticManager.class);
            MockedStatic<ModuleLogInfo> mockedLog = mockStatic(ModuleLogInfo.class);) {
            StatisticManager mockManager = mock(StatisticManager.class);
            ModuleLogInfo mockModuleLog = mock(ModuleLogInfo.class);
            StatisticDataSource mockSource = mock(StatisticDataSource.class);
            mockedStatic.when(StatisticManager::getInstance).thenReturn(mockManager);
            mockedLog.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLog);
            when(mockManager.getSds()).thenReturn(mockSource);
            doNothing().when(mockSource).removeLogicalTableList(any(), any());
            doNothing().when(mockModuleLog).logRecord(any(), any(), any(), any(), any(Exception.class));

            handleException(shardKey, shardPart, e, "schemaName", new String[] {"shardKeys1"});
            fail("Should throw exception");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testHandleExceptionDifferentSqlState() {
        // test sql state that is not 42S02
        String shardKey = "shardKey";
        String shardPart = "shardPart";
        SQLException e = mock(SQLException.class);
        when(e.getErrorCode()).thenReturn(1146);
        when(e.getSQLState()).thenReturn("50000");
        try (MockedStatic<StatisticManager> mockedStatic = mockStatic(StatisticManager.class);
            MockedStatic<ModuleLogInfo> mockedLog = mockStatic(ModuleLogInfo.class);) {
            StatisticManager mockManager = mock(StatisticManager.class);
            ModuleLogInfo mockModuleLog = mock(ModuleLogInfo.class);
            StatisticDataSource mockSource = mock(StatisticDataSource.class);
            mockedStatic.when(StatisticManager::getInstance).thenReturn(mockManager);
            mockedLog.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLog);
            when(mockManager.getSds()).thenReturn(mockSource);
            doNothing().when(mockSource).removeLogicalTableList(any(), any());
            doNothing().when(mockModuleLog).logRecord(any(), any(), any(), any(), any(Exception.class));

            handleException(shardKey, shardPart, e, "schemaName", new String[] {"shardKeys1"});
            fail("Should throw exception");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testHandleExceptionNonSqlException() throws Exception {
        // test exception that is not SQLException
        String shardKey = "shardKey";
        String shardPart = "shardPart";
        Exception e = new RuntimeException("Non-SQL exception");
        try (MockedStatic<StatisticManager> mockedStatic = mockStatic(StatisticManager.class);
            MockedStatic<ModuleLogInfo> mockedLog = mockStatic(ModuleLogInfo.class);) {
            StatisticManager mockManager = mock(StatisticManager.class);
            ModuleLogInfo mockModuleLog = mock(ModuleLogInfo.class);
            StatisticDataSource mockSource = mock(StatisticDataSource.class);
            mockedStatic.when(StatisticManager::getInstance).thenReturn(mockManager);
            mockedLog.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLog);
            when(mockManager.getSds()).thenReturn(mockSource);
            doNothing().when(mockSource).removeLogicalTableList(any(), any());
            doNothing().when(mockModuleLog).logRecord(any(), any(), any(), any(), any(Exception.class));

            handleException(shardKey, shardPart, e, "schemaName", new String[] {"shardKeys1"});
            fail("Should throw exception");
        } catch (Exception ex) {
            ex.printStackTrace();
            assertEquals(e, ex);
        }
    }

    /**
     * 测试用例1: 当schema存在时返回true
     * 设计思路:
     * - 假设shardKey包含有效的schema名称、表名和列名。
     * - mock buildShardParts使其返回预期的结果。
     * - 确保当传入的shardParts与构建出的一致时，返回true。
     */
    @Test
    public void testIsValidShardPartSchemaExists() {
        // 准备
        String shardKey = "test_schema:test_table:test_column";
        String[] shardParts = {"part1", "part2"};
        String[] shardParts1 = {"part2", "part1"};
        String[] shardParts2 = {"part2", "part1", "part3"};

        // 模拟
        Pair<String, String[]> mockedResult = mock(Pair.class);
        when(mockedResult.getValue()).thenReturn(shardParts);

        try (MockedStatic<NDVShardSketch> ndvShardSketch = Mockito.mockStatic(NDVShardSketch.class)) {
            ndvShardSketch.when(() -> NDVShardSketch.buildShardParts(any(), any(), any()))
                .thenReturn(mockedResult);
            ndvShardSketch.when(() -> NDVShardSketch.isValidShardPart(anyString(), any())).thenCallRealMethod();
            // 执行
            boolean result = NDVShardSketch.isValidShardPart(shardKey, shardParts).booleanValue();

            // 验证
            assertTrue(result);

            when(mockedResult.getValue()).thenReturn(shardParts1);

            // 执行
            result = NDVShardSketch.isValidShardPart(shardKey, shardParts).booleanValue();

            // 验证
            assertTrue(result);

            when(mockedResult.getValue()).thenReturn(shardParts2);

            // 执行
            result = NDVShardSketch.isValidShardPart(shardKey, shardParts).booleanValue();

            // 验证
            assertFalse(result);
        }
    }

    /**
     * 测试用例2: 当schema不存在时返回null
     * 设计思路:
     * - 设置shardKey为有效值但让buildShardParts返回null。
     * - 预期isValidShardPart应该返回null。
     */
    @Test
    public void testIsValidShardPartSchemaNotExists() {
        // 准备
        String shardKey = "nonexistent_schema:test_table:test_column";
        String[] shardParts = {"part1", "part2"};

        // 执行
        Boolean result = NDVShardSketch.isValidShardPart(shardKey, shardParts);

        // 验证
        assertNull(result);
    }

    /**
     * 测试用例3: 当shardKey为空时抛出异常
     * 设计思路:
     * - 输入一个空字符串作为shardKey。
     * - 预期程序会因为split操作失败而抛出异常。
     */
    @Test
    public void testIsValidShardPartWithEmptyShardKey() {
        // 准备
        String shardKey = "";
        String[] shardParts = {"part1", "part2"};

        // 执行并验证
        Assert.assertTrue(NDVShardSketch.isValidShardPart(shardKey, shardParts) == null);
    }

    /**
     * 测试用例4: 当shardKey缺少部分信息时返回false
     * 设计思路:
     * - 提供一个缺少一部分信息的shardKey。
     * - 即使buildShardParts正确返回，但由于shardParts不匹配，所以返回false。
     */
    @Test
    public void testIsValidShardPartWithIncompleteShardKey() {
        // 准备
        String shardKey = "test_schema:test_table:column";
        String[] shardParts = {"part1", "part2"};

        // 模拟
        Pair<String, String[]> mockedResult = mock(Pair.class);
        when(mockedResult.getValue()).thenReturn(new String[] {"part1"});
        try (MockedStatic<NDVShardSketch> ndvShardSketch = Mockito.mockStatic(NDVShardSketch.class)) {
            ndvShardSketch.when(() -> NDVShardSketch.buildShardParts(any(), any(), any()))
                .thenReturn(mockedResult);
            ndvShardSketch.when(() -> NDVShardSketch.isValidShardPart(anyString(), any())).thenCallRealMethod();
            // 执行
            boolean result = NDVShardSketch.isValidShardPart(shardKey, shardParts).booleanValue();

            // 验证
            assertFalse(result);
        }
    }

    @NotNull
    private Map<String, GsiMetaManager.GsiIndexMetaBean> getStringGsiIndexMetaBeanMap() {
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiMeta = new HashMap<>();
        GsiMetaManager.GsiIndexMetaBean gsiMeta1 = new GsiMetaManager.GsiIndexMetaBean(
            "testIndex",
            "testSchema",
            "index_test_table",
            false,
            "index_schema",
            "non_Matching_Index",
            Lists.newArrayList(),
            Lists.newArrayList(),
            "indexType",
            "",
            "",
            null,
            "index_test_table",
            IndexStatus.PUBLIC,
            0,
            false,
            false,
            IndexVisibility.VISIBLE);

        final GsiMetaManager.GsiIndexMetaBean gsiMeta2 = getGsiIndexMetaBean();
        gsiMeta.put("1", gsiMeta1);
        gsiMeta.put("2", gsiMeta2);
        return gsiMeta;
    }

    @NotNull
    private GsiMetaManager.GsiIndexMetaBean getGsiIndexMetaBean() {
        GsiMetaManager.GsiIndexColumnMetaBean gsiIndexColumnMetaBean1 =
            new GsiMetaManager.GsiIndexColumnMetaBean(1, "col1", "", 0, 0L, "", "", false);
        GsiMetaManager.GsiIndexColumnMetaBean gsiIndexColumnMetaBean2 =
            new GsiMetaManager.GsiIndexColumnMetaBean(1, "col2", "", 0, 0L, "", "", false);
        GsiMetaManager.GsiIndexMetaBean gsiMeta2 = new GsiMetaManager.GsiIndexMetaBean(
            "testIndex",
            "testSchema",
            "index_test_table",
            false,
            "index_schema",
            "non_Matching_Index",
            ImmutableList.of(gsiIndexColumnMetaBean1, gsiIndexColumnMetaBean2),
            Lists.newArrayList(),
            "indexType",
            "",
            "",
            null,
            "index_test_table",
            IndexStatus.PUBLIC,
            0,
            false,
            false,
            IndexVisibility.VISIBLE);
        return gsiMeta2;
    }
}
