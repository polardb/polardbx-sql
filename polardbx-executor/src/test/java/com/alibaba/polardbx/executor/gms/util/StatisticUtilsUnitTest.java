package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_HLL;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_START;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.SELECT_TABLE_ROWS_SQL;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getIndexInfoFromGsi;
import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class StatisticUtilsUnitTest {
    @Test
    public void testSketchTableDdlNormal() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        // 测试用例1：当参数正确时，不抛出异常
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
        ) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("test_schema"))
                .thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable("test_table")).thenReturn(mock(TableMeta.class));
            globalIndexMetaMockedStatic.when(() -> GlobalIndexMeta.getTableIndexMap(any(), any()))
                .thenReturn(Maps.newHashMap());
            StatisticSubProcessUtils.sketchTableDdl("test_schema", "test_table", false, new ExecutionContext());
        }
    }

    @Test
    public void testSketchTableDdlFileStore() throws Exception {
        // 测试用例2：如果是文件存储表，则直接返回而不执行后续操作
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
            MockedStatic<StatisticSubProcessUtils> statisticSubProcessUtilsMockedStatic = mockStatic(
                StatisticSubProcessUtils.class);
        ) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("test_schema"))
                .thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable("test_table")).thenReturn(mock(TableMeta.class));

            globalIndexMetaMockedStatic.when(() -> GlobalIndexMeta.getTableIndexMap(any(), any()))
                .thenReturn(Maps.newHashMap());

            statisticUtilsMockedStatic.when(() -> StatisticUtils.isFileStore(any(), any()))
                .thenReturn(true);
            statisticSubProcessUtilsMockedStatic.when(
                    () -> StatisticSubProcessUtils.sketchTableDdl(any(), any(), Mockito.anyBoolean(), any()))
                .thenCallRealMethod();

            StatisticSubProcessUtils.sketchTableDdl("test_schema", "test_table", false, new ExecutionContext());
        }
    }

    @Test
    public void testSketchTableDdl_JobInterrupted() {
        // 测试用例3：如果DDL作业被取消，则抛出TddlRuntimeException
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
            MockedStatic<StatisticSubProcessUtils> statisticSubProcessUtilsMockedStatic = mockStatic(
                StatisticSubProcessUtils.class);
            MockedStatic<CrossEngineValidator> crossEngineValidatorMockedStatic = mockStatic(
                CrossEngineValidator.class);
        ) {
            Map<String, java.util.Map<String, List<String>>> indexMap = Maps.newHashMap();
            indexMap.put("test_schema", Maps.newHashMap());

            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("test_schema"))
                .thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);

            TableMeta tableMeta = mock(TableMeta.class);
            when(schemaManager.getTable("test_table")).thenReturn(tableMeta);
            IndexMeta uniqueIndex = createIndex(false);
            List<IndexMeta> indexMetas = Lists.newArrayList();
            indexMetas.add(uniqueIndex);
            when(tableMeta.getAllIndexes()).thenReturn(indexMetas);

            globalIndexMetaMockedStatic.when(() -> GlobalIndexMeta.getTableIndexMap(any(), any()))
                .thenReturn(indexMap);
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getIndexInfoFromGsi(any(), any(), any()))
                .thenCallRealMethod();
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getIndexInfoFromLocalIndex(any(), any(), any()))
                .thenCallRealMethod();
            statisticUtilsMockedStatic.when(() -> StatisticUtils.isFileStore(any(), any()))
                .thenReturn(false);
            statisticSubProcessUtilsMockedStatic.when(
                    () -> StatisticSubProcessUtils.sketchTableDdl(any(), any(), Mockito.anyBoolean(), any()))
                .thenCallRealMethod();
            crossEngineValidatorMockedStatic.when(() -> CrossEngineValidator.isJobInterrupted(any()))
                .thenReturn(true);

            StatisticSubProcessUtils.sketchTableDdl("test_schema", "test_table", false, new ExecutionContext());
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("4636"));
        }
    }

    @Test
    public void testSketchTableDdlRebuild() throws Exception {
        // 测试用例5：当需要强制重建时，调用rebuildShardParts方法
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
            MockedStatic<StatisticSubProcessUtils> statisticSubProcessUtilsMockedStatic = mockStatic(
                StatisticSubProcessUtils.class);
            MockedStatic<CrossEngineValidator> crossEngineValidatorMockedStatic = mockStatic(
                CrossEngineValidator.class);
            MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);

        ) {
            Map<String, java.util.Map<String, List<String>>> indexMap = Maps.newHashMap();
            indexMap.put("test_schema", Maps.newHashMap());
            indexMap.get("test_schema").put("test_table", ImmutableList.of("test_col"));

            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("test_schema"))
                .thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            TableMeta tableMeta = mock(TableMeta.class);
            when(schemaManager.getTable("test_table")).thenReturn(tableMeta);
            IndexMeta uniqueIndex = createIndex(false);
            List<IndexMeta> indexMetas = Lists.newArrayList();
            indexMetas.add(uniqueIndex);
            when(tableMeta.getAllIndexes()).thenReturn(indexMetas);

            statisticUtilsMockedStatic.when(() -> StatisticUtils.isFileStore(any(), any()))
                .thenReturn(false);
            statisticSubProcessUtilsMockedStatic.when(
                    () -> StatisticSubProcessUtils.sketchTableDdl(any(), any(), Mockito.anyBoolean(), any()))
                .thenCallRealMethod();
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getIndexInfoFromGsi(any(), any(), any()))
                .thenCallRealMethod();
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getIndexInfoFromLocalIndex(any(), any(), any()))
                .thenCallRealMethod();
            crossEngineValidatorMockedStatic.when(() -> CrossEngineValidator.isJobInterrupted(any()))
                .thenReturn(false);

            StatisticManager statisticManager = mock(StatisticManager.class);
            statisticManagerMockedStatic.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);

            ExecutionContext executionContext = new ExecutionContext();

            StatisticSubProcessUtils.sketchTableDdl("test_schema", "test_table", true, executionContext);

            verify(statisticManager, times(2)).rebuildShardParts(any(), any(), any(), any(), any());
            verify(statisticManager, times(0)).updateAllShardParts(any(), any(), any(), any(), any());

            clearInvocations(statisticManager);

            StatisticSubProcessUtils.sketchTableDdl("test_schema", "test_table", false, executionContext);

            verify(statisticManager, times(0)).rebuildShardParts(any(), any(), any(), any(), any());
            verify(statisticManager, times(2)).updateAllShardParts(any(), any(), any(), any(), any());
        }
    }

    @Test
    public void testConstructScanSamplingSql() {
        // 准备测试数据
        String logicalTableName = "testTable";
        float sampleRate = 0.5f;
        List<ColumnMeta> columnMetaList = new ArrayList<>();
        columnMetaList.add(new ColumnMeta("testTable", "col1", "c1", null));
        columnMetaList.add(new ColumnMeta("testTable", "col2", "c2", null));

        // 调用待测试的函数
        String sql = StatisticUtils.constructScanSamplingSql(logicalTableName, columnMetaList, sampleRate);

        // 构建期望的结果
        String expectedSqlStart =
            "/*+TDDL:cmd_extra(enable_post_planner=false,enable_index_selection=false,merge_union=false,enable_direct_plan=false,sample_percentage=50.0) */ "
                + "select `col1`,`col2` from `testTable`";

        // 断言结果是否正确
        assertTrue(sql.startsWith(expectedSqlStart));

    }

    @Test
    public void testBuildCollectRowCountSql() {
        String[] tbls = {
            "select_base_four_multi_db_multi_tb_Nu9i_00", "select_base_four_multi_db_multi_tb_Nu9i_01",
            "select_base_four_multi_db_multi_tb_Nu9i_02", "select_base_four_multi_db_multi_tb_Nu9i_03",
            "select_base_four_multi_db_multi_tb_Nu9i_06"};
        String sql = StatisticUtils.buildCollectRowCountSql(Lists.newArrayList(tbls));
        System.out.println(sql);
        assertTrue(
            ("SELECT table_schema, table_name, table_rows FROM information_schema.tables "
                + "WHERE TABLE_NAME IN ("
                + "'select_base_four_multi_db_multi_tb_Nu9i_00',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_01',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_02',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_03',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_06')")
                .equals(sql));

        sql = StatisticUtils.buildCollectRowCountSql(null);
        assertTrue(SELECT_TABLE_ROWS_SQL.equals(sql));
        sql = StatisticUtils.buildCollectRowCountSql(Collections.emptyList());
        assertTrue(SELECT_TABLE_ROWS_SQL.equals(sql));
    }

    /**
     * test com.alibaba.polardbx.executor.utils.ExecUtils#needSketchInterrupted() and its branch
     */
    @Test
    public void testNeedSketchInterrupted() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        // Test sketch interrupted by ENABLE_BACKGROUND_STATISTIC_COLLECTION=false
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap()
            .put("ENABLE_BACKGROUND_STATISTIC_COLLECTION", "false");

        assertTrue(InstConfUtil.getBool(ENABLE_BACKGROUND_STATISTIC_COLLECTION) == false);

        Pair<Boolean, String> pair = ExecUtils.needSketchInterrupted();

        assertTrue(pair.getKey());

        assertTrue(pair.getValue().equals("ENABLE_BACKGROUND_STATISTIC_COLLECTION not enabled"));

        // revert ENABLE_BACKGROUND_STATISTIC_COLLECTION = true
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap()
            .put("ENABLE_BACKGROUND_STATISTIC_COLLECTION", "true");
        assertTrue(InstConfUtil.getBool(ENABLE_BACKGROUND_STATISTIC_COLLECTION));

        // Test  sketch interrupted by ENABLE_HLL=false
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ENABLE_HLL", "false");
        assertTrue(InstConfUtil.getBool(ENABLE_HLL) == false);

        pair = ExecUtils.needSketchInterrupted();

        assertTrue(pair.getKey());

        assertTrue(pair.getValue().equals("ENABLE_HLL not enabled"));

        // revert ENABLE_HLL = true
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ENABLE_HLL", "true");
        assertTrue(InstConfUtil.getBool(ENABLE_HLL));
    }

    /**
     * test com.alibaba.polardbx.gms.config.impl.InstConfUtil#isInMaintenanceTimeWindow(java.util.Calendar)
     */
    @Test
    public void testIsInMaintenanceTimeWindow() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Calendar calendar = Calendar.getInstance();

        calendar.set(Calendar.HOUR_OF_DAY, 1);
        assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 2);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 3);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        calendar.set(Calendar.MINUTE, 40);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 5);
        calendar.set(Calendar.MINUTE, 1);
        assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        // mock error config for MAINTENANCE_TIME_START / MAINTENANCE_TIME_END
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_START", "xx");
        assertTrue(InstConfUtil.getOriginVal(MAINTENANCE_TIME_START).equals("xx"));

        assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().remove("MAINTENANCE_TIME_START");
        assertTrue(InstConfUtil.getOriginVal(MAINTENANCE_TIME_START).equals("02:00"));

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_START", "23:00");
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_END", "03:00");
        calendar.set(Calendar.HOUR_OF_DAY, 1);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));
    }

    /**
     * 空的gsiPublished集合
     */
    @Test
    public void testGetIndexInfoFromGsiWithEmptyGsiPublished() {
        TableMeta mockTableMeta = mock(TableMeta.class);

        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();
        getIndexInfoFromGsi(mockTableMeta, colDoneSet, indexColsMap);
        assertTrue(indexColsMap.isEmpty(), "当没有GSI时，索引列映射应该是空的");
    }

    /**
     * 包含非唯一索引的gsiPublished集合
     */
    @Test
    public void testGetIndexInfoFromGsiWithNonUniqueGsi() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        // 假设有一个非唯一的GSI
        List<GsiMetaManager.GsiIndexColumnMetaBean> columns = Arrays.asList(
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnA", "", -1L, null, null, null, false),
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnB", "", -1L, null, null, null, false)
        );
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = new GsiMetaManager.GsiIndexMetaBean(null, null, null, true,
            null, null, columns, null, null, null, null, null, null, null, -1L, false, false, null, null);
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();
        gsiPublished.put("index1", gsiIndexMetaBean);
        when(mockTableMeta.getGsiPublished()).thenReturn(gsiPublished);

        getIndexInfoFromGsi(mockTableMeta, colDoneSet, indexColsMap);

        assertEquals("应该有两个索引列被加入到集合中", 2, indexColsMap.get("index1").size());
    }

    /**
     * 包含唯一索引的gsiPublished集合
     */
    @Test
    public void testGetIndexInfoFromGsiWithUniqueGsi() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        // 假设有一个非唯一的GSI
        List<GsiMetaManager.GsiIndexColumnMetaBean> columns = Arrays.asList(
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnA", "", -1L, null, null, null, false),
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnB", "", -1L, null, null, null, false),
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnC", "", -1L, null, null, null, false)
        );
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = new GsiMetaManager.GsiIndexMetaBean(null, null, null, false,
            null, null, columns, null, null, null, null, null, null, null, -1L, false, false, null, null);
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();
        gsiPublished.put("index1", gsiIndexMetaBean);
        when(mockTableMeta.getGsiPublished()).thenReturn(gsiPublished);

        getIndexInfoFromGsi(mockTableMeta, colDoneSet, indexColsMap);

        assertEquals("应该有一个索引被加入到集合中", 1, indexColsMap.size());
        assertEquals("应该有两个索引列被加入到集合中", 2, indexColsMap.values().iterator().next().size());
    }

    /**
     * 已经存在的索引名称
     */
    @Test
    public void testGetIndexInfoFromGsiWithExistingIndexName() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<GsiMetaManager.GsiIndexColumnMetaBean> columns = Arrays.asList(
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnA", "", -1L, null, null, null, true),
            new GsiMetaManager.GsiIndexColumnMetaBean(-1, "columnB", "", -1L, null, null, null, true)
        );
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = new GsiMetaManager.GsiIndexMetaBean(null, null, null, true,
            null, null, columns, null, null, null, null, null, null, null, -1L, false, false, null, null);
        // 假设有两个相同的非唯一GSI
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();
        gsiPublished.put("index1", gsiIndexMetaBean);
        gsiPublished.put("index1", gsiIndexMetaBean); // 同样的名字
        when(mockTableMeta.getGsiPublished()).thenReturn(gsiPublished);

        getIndexInfoFromGsi(mockTableMeta, colDoneSet, indexColsMap);
        assertEquals("即使存在多个同名索引，也应该只记录一次", 2, indexColsMap.get("index1").size());
        assertEquals("即使存在多个同名索引，也应该只记录一次", 1, indexColsMap.size());
    }

    /**
     * 空的tableMeta, 当传入空的tableMeta时，应该没有任何改变。
     */
    @Test
    public void testGetIndexInfoFromLocalIndexWithEmptyTableMeta() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        when(mockTableMeta.getAllIndexes()).thenReturn(Collections.emptyList());

        StatisticUtils.getIndexInfoFromLocalIndex(mockTableMeta, colDoneSet, indexColsMap);

        assertEquals(0, indexColsMap.size());
        assertEquals(0, colDoneSet.size());
    }

    /**
     * 包含唯一索引的tableMeta
     * 唯一索引不应该被处理。
     */
    @Test
    public void testGetIndexInfoFromLocalIndexWithUniqueIndex() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        IndexMeta uniqueIndex = createIndex(true); // 创建唯一的索引
        List<IndexMeta> indexMetas = Lists.newArrayList();
        indexMetas.add(uniqueIndex);
        when(mockTableMeta.getAllIndexes()).thenReturn(indexMetas);

        StatisticUtils.getIndexInfoFromLocalIndex(mockTableMeta, colDoneSet, indexColsMap);

        assertEquals(1, indexColsMap.size());
        assertEquals(1, colDoneSet.size());
    }

    /**
     * 正常情况下的非唯一索引
     * 应当正确填充indexColsMap并更新colDoneSet。
     */
    @Test
    public void testGetIndexInfoFromLocalIndexWithNonUniqueIndex() {
        TableMeta mockTableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = new HashSet<>();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        IndexMeta nonUniqueIndex = createIndex(false); // 创建非唯一的索引
        List<IndexMeta> indexMetas = Lists.newArrayList();
        indexMetas.add(nonUniqueIndex);
        when(mockTableMeta.getAllIndexes()).thenReturn(indexMetas);

        StatisticUtils.getIndexInfoFromLocalIndex(mockTableMeta, colDoneSet, indexColsMap);

        assertEquals(1, indexColsMap.size());
        assertTrue(indexColsMap.containsKey("idx1"));
        assertEquals(2, colDoneSet.size());
        assertTrue(colDoneSet.contains("c1"));
        assertTrue(colDoneSet.contains("c1,c2"));
    }

    @Test
    public void testBuildTopnAndHistogram() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        String schema = "test_schema";
        String table = "test_table";
        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);

        List<ColumnMeta> analyzeColumnList = Lists.newArrayList();
        analyzeColumnList.add(
            new ColumnMeta("schema", "table", "column", new Field("1", "id",
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));

        try (MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);) {
            statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(statisticManager);
            statisticUtilsMockedStatic.when(() -> StatisticUtils.canUseNewTopN(anyString(), anyString(), anyString()))
                .thenReturn(true);
            statisticUtilsMockedStatic.when(
                () -> StatisticUtils.buildTopnAndHistogram(anyString(), anyString(), anyList(), anyList(),
                    any(), anyFloat(), anyDouble(), anyInt(), anyBoolean())).thenCallRealMethod();

            StatisticUtils.buildTopnAndHistogram(schema, table, analyzeColumnList, Lists.newArrayList(), null, 1.0f,
                1.0, 10, false);
        }

    }

    private IndexMeta createIndex(boolean isUnique) {
        List<ColumnMeta> columns = Arrays.asList(
            new ColumnMeta(null, "c1", null, mock(Field.class)),
            new ColumnMeta(null, "c2", null, mock(Field.class))
        );
        return new IndexMeta("t1", columns, columns, null, null, false, false, isUnique, "idx1");
    }

}
