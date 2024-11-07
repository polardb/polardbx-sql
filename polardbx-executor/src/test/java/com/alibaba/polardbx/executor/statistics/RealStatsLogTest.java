package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.executor.statistic.RealStatsLogType;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class RealStatsLogTest {

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testPlanCacheSize() {
        PlanCache mockPlanCache = mock(PlanCache.class);
        when(mockPlanCache.getCacheKeyCount()).thenReturn(123L);
        try (MockedStatic<PlanCache> mock = mockStatic(PlanCache.class)) {
            mock.when(PlanCache::getInstance).thenReturn(mockPlanCache);
            String result = RealStatsLogType.PLAN_CACHE_SIZE.getLog();
            assertEquals("123", result);
        }
    }

    @Test
    public void testTableCount() {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);

        // mock db list
        String[] dbList = {"db1", "db2"};
        when(mockDbInfoManager.getDbList()).thenReturn(Arrays.asList(dbList));

        try (MockedStatic<SystemDbHelper> mock = mockStatic(SystemDbHelper.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
        ) {
            mock.when(() -> SystemDbHelper.isDBBuildIn("db1")).thenReturn(false);
            mock.when(() -> SystemDbHelper.isDBBuildIn("db2")).thenReturn(false);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            Set<String> set = new HashSet<>();
            set.add("db1");
            optimizerContextMockedStatic.when(OptimizerContext::getActiveSchemaNames).thenReturn(set);

            List<TableMeta> tableList = Lists.newArrayList();
            tableList.add(mock(TableMeta.class));
            tableList.add(mock(TableMeta.class));
            tableList.add(mock(TableMeta.class));
            SchemaManager mockSchemaManager = mock(SchemaManager.class);
            when(mockSchemaManager.getAllUserTables()).thenReturn(tableList);
            when(mockOptimizerContext.getContext("db1")).thenReturn(mockOptimizerContext);
            when(mockOptimizerContext.getLatestSchemaManager()).thenReturn(mockSchemaManager);

            String result = RealStatsLogType.TABLE_NUM.getLog();

            // 验证返回的表数量
            assertEquals("3", result);
        }
    }

    @Test
    public void testTableInStatsCount() {
        StatisticManager mockStatisticManager = mock(StatisticManager.class);
        Map<String, Map<String, StatisticManager.CacheLine>> mockCache = new HashMap<>();
        Map<String, StatisticManager.CacheLine> tbMap1 = Maps.newHashMap();
        tbMap1.put("tb1", mock(StatisticManager.CacheLine.class));
        tbMap1.put("tb2", mock(StatisticManager.CacheLine.class));
        tbMap1.put("tb3", mock(StatisticManager.CacheLine.class));

        Map<String, StatisticManager.CacheLine> tbMap2 = Maps.newHashMap();
        tbMap2.put("tb4", mock(StatisticManager.CacheLine.class));
        tbMap2.put("tb5", mock(StatisticManager.CacheLine.class));
        tbMap2.put("tb6", mock(StatisticManager.CacheLine.class));
        tbMap2.put("tb7", mock(StatisticManager.CacheLine.class));

        mockCache.put("schema1", tbMap1);
        mockCache.put("schema2", tbMap2);
        when(mockStatisticManager.getStatisticCache()).thenReturn(mockCache);

        try (MockedStatic<StatisticManager> managerMockedStatic = mockStatic(StatisticManager.class)) {
            managerMockedStatic.when(StatisticManager::getInstance).thenReturn(mockStatisticManager);
            String result = RealStatsLogType.STATISTIC_TABLE_NUM.getLog();

            assertEquals("7", result);
        }
    }

    @Test
    public void testProcedureCount() {
        ProcedureManager mockManager = mock(ProcedureManager.class);
        when(mockManager.getProcedureSize()).thenReturn(12L);
        try (MockedStatic<ProcedureManager> mock = mockStatic(ProcedureManager.class)) {
            mock.when(ProcedureManager::getInstance).thenReturn(mockManager);
            String result = RealStatsLogType.PROCEDURE_NUM.getLog();
            assertEquals("12", result);
        }
    }

    @Test
    public void testCountFreshHyperLogLogStatsColumns() {
        // Mock DbInfoManager
        List<String> databaseNames = new ArrayList<>();
        databaseNames.add("db1");
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        when(dbInfoManager.getDbList()).thenReturn(databaseNames);

        // Mock OptimizerContext and TableMeta
        OptimizerContext context = mock(OptimizerContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(context.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));
        when(context.getLatestSchemaManager().getAllUserTables()).thenReturn(Arrays.asList(tableMeta));

        // Mock StatisticManager and CacheLine
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(cacheLine.getRowCount()).thenReturn(100001L);
        StatisticManager statisticManager = mock(StatisticManager.class);
        when(statisticManager.getCacheLine("db1", "table1")).thenReturn(cacheLine);

        // Mock GlobalIndexMeta and TableIndexMap
        Map<String, Map<String, List<String>>> indexColsMap = new HashMap<>();
        Map<String, List<String>> indexColumnMap = new HashMap<>();
        indexColumnMap.put("index1", Arrays.asList("col1", "col2"));
        indexColsMap.put("indexKey", indexColumnMap);
        when(tableMeta.getTableName()).thenReturn("table1");

        try (MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);
        ) {
            globalIndexMetaMockedStatic.when(() -> GlobalIndexMeta.getTableIndexMap(tableMeta, null))
                .thenReturn(indexColsMap);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("db1")).thenReturn(context);
            statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(statisticManager);

            // Mock StatisticManager and Sds ndvModifyTime
            StatisticDataSource sds = mock(StatisticDataSource.class);
            long currentTime = System.currentTimeMillis();
            long lastUpdate = currentTime - 1000 * 60 * 60 * 24 * 7 / 2;
            when(sds.ndvModifyTime("db1", "table1", "col1,col2")).thenReturn(lastUpdate);
            when(statisticManager.getSds()).thenReturn(sds);

            long result = RealStatsLogType.countFreshHyperLogLogStatsColumns();

            assertEquals(1, result);
        }
    }

    /**
     * test return 0 when db list is empty
     */
    @Test
    public void testCalculateColumnsForHyperLoglogStatsWhenDatabaseListIsEmpty() {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        when(mockDbInfoManager.getDbList()).thenReturn(new ArrayList<>());

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);
            long result = RealStatsLogType.calculateColumnsForHyperLoglogStats();
            assertEquals(0L, result);
        }
    }

    /**
     * test return 0 when there was no table
     */
    @Test
    public void testCalculateColumnsForHyperLoglogStatsWhenNoTablesExist() {
        List<String> databaseNames = new ArrayList<>();
        databaseNames.add("db1");
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        when(mockDbInfoManager.getDbList()).thenReturn(databaseNames);
        OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        when(mockOptimizerContext.getLatestSchemaManager()).thenReturn(mockSchemaManager);
        when(mockSchemaManager.getAllUserTables()).thenReturn(Collections.emptyList());

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class)) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("db1"))
                .thenReturn(mockOptimizerContext);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            long result = RealStatsLogType.calculateColumnsForHyperLoglogStats();

            assertEquals(0L, result);
        }
    }

    /**
     * test sample size
     */
    @Test
    public void testCalculateColumnsForHyperLoglogStatsWhenRowCountIsLessThanOrEqualToDefaultSampleSize() {
        List<String> databaseNames = new ArrayList<>();
        databaseNames.add("db1");
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
        OptimizerContext optimizerContextMock = mock(OptimizerContext.class);
        StatisticManager statisticManagerMock = mock(StatisticManager.class);
        TableMeta tableMetaMock = mock(TableMeta.class);

        when(dbInfoManagerMock.getDbList()).thenReturn(databaseNames);
        when(statisticManagerMock.getCacheLine("db1", "table1")).thenReturn(new StatisticManager.CacheLine());
        when(tableMetaMock.getTableName()).thenReturn("table1");

        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        when(optimizerContextMock.getLatestSchemaManager()).thenReturn(mockSchemaManager);
        when(mockSchemaManager.getAllUserTables()).thenReturn(ImmutableList.of(tableMetaMock));

        Map<String, Map<String, List<String>>> indexColsMap = new HashMap<>();
        Map<String, List<String>> indexColumnMap = new HashMap<>();
        indexColumnMap.put("index1", Arrays.asList("col1", "col2"));
        indexColsMap.put("indexKey", indexColumnMap);

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaMockedStatic = mockStatic(GlobalIndexMeta.class);
        ) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("db1"))
                .thenReturn(optimizerContextMock);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);
            statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(statisticManagerMock);
            globalIndexMetaMockedStatic.when(() -> GlobalIndexMeta.getTableIndexMap(tableMetaMock, null))
                .thenReturn(indexColsMap);

            long result = RealStatsLogType.calculateColumnsForHyperLoglogStats();

            assertEquals(0L, result);

            StatisticManager.CacheLine cl = new StatisticManager.CacheLine();
            cl.setRowCount(100001L);

            when(statisticManagerMock.getCacheLine("db1", "table1")).thenReturn(cl);

            result = RealStatsLogType.calculateColumnsForHyperLoglogStats();

            assertEquals(2L, result);
        }
    }

    /**
     * test return 0 while db list is empty
     */
    @Test
    public void testCalculateColumnsForSamplingStatsWhenNoDatabases() {
        // 准备
        List<String> emptyDatabaseNames = new ArrayList<>();
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
        when(dbInfoManagerMock.getDbList()).thenReturn(emptyDatabaseNames);

        // 执行
        long result = RealStatsLogType.calculateColumnsForSamplingStats();

        // 验证
        assertEquals(0L, result);
    }

    /**
     * test json type meta columns
     */
    @Test
    public void testCalculateColumnsForSamplingStatsExcludingBinaryAndJsonTypes() {
        // 准备
        List<String> databaseNames = Arrays.asList("db1");
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
        when(dbInfoManagerMock.getDbList()).thenReturn(databaseNames);

        OptimizerContext optimizerContextMock = mock(OptimizerContext.class);
        SchemaManager schemaManagerMock = mock(SchemaManager.class);
        TableMeta tableMetaMock = mock(TableMeta.class);
        ColumnMeta binaryColumnMock = mock(ColumnMeta.class);
        ColumnMeta jsonColumnMock = mock(ColumnMeta.class);

        when(optimizerContextMock.getLatestSchemaManager()).thenReturn(schemaManagerMock);
        when(schemaManagerMock.getAllUserTables()).thenReturn(Arrays.asList(tableMetaMock));
        when(tableMetaMock.getAllColumns()).thenReturn(Arrays.asList(binaryColumnMock, jsonColumnMock));
        try (MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);) {
            statisticUtilsMockedStatic.when(() -> StatisticUtils.isBinaryOrJsonColumn(binaryColumnMock))
                .thenReturn(true);
            statisticUtilsMockedStatic.when(() -> StatisticUtils.isBinaryOrJsonColumn(jsonColumnMock)).thenReturn(true);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("db1"))
                .thenReturn(optimizerContextMock);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);

            long result = RealStatsLogType.calculateColumnsForSamplingStats();

            assertEquals(0L, result);

            statisticUtilsMockedStatic.when(() -> StatisticUtils.isBinaryOrJsonColumn(jsonColumnMock))
                .thenReturn(false);

            result = RealStatsLogType.calculateColumnsForSamplingStats();

            assertEquals(1L, result);
        }
    }

    @Test
    public void testCountFreshSampleStatsColumnsWhenDbListIsEmpty() {
        List<String> emptyDatabaseSchemas = new ArrayList<>();
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
        when(dbInfoManagerMock.getDbList()).thenReturn(emptyDatabaseSchemas);

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);) {
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);

            long result = RealStatsLogType.countFreshSampleStatsColumns();

            assertEquals(0L, result);
        }

    }

    /**
     * test return 0 when there is no db or table
     */
    @Test
    public void testCountFreshSampleStatsColumnsWhenNoTablesInSchema() {
        // 准备
        String schema = "test_schema";
        List<String> databaseSchemas = ImmutableList.of(schema);
        OptimizerContext context = mock(OptimizerContext.class);
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);

        when(dbInfoManagerMock.getDbList()).thenReturn(databaseSchemas);
        when(context.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(schema))
                .thenReturn(context);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);
            statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(mock(StatisticManager.class));

            long result = RealStatsLogType.countFreshSampleStatsColumns();

            verify(context).getLatestSchemaManager();
            verifyNoInteractions(StatisticManager.getInstance());
            assertEquals(0L, result);
        }
    }

    @Test
    public void testCountFreshSampleStatsColumns() {
        // 准备
        String schema = "test_schema";
        List<String> databaseSchemas = ImmutableList.of(schema);
        OptimizerContext context = mock(OptimizerContext.class);
        DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(dbInfoManagerMock.getDbList()).thenReturn(databaseSchemas);
        when(context.getLatestSchemaManager()).thenReturn(schemaManager);

        ColumnMeta columnMeta1 = mock(ColumnMeta.class);
        ColumnMeta columnMeta2 = mock(ColumnMeta.class);

        when(columnMeta1.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);

        ArrayList<ColumnMeta> allNonBinaryOrJson = new ArrayList<>(Arrays.asList(
            columnMeta1,
            columnMeta2
        ));

        TableMeta tableMeta = mock(TableMeta.class);
        List<TableMeta> tableMetas = Lists.newArrayList();
        tableMetas.add(tableMeta);
        when(schemaManager.getAllUserTables()).thenReturn(tableMetas);
        when(tableMeta.getTableName()).thenReturn("table1");
        when(tableMeta.getAllColumns()).thenReturn(allNonBinaryOrJson);

        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);

        StatisticManager statisticManager = mock(StatisticManager.class);
        when(statisticManager.getCacheLine(schema, "table1")).thenReturn(cacheLine);
        when(cacheLine.hasExpire()).thenReturn(false);

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(schema))
                .thenReturn(context);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);
            statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(statisticManager);

            long result = RealStatsLogType.countFreshSampleStatsColumns();

            verify(context).getLatestSchemaManager();
            assertEquals(2L, result);
        }
    }

    @Test
    public void testSqlUdfCount() {
        StoredFunctionManager mockManager = mock(StoredFunctionManager.class);
        when(mockManager.getUdfCount()).thenReturn(0L);
        try (MockedStatic<StoredFunctionManager> mock = mockStatic(StoredFunctionManager.class)) {
            mock.when(StoredFunctionManager::getInstance).thenReturn(mockManager);
            String result = RealStatsLogType.SQL_UDF_NUM.getLog();
            assertEquals("0", result);
        }
    }

    public void testCountColumnsWithoutBinaryOrJsonEmptyList() {
        ArrayList<ColumnMeta> emptyList = new ArrayList<>();
        long expected = 0; // 期望的结果
        long actual = RealStatsLogType.countColumnsWithoutBinaryOrJson(emptyList);
        assertEquals(expected, actual); // 验证实际结果等于预期结果
    }

    @Test
    public void testCountColumnsWithoutBinaryOrJsonAllNonBinaryOrJson() {
        ColumnMeta columnMeta1 = mock(ColumnMeta.class);
        ColumnMeta columnMeta2 = mock(ColumnMeta.class);

        when(columnMeta1.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);

        ArrayList<ColumnMeta> allNonBinaryOrJson = new ArrayList<>(Arrays.asList(
            columnMeta1,
            columnMeta2
        ));
        long expected = 2;
        long actual = RealStatsLogType.countColumnsWithoutBinaryOrJson(allNonBinaryOrJson);

        assertEquals(expected, actual);
    }

    @Test
    public void testCountColumnsWithoutBinaryOrJsonMixedTypes() {
        ColumnMeta columnMeta1 = mock(ColumnMeta.class);
        ColumnMeta columnMeta2 = mock(ColumnMeta.class);
        ColumnMeta columnMeta3 = mock(ColumnMeta.class);

        when(columnMeta1.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.BinaryType);

        ArrayList<ColumnMeta> mixedTypes = new ArrayList<>(Arrays.asList(
            columnMeta1,
            columnMeta2,
            columnMeta3
        ));

        long expected = 2;
        long actual = RealStatsLogType.countColumnsWithoutBinaryOrJson(mixedTypes);

        assertEquals(expected, actual);
    }

    @Test
    public void testCountColumnsWithMissingStatisticsEmptyList() {

        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);

        long result = RealStatsLogType.countColumnsWithNotExpiredStatistics(cacheLine, Lists.newArrayList());

        assertEquals(0L, result);
    }

    @Test
    public void testCountColumnsWithMissingStatisticsAllComplete() {
        ColumnMeta columnMeta1 = mock(ColumnMeta.class);
        ColumnMeta columnMeta2 = mock(ColumnMeta.class);
        ColumnMeta columnMeta3 = mock(ColumnMeta.class);

        when(columnMeta1.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.BinaryType);

        ArrayList<ColumnMeta> mixedTypes = new ArrayList<>(Arrays.asList(
            columnMeta1,
            columnMeta2,
            columnMeta3
        ));

        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        try (
            MockedStatic<RealStatsLogType> realStatsLogTypeMockedStatic = mockStatic(RealStatsLogType.class);) {
            realStatsLogTypeMockedStatic.when(
                    () -> RealStatsLogType.countColumnsWithNotExpiredStatistics(cacheLine, mixedTypes))
                .thenCallRealMethod();
            realStatsLogTypeMockedStatic.when(() -> RealStatsLogType.hasMissingStatistics(any(), anyString()))
                .thenReturn(false);

            long result = RealStatsLogType.countColumnsWithNotExpiredStatistics(cacheLine, mixedTypes);

            assertEquals(2L, result);
        }
    }

    @Test
    public void testJavaUdfCount() {
        JavaFunctionManager mockManager = mock(JavaFunctionManager.class);
        when(mockManager.getFuncNum()).thenReturn(11L);
        try (MockedStatic<JavaFunctionManager> mock = mockStatic(JavaFunctionManager.class)) {
            mock.when(JavaFunctionManager::getInstance).thenReturn(mockManager);
            String result = RealStatsLogType.JAVA_UDF_NUM.getLog();
            assertEquals("11", result);
        }
    }

    @Test
    public void testCountColumnsWithMissingStatisticsSomeMissing() {
        ColumnMeta columnMeta1 = mock(ColumnMeta.class);
        ColumnMeta columnMeta2 = mock(ColumnMeta.class);
        ColumnMeta columnMeta3 = mock(ColumnMeta.class);

        when(columnMeta1.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.BinaryType);
        when(columnMeta1.getName()).thenReturn("c1");
        when(columnMeta2.getName()).thenReturn("c2");
        when(columnMeta3.getName()).thenReturn("c3");

        ArrayList<ColumnMeta> mixedTypes = new ArrayList<>(Arrays.asList(
            columnMeta1,
            columnMeta2,
            columnMeta3
        ));

        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        try (
            MockedStatic<RealStatsLogType> realStatsLogTypeMockedStatic = mockStatic(RealStatsLogType.class);) {
            realStatsLogTypeMockedStatic.when(
                    () -> RealStatsLogType.countColumnsWithNotExpiredStatistics(cacheLine, mixedTypes))
                .thenCallRealMethod();
            realStatsLogTypeMockedStatic.when(() -> RealStatsLogType.hasMissingStatistics(cacheLine, "c1"))
                .thenReturn(false);
            realStatsLogTypeMockedStatic.when(() -> RealStatsLogType.hasMissingStatistics(cacheLine, "c2"))
                .thenReturn(true);
            realStatsLogTypeMockedStatic.when(() -> RealStatsLogType.hasMissingStatistics(cacheLine, "c3"))
                .thenReturn(false);

            long result = RealStatsLogType.countColumnsWithNotExpiredStatistics(cacheLine, mixedTypes);

            assertEquals(1L, result);
        }
    }

    @Test
    public void testHasMissingStatisticsWithValidStats() {
        StatisticManager.CacheLine mockCacheLine = mock(StatisticManager.CacheLine.class);

        Map<String, Histogram> histogramMap = Maps.newHashMap();
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        histogram.buildFromData(list);

        histogramMap.put("column_name", histogram);
        when(mockCacheLine.getTopN("column_name")).thenReturn(new TopN(new IntegerType(), 1));
        when(mockCacheLine.getHistogramMap()).thenReturn(histogramMap);

        boolean result = RealStatsLogType.hasMissingStatistics(mockCacheLine, "Column_Name");

        assertFalse("当缓存行包含有效的统计信息时，应该返回false", result);
    }

    @Test
    public void testHasMissingStatisticsTopNIsNull() {
        String colName = "column_name";
        Map<String, Long> nullCountMap = Maps.newHashMap();
        nullCountMap.put(colName, 1L);
        StatisticManager.CacheLine mockCacheLine = mock(StatisticManager.CacheLine.class);
        when(mockCacheLine.getTopN("column_name")).thenReturn(null);
        when(mockCacheLine.getRowCount()).thenReturn(2L);
        when(mockCacheLine.getNullCountMap()).thenReturn(nullCountMap);

        boolean result = RealStatsLogType.hasMissingStatistics(mockCacheLine, colName);

        assertTrue(result, "当topN统计为空时，应该返回true");
    }

    @Test
    public void testHasMissingStatisticsHistogramMapIsNull() {
        String colName = "column_name";
        Map<String, Long> nullCountMap = Maps.newHashMap();
        nullCountMap.put(colName, 1L);
        StatisticManager.CacheLine mockCacheLine = mock(StatisticManager.CacheLine.class);
        when(mockCacheLine.getHistogramMap()).thenReturn(null);
        when(mockCacheLine.getRowCount()).thenReturn(2L);
        when(mockCacheLine.getNullCountMap()).thenReturn(nullCountMap);

        boolean result = RealStatsLogType.hasMissingStatistics(mockCacheLine, colName);

        assertTrue(result, "当缓存行的直方图映射为null时，应该返回true");
    }
}
