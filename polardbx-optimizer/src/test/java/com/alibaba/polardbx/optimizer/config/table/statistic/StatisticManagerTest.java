package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_STATISTIC_FEEDBACK;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_SKIPPED;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildSketchKey;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.digestForStatisticTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class StatisticManagerTest {

    @Test
    public void testHandleInFrequencyDegradation() {
        StatisticResult rowCount = StatisticResult.build().setValue(0L, null);
        StatisticResult ndv = StatisticResult.build().setValue(0L, null);
        StatisticResult statisticResult =
            StatisticManager.handleInFrequencyDegradation(rowCount, ndv, null, null, null, null, true, -1L);
        assert statisticResult.getLongValue() == 0L;

        rowCount.setValue(100000L, null);
        ndv.setValue(10000L, null);
        List<Object> rowList = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            rowList.add(i);
        }
        statisticResult =
            StatisticManager.handleInFrequencyDegradation(rowCount, ndv, null, null, null, rowList, true, -1L);
        assert statisticResult.getLongValue() == 1000L;
    }

    @Test
    public void testNdvOutlierIllegalArg1() {
        try (MockedStatic<ConfigDataMode> configDataMode = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataMode.when(() -> ConfigDataMode.isFastMock()).thenReturn(true);
            // test illegal args
            StatisticManager statisticManager = StatisticManager.getInstance();
            try {
                statisticManager.getCardinality("schema", "table", "", false, true);
            } catch (IllegalArgumentException e) {
                return;
            }
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        }
    }

    @Test
    public void testNdvOutlierWithNullSource() {
        String schema = "schema";
        String table = "table";
        String col = "col";
        try (MockedStatic<ConfigDataMode> configDataMode = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataMode.when(() -> ConfigDataMode.isFastMock()).thenReturn(true);
            StatisticManager statisticManager = StatisticManager.getInstance();

            StatisticResult statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == -1L;
            assert statisticResult.getSource() == StatisticResultSource.NULL;
        }
    }

    @Test
    public void testNdvOutlier() {
        String schema = "schema";
        String table = "table";
        String col = "col";
        try (MockedStatic<ConfigDataMode> configDataMode = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataMode.when(() -> ConfigDataMode.isFastMock()).thenReturn(true);
            StatisticManager statisticManager = StatisticManager.getInstance();
            statisticManager.sds = MockStatisticDatasource.getInstance();
            // ndv is 0, rowcount is 0, then return 0 with hll source
            statisticManager.getCardinalitySketch().put(buildSketchKey(schema, table, col), 0L);
            StatisticResult statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == 0L;
            assert statisticResult.getSource() == StatisticResultSource.HLL_SKETCH;

            // ndv is 0, rowcount is not 0, topn&histogram were both null, then return 0 with hll source
            statisticManager.getCardinalitySketch().put(buildSketchKey(schema, table, col), 0L);
            statisticManager.getStatisticCache().get(schema).get(table).setRowCount(100000L);
            statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == 0L;
            assert statisticResult.getSource() == StatisticResultSource.HLL_SKETCH;

            // ndv is 0, rowcount is not 0, topn&histogram weren't both null, then return -1 with NULL source
            statisticManager.getCardinalitySketch().put(buildSketchKey(schema, table, col), 0L);
            statisticManager.getStatisticCache().get(schema).get(table).setRowCount(100000L);
            statisticManager.getStatisticCache().get(schema).get(table)
                .setTopN(col, new TopN(DataTypes.StringType, 1.0));
            statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == -1L;
            assert statisticResult.getSource() == StatisticResultSource.NULL;

            // ndv is not 0, rowcount is not 0, topn&histogram weren't both null, then return ndv with HLL_SKETCH source
            statisticManager.getCardinalitySketch().put(buildSketchKey(schema, table, col), 1L);
            statisticManager.getStatisticCache().get(schema).get(table).setRowCount(100000L);
            statisticManager.getStatisticCache().get(schema).get(table)
                .setTopN(col, new TopN(DataTypes.StringType, 1.0));
            statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == 1L;
            assert statisticResult.getSource() == StatisticResultSource.HLL_SKETCH;
        }
    }

    @Test
    public void testUpdateAllShardParts() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        String schema = "test_schema";
        String tableName = "test_table";
        String columnName = "test_col";
        StatisticManager statisticManager = new MockStatisticManager();

        statisticManager.sds = mock(StatisticDataSource.class);
        statisticManager.updateAllShardParts(schema, tableName, columnName, new ExecutionContext(), null);

        assertTrue(statisticManager.getCardinalitySketch().containsKey("test_schema:test_table:test_col"));
    }

    @Test
    public void testEnableHll() {
        String schema = "schema";
        String table = "table";
        String col = "col";
        try (MockedStatic<ConfigDataMode> configDataMode = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataMode.when(() -> ConfigDataMode.isFastMock()).thenReturn(true);
            StatisticManager statisticManager = StatisticManager.getInstance();
            statisticManager.sds = MockStatisticDatasource.getInstance();
            statisticManager.getStatisticCache().put(schema, new ConcurrentHashMap<>());
            statisticManager.getStatisticCache().get(schema).put(table, new StatisticManager.CacheLine());
            statisticManager.getStatisticCache().get(schema).get(table).setCardinalityMap(new HashMap<>());
            statisticManager.getStatisticCache().get(schema).get(table).getCardinalityMap().put(col, 100L);
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_HLL, "false");
            StatisticResult statisticResult = statisticManager.getCardinality(schema, table, col, false, true);
            System.out.println(statisticResult.getLongValue());
            System.out.println(statisticResult.getTrace().print());
            assert statisticResult.getLongValue() == 100L;
            assert statisticResult.getSource() == StatisticResultSource.CACHE_LINE;
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_HLL, "true");
        }
    }

    @Test
    public void testGetFrequencyInnerWithCorrection() {
        String schema = "testSchema";
        String logicalTableName = "testTable";
        String columnName = "testColumn";
        String value = "testValue";
        boolean isNeedTrace = true;
        StatisticManager statisticManager = mock(StatisticManager.class);

        when(statisticManager.getCorrectionResult(anyString(), anyString(), eq(isNeedTrace)))
            .thenReturn(new StatisticResult().setSource(StatisticResultSource.CORRECTIONS).setValue(10L, null));
        when(statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value,
            isNeedTrace)).thenCallRealMethod();

        StatisticResult result =
            statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value, isNeedTrace);

        assertEquals(10, result.getLongValue());
        assertEquals(StatisticResultSource.CORRECTIONS, result.getSource());
        verify(statisticManager).getCorrectionResult(
            eq(schema.toLowerCase() + "," + logicalTableName.toLowerCase() + "," + columnName.toLowerCase() + ","
                + digestForStatisticTrace(value)),
            eq("getFrequency"), eq(isNeedTrace));
    }

    @Test
    public void testGetFrequencyInnerWithTopN() {
        String schema = "testSchema";
        String logicalTableName = "testTable";
        String columnName = "testColumn";
        String value = "testValue";
        boolean isNeedTrace = true;
        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        TopN topN = mock(TopN.class);

        when(cacheLine.getTopN(columnName.toLowerCase())).thenReturn(topN);
        when(topN.rangeCount(value, true, value, true)).thenReturn(10L);
        when(cacheLine.getSampleRate()).thenReturn(0.5F);
        when(statisticManager.getCacheLine(schema.toLowerCase(), logicalTableName.toLowerCase())).thenReturn(cacheLine);
        when(statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value,
            isNeedTrace)).thenCallRealMethod();

        StatisticResult result =
            statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value, isNeedTrace);

        assertEquals(20L, result.getLongValue());
        assertEquals(StatisticResultSource.TOP_N, result.getSource());
        verify(statisticManager).getCacheLine(eq(schema.toLowerCase()), eq(logicalTableName.toLowerCase()));
        verify(cacheLine).getTopN(columnName.toLowerCase());
        verify(cacheLine, times(2)).getSampleRate();
    }

    @Test
    public void testGetFrequencyInnerWithSampleNdvHllSketchAlike() {
        String schema = "testSchema";
        String logicalTableName = "testTable";
        String columnName = "testColumn";
        String value = "testValue";
        boolean isNeedTrace = true;
        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        Map<String, Long> ndvMap = new HashMap<>();
        ndvMap.put(columnName.toLowerCase(), 100L);

        try (MockedStatic<GeneralUtil> generalUtilMockedStatic = Mockito.mockStatic(GeneralUtil.class)) {
            generalUtilMockedStatic.when(() -> GeneralUtil.isWithinPercentage(anyLong(), anyLong(), anyDouble()))
                .thenReturn(true);
            Map<String, Histogram> map = new HashMap<>();
            when(cacheLine.getCardinalityMap()).thenReturn(ndvMap);
            when(cacheLine.getHistogramMap()).thenReturn(map);
            when(cacheLine.getSampleRate()).thenReturn(1.0f);
            when(statisticManager.getCacheLine(any(), anyString())).thenReturn(cacheLine);
            when(statisticManager.getCardinality(schema.toLowerCase(), logicalTableName.toLowerCase(),
                columnName.toLowerCase(), true,
                isNeedTrace)).thenReturn(
                new StatisticResult().setValue(11, mock(StatisticTrace.class))
                    .setSource(StatisticResultSource.HLL_SKETCH));
            when(statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value,
                isNeedTrace)).thenCallRealMethod();

            Histogram histogram = mock(Histogram.class);
            when(histogram.rangeCount(any(), anyBoolean(), anyString(), anyBoolean())).thenReturn(13L);
            map.put(columnName.toLowerCase(), histogram);

            StatisticResult result =
                statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value, isNeedTrace);

            assertEquals(13L, result.getLongValue());
            assertEquals(StatisticResultSource.HISTOGRAM, result.getSource());
            verify(statisticManager).getCacheLine(eq(schema.toLowerCase()), eq(logicalTableName.toLowerCase()));
        }
    }

    @Test
    public void testGetFrequencyInnerWithSampleNdvHllSketchDislike() {
        String schema = "testSchema";
        String logicalTableName = "testTable";
        String columnName = "testColumn";
        String value = "testValue";
        boolean isNeedTrace = true;
        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        Map<String, Long> ndvMap = new HashMap<>();
        ndvMap.put(columnName.toLowerCase(), 100L);

        when(cacheLine.getCardinalityMap()).thenReturn(ndvMap);
        when(statisticManager.getCacheLine(schema.toLowerCase(), logicalTableName.toLowerCase())).thenReturn(cacheLine);
        when(statisticManager.getCardinality(schema.toLowerCase(), logicalTableName.toLowerCase(),
            columnName.toLowerCase(), true,
            isNeedTrace)).thenReturn(
            new StatisticResult().setValue(11, mock(StatisticTrace.class)).setSource(StatisticResultSource.HLL_SKETCH));
        when(statisticManager.getRowCount(eq(schema.toLowerCase()), eq(logicalTableName.toLowerCase()),
            eq(isNeedTrace))).thenReturn(
            new StatisticResult().setValue(100000, mock(StatisticTrace.class))
                .setSource(StatisticResultSource.CACHE_LINE));
        when(statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value,
            isNeedTrace)).thenCallRealMethod();

        try (MockedStatic<GeneralUtil> generalUtilMockedStatic = Mockito.mockStatic(GeneralUtil.class)) {
            generalUtilMockedStatic.when(() -> GeneralUtil.isWithinPercentage(anyLong(), anyLong(), anyDouble()))
                .thenReturn(false);

            StatisticResult result =
                statisticManager.getFrequencyInner(schema, logicalTableName, columnName, value, isNeedTrace);

            assertEquals(9090L, result.getLongValue());
            assertEquals(StatisticResultSource.CACHE_LINE, result.getSource());
            verify(statisticManager).getCacheLine(eq(schema.toLowerCase()), eq(logicalTableName.toLowerCase()));
        }
    }

    @Test
    public void testFeedback() {
        StatisticManager statisticManager = mock(StatisticManager.class);
        ModuleLogInfo moduleLogInfo = mock(ModuleLogInfo.class);

        String schema = "schema";
        String tbl = "tbl";
        try (MockedStatic<ModuleLogInfo> moduleLogInfoMockedStatic = Mockito.mockStatic(ModuleLogInfo.class);
            MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            moduleLogInfoMockedStatic.when(() -> ModuleLogInfo.getInstance()).thenReturn(moduleLogInfo);
            Mockito.doCallRealMethod().when(statisticManager).feedback(anyString(), anyString());
            // 1 branch, cache line is null
            when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(null);

            statisticManager.feedback(schema, tbl);

            verify(moduleLogInfo).logError(any(), anyString(), any());
            clearInvocations(moduleLogInfo);

            // 2 branch, feedback quit by time check
            StatisticManager.CacheLine cacheLine = new StatisticManager.CacheLine();
            cacheLine.setLastModifyTime(GeneralUtil.unixTimeStamp());
            when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);

            statisticManager.feedback(schema, tbl);

            verify(moduleLogInfo).logInfo(Module.STATISTICS, PROCESS_END,
                new String[] {"FEEDBACK", schema + ":" + tbl});
            clearInvocations(moduleLogInfo);

            // 3 branch, feedback job submit its job
            cacheLine.setLastModifyTime(GeneralUtil.unixTimeStamp() - 100);
            cacheLine.setRowCount(1001L);
            Assert.assertTrue(cacheLine.addUpdateRowCount(5L) == 1006L);
            StatisticManager.setExecutor(mock(ThreadPoolExecutor.class));

            statisticManager.feedback(schema, tbl);

            Assert.assertTrue(cacheLine.addUpdateRowCount(0L) == 1001L);
        }
    }

    @Test
    public void testFeedbackJob() {
        ModuleLogInfo moduleLogInfo = mock(ModuleLogInfo.class);
        try (MockedStatic<ModuleLogInfo> moduleLogInfoMockedStatic = Mockito.mockStatic(ModuleLogInfo.class);
            MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            moduleLogInfoMockedStatic.when(() -> ModuleLogInfo.getInstance()).thenReturn(moduleLogInfo);
            StatisticManager statisticManager = new MockStatisticManager();

            // 1 branch, feedback job quit by time check
            StatisticManager.CacheLine cacheLine = new StatisticManager.CacheLine();
            cacheLine.setLastModifyTime(GeneralUtil.unixTimeStamp());

            statisticManager.feedbackJob("schema", "table", cacheLine);

            verify(moduleLogInfo, times(1)).logInfo(Module.STATISTICS, PROCESS_END,
                new String[] {"FEEDBACK", "schema:table"});
            clearInvocations(moduleLogInfo);

            // 2 branch, feedback job quit by ENABLE_STATISTIC_FEEDBACK is false
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_STATISTIC_FEEDBACK))
                .thenReturn(false);

            cacheLine.setLastModifyTime(GeneralUtil.unixTimeStamp() - 100);

            statisticManager.feedbackJob("schema", "table", cacheLine);

            verify(moduleLogInfo, times(1)).logInfo(Module.STATISTICS, PROCESS_SKIPPED,
                new String[] {
                    "statistic feedback schema,table",
                    ENABLE_STATISTIC_FEEDBACK + " is false"});
            clearInvocations(moduleLogInfo);

            // 3 branch, feedback job finished its job
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_STATISTIC_FEEDBACK))
                .thenReturn(true);

            cacheLine.setLastModifyTime(GeneralUtil.unixTimeStamp() - 100);

            statisticManager.sds = mock(StatisticDataSource.class);

            statisticManager.feedbackJob("schema", "table", cacheLine);

            verify(moduleLogInfo, times(1)).logInfo(eq(Module.STATISTICS), eq(PROCESS_END), any());
            clearInvocations(moduleLogInfo);
        }
    }

    @Test
    public void testGetSingleValFrequencyFromHistogramWithValidHistogram() {
        String serialize =
            "{\"buckets\":[{\"ndv\":1200,\"upper\":-9,\"lower\":-999,\"count\":1200,\"preSum\":0},"
                + "{\"ndv\":1200,\"upper\":100,\"lower\":10,\"count\":120001,\"preSum\":1200},"
                + "{\"ndv\":1200,\"upper\":299,\"lower\":150,\"count\":1200,\"preSum\":121201}],"
                + "\"maxBucketSize\":64,\"type\":\"Int\",\"sampleRate\":0.0098707926}";

        Histogram histogram = Histogram.deserializeFromJson(serialize);
        Map<String, Histogram> histogramMap = new HashMap<>();
        histogramMap.put("columnName", histogram);

        StatisticResult result =
            StatisticManager.getSingleValFrequencyFromHistogram("schema", "table", "columnName", "9", histogramMap,
                false, System.currentTimeMillis(), 1.0f);

        assertNull(result);

        result =
            StatisticManager.getSingleValFrequencyFromHistogram("schema", "table", "columnName", "55", histogramMap,
                false, System.currentTimeMillis(), 1.0f);

        assertEquals(10130L, result.getValue());
    }

    @Test
    public void testGetSingleValFrequencyFromHistogramWithEmptyHistogram() {
        Map<String, Histogram> emptyHistogramMap = new HashMap<>();

        StatisticResult result =
            StatisticManager.getSingleValFrequencyFromHistogram("schema", "table", "columnName", "value",
                emptyHistogramMap, false, System.currentTimeMillis(), 1.0f);

        assertNull(result);
    }
}
