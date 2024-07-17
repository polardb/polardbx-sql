package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildSketchKey;

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
}
