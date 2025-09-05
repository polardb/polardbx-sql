package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.STATISTIC_PERSIST_FAIL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatisticSubProcessUtilsTest {

    @Test
    public void testSampleTableDdl1() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);
    }

    @Test
    public void testSampleTableDdl2() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        List<ColumnMeta> analyzeColumnList = Lists.newArrayList();
        analyzeColumnList.add(new ColumnMeta("schema", "table", "column", null));

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);

        try (MockedStatic<com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils> mock = mockStatic(
            com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.class);
            MockedStatic<StatisticManager> mockStatisticManager = mockStatic(StatisticManager.class);
            MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
        ) {
            mock.when(
                () -> com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas(anyBoolean(),
                    anyString(), anyString())).thenReturn(analyzeColumnList);
            mockStatisticManager.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);

            when(cacheLine.getRowCount()).thenReturn(Long.MAX_VALUE);
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getSampleRate(anyLong())).thenReturn(1f);

            StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);

            verify(cacheLine, times(1)).remainColumns(analyzeColumnList);
            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.statisticsAlert(anyString(), anyString(),
                    eq(OptimizerAlertType.STATISTIC_SAMPLE_FAIL), any(), anyString()), times(1));

            when(cacheLine.getRowCount()).thenReturn(10L);
            statisticUtilsMockedStatic.when(
                () -> StatisticUtils.scanAnalyze(anyString(), anyString(), anyList(), anyFloat(), anyInt(), anyList(),
                    anyBoolean())).thenReturn(10D);

            StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);

            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.checkStatisticsMiss(anyString(), anyString(), eq(cacheLine), anyInt()),
                times(1));
        }
    }

    @Test
    public void testPersistStatistic() throws SQLException {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        StatisticDataSource statisticDataSource = mock(StatisticDataSource.class);
        when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);
        when(statisticManager.getSds()).thenReturn(statisticDataSource);

        Map<String, Long> cardinalityMap = Maps.newConcurrentMap();
        cardinalityMap.put("column", 10L);

        Map<String, Long> nullCountMap = Maps.newConcurrentMap();
        nullCountMap.put("column", 10L);

        when(cacheLine.getCardinalityMap()).thenReturn(cardinalityMap);
        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticManager> mockStatisticManager = mockStatic(StatisticManager.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
            ) {
            mockStatisticManager.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);

            StatisticSubProcessUtils.persistStatistic("schema", "table", true, null);

            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.statisticsAlert(anyString(), anyString(), eq(STATISTIC_PERSIST_FAIL), any(),
                    any()), times(1));
            when(cacheLine.getNullCountMap()).thenReturn(nullCountMap);

            StatisticSubProcessUtils.persistStatistic("schema", "table", true, null);

            statisticUtilsMockedStatic.verify(() -> StatisticUtils.updateMetaDbInformationSchemaTables(anyString(), anyString()),
                times(1));
        }
    }
}
