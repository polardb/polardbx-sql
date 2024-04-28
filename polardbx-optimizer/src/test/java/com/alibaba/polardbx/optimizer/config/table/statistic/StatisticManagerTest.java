package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.clearspring.analytics.util.Lists;
import org.junit.Test;

import java.util.List;

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
}
