package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import org.junit.Test;

/**
 * @author fangwu
 */
public class StatisticTraceTest {
    @Test
    public void testPrint() {
        StatisticTrace statisticTrace = new StatisticTrace("schema:table:column", "action", "value", "desc",
            StatisticResultSource.CACHE_LINE, 1L);
        System.out.println(statisticTrace.print());
        assert ("Catalog:schema:table:column\n"
            + "Action:action\n"
            + "StatisticValue:value\n"
            + "desc\n"
            + "CACHE_LINE, modify by 1970-01-01 08:00:01\n").equals(statisticTrace.print());
    }
}
