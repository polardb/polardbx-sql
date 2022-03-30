package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row;
import com.google.common.collect.Lists;
import org.junit.Test;

public class StatisticTest {
    @Test
    public void smokeTest() {
        MockStatisticDatasource msd = new MockStatisticDatasource();
        StatisticService sm = new StatisticManager("not_exist_db", msd);

        String tableName = "not_exist_table";
        String columnName = "not_exist_column";
        String value = "not_exist_value";
        Row.RowValue rowValue = new Row.RowValue(Lists.newArrayList(value));

        StringBuilder s = new StringBuilder();
        s.append("getFrequency(tableName, columnName, value):")
            .append(sm.getFrequency(tableName, columnName, value))
            .append("\n")
            .append("sm.getFrequency(tableName, columnName, rowValue):")
            .append(sm.getFrequency(tableName, columnName, rowValue))
            .append("\n")
            .append("sm.getRowCount(tableName):")
            .append(sm.getRowCount(tableName))
            .append("\n")
            .append("sm.getCardinality(tableName, columnName):")
            .append(sm.getCardinality(tableName, columnName))
            .append("\n")
            .append("sm.getNullCount(tableName, columnName):")
            .append(sm.getNullCount(tableName, columnName))
            .append("\n")
            .append("sm.getRangeCount(tableName, columnName, value, true, value, true):")
            .append(sm.getRangeCount(tableName, columnName, value, true, value, true))
            .append("\n")
            .append("sm.getDataType(tableName, columnName):")
            .append(sm.getDataType(tableName, columnName))
            .append("\n")
            .append("sm.getStatisticLogInfo():")
            .append(sm.getStatisticLogInfo())
            .append("\n")
            .append("sm.getAutoAnalyzeTask():")
            .append(sm.getAutoAnalyzeTask())
            .append("\n")
            .append("sm.getTableNamesCollected():")
            .append(sm.getTableNamesCollected())
            .append("\n")
        ;

        sm.renameTable(tableName, tableName);
        sm.removeLogicalColumnList(tableName, Lists.newArrayList(columnName));
        sm.addUpdateRowCount(tableName, 1L);
        sm.sampleTable(tableName);
        System.out.println(s.toString());

    }
}
