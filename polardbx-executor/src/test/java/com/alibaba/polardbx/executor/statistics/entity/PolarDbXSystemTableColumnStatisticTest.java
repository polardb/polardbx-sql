package com.alibaba.polardbx.executor.statistics.entity;

import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableColumnStatistic;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

/**
 * @author fangwu
 */
public class PolarDbXSystemTableColumnStatisticTest {

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    // 测试用例1: 当传入的直方图字符串可以成功反序列化时，函数应该正常返回空字符串。
    public void testCheckHistogramStringWithValidInput() {
        // 准备
        String histogramString = "{\"buckets\":[[\"a\",1],[\"b\",2]]}";
        String schema = "test_schema";
        String tableName = "test_table";
        String columnName = "test_column";

        // 使用Mockito模拟Histogram类的deserializeFromJson方法，使其返回一个非null对象。
        Histogram mockHistogram = Mockito.mock(Histogram.class);
        try (MockedStatic<Histogram> mockedStatic = Mockito.mockStatic(Histogram.class);) {
            mockedStatic.when(() -> Histogram.deserializeFromJson(histogramString)).thenReturn(mockHistogram);

            String result =
                PolarDbXSystemTableColumnStatistic.checkHistogramString(histogramString, schema, tableName, columnName);

            assertEquals(histogramString, result);
        }
    }

    @Test
    // 测试用例2: 当传入的直方图字符串无法反序列化成Histogram对象时，函数应该记录错误并返回空字符串。
    public void testCheckHistogramStringWithInvalidInput() {
        String schema = "test_schema";
        String tableName = "test_table";
        String columnName = "test_column";

        String result =
            PolarDbXSystemTableColumnStatistic.checkHistogramString("", schema, tableName, columnName);

        assertEquals("", result);
    }
}
