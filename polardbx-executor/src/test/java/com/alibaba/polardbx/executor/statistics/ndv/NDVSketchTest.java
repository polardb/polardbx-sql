package com.alibaba.polardbx.executor.statistics.ndv;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.executor.statistic.ndv.NDVSketch;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.common.utils.Assert.fail;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildSketchKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class NDVSketchTest {
    @Test
    public void testParseWithValidInput() {
        SystemTableNDVSketchStatistic.SketchRow[] sketchRows = createMockSketchRows(3);
        NDVSketch instanceUnderTest = new NDVSketch();

        instanceUnderTest.parse(sketchRows);

        assertTrue(instanceUnderTest.getStringNDVShardSketchMap().size() == 3);
    }

    private SystemTableNDVSketchStatistic.SketchRow[] createMockSketchRows(int count) {
        List<SystemTableNDVSketchStatistic.SketchRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SystemTableNDVSketchStatistic.SketchRow row = mock(SystemTableNDVSketchStatistic.SketchRow.class);
            when(row.getSchemaName()).thenReturn("schema" + i);
            when(row.getTableName()).thenReturn("table" + i);
            when(row.getColumnNames()).thenReturn("column" + i);
            when(row.getShardPart()).thenReturn("part" + i);
            when(row.getIndexName()).thenReturn("index" + i);
            when(row.getDnCardinality()).thenReturn((long) i);
            when(row.getGmtUpdate()).thenReturn((long) (i * 1000));
            when(row.getGmtCreate()).thenReturn((long) (i * 100));
            when(row.getCompositeCardinality()).thenReturn((long) (i + 1));
            rows.add(row);
        }
        return rows.toArray(new SystemTableNDVSketchStatistic.SketchRow[0]);
    }

    @Test
    public void testParseWithEmptyInput() {
        SystemTableNDVSketchStatistic.SketchRow[] emptySketchRows = {};
        NDVSketch instanceUnderTest = new NDVSketch();

        instanceUnderTest.parse(emptySketchRows);

        assertTrue(instanceUnderTest.getStringNDVShardSketchMap().isEmpty());
    }

    @Test
    public void testParseWithDuplicateKeys() {
        SystemTableNDVSketchStatistic.SketchRow[] duplicateSketchRows = createMockSketchRowsWithDuplicates(2);
        NDVSketch instanceUnderTest = new NDVSketch();

        instanceUnderTest.parse(duplicateSketchRows);

        assertTrue(instanceUnderTest.getStringNDVShardSketchMap().size() == 1);
    }

    @Test
    public void testRemove() {
        PolarDbXSystemTableNDVSketchStatistic mockedInstance = mock(PolarDbXSystemTableNDVSketchStatistic.class);
        try (MockedStatic<PolarDbXSystemTableNDVSketchStatistic> mockedStatic = mockStatic(
            PolarDbXSystemTableNDVSketchStatistic.class)) {
            mockedStatic.when(PolarDbXSystemTableNDVSketchStatistic::getInstance).thenReturn(mockedInstance);
            NDVSketch ndvSketch = new NDVSketch();
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 10; j++) {
                    for (int m = 0; m < 5; m++) {
                        String schema = "schema" + i;
                        String table = "tbl" + j;
                        String col = "col" + m;
                        String sketchKey = buildSketchKey(schema, table, col);
                        ndvSketch.getStringNDVShardSketchMap().put(sketchKey, Mockito.mock(NDVShardSketch.class));
                    }
                }
            }
            ndvSketch.remove("schema2", "tbl5");
            Assert.assertTrue(ndvSketch.getStringNDVShardSketchMap().size() == 495);
            verify(mockedInstance, times(5)).deleteByColumn(eq("schema2"), eq("tbl5"), anyString());
            Mockito.clearInvocations(mockedInstance);

            ndvSketch.remove("sChema1", "Tbl5");
            Assert.assertTrue(ndvSketch.getStringNDVShardSketchMap().size() == 490);
            verify(mockedInstance, times(5)).deleteByColumn(eq("schema1"), eq("tbl5"), anyString());
            Mockito.clearInvocations(mockedInstance);

            ndvSketch.remove("", "Tbl5");
            Assert.assertTrue(ndvSketch.getStringNDVShardSketchMap().size() == 490);
            verify(mockedInstance, times(0)).deleteByColumn(anyString(), anyString(), anyString());
        }
    }

    /**
     * 测试用例1: 当传入参数为空时，直接返回不执行后续操作。
     * 确认当任意一个参数为空字符串时，方法正常退出。
     */
    @Test
    public void testUpdateAllShardPartsWhenParamsAreEmptyShouldReturnWithoutExecuting() throws Exception {
        PolarDbXSystemTableNDVSketchStatistic mockedInstance = mock(PolarDbXSystemTableNDVSketchStatistic.class);
        NDVSketch sketchUnderTest = new NDVSketch();

        try (MockedStatic<PolarDbXSystemTableNDVSketchStatistic> mockedStatic = mockStatic(
            PolarDbXSystemTableNDVSketchStatistic.class)) {
            mockedStatic.when(PolarDbXSystemTableNDVSketchStatistic::getInstance).thenReturn(mockedInstance);

            sketchUnderTest.updateAllShardParts("", "tableName", "columnName", mock(ExecutionContext.class),
                mock(ThreadPoolExecutor.class));
            sketchUnderTest.updateAllShardParts("schema", "", "columnName", mock(ExecutionContext.class),
                mock(ThreadPoolExecutor.class));
            sketchUnderTest.updateAllShardParts("schema", "tableName", "", mock(ExecutionContext.class),
                mock(ThreadPoolExecutor.class));

            verify(mockedInstance, times(0)).deleteByColumn(eq("schema"), eq("tablename"), eq("columnname"));
        }
    }

    /**
     * 测试用例2: 当NDVShardSketch有效但部分分区已过期时，应该重新构建。
     * 假设genColumnarHllHint返回非空值。
     */
    @Test
    public void testUpdateAllShardPartsWhenNDVShardSketchHasExpiredPartsShouldRebuild() throws Exception {
        NDVSketch sketchUnderTest = new NDVSketch();

        NDVShardSketch expiredSketch = mock(NDVShardSketch.class);
        when(expiredSketch.getShardKey()).thenReturn("key");
        when(expiredSketch.getShardParts()).thenReturn(Arrays.asList("part1", "part2").toArray(new String[0]));
        sketchUnderTest.getStringNDVShardSketchMap().put("key", expiredSketch);
        PolarDbXSystemTableNDVSketchStatistic mockedInstance = mock(PolarDbXSystemTableNDVSketchStatistic.class);
        try (MockedStatic<NDVShardSketch> mockedStatic = mockStatic(NDVShardSketch.class);
            MockedStatic<PolarDbXSystemTableNDVSketchStatistic> mockedStatic1 = mockStatic(
                PolarDbXSystemTableNDVSketchStatistic.class)) {
            mockedStatic.when(() -> NDVShardSketch.isValidShardPart(anyString(), any())).thenReturn(true);
            mockedStatic.when(
                    () -> NDVShardSketch.genColumnarHllHint(mock(ExecutionContext.class), "schema", "tableName"))
                .thenReturn("hint");
            mockedStatic1.when(PolarDbXSystemTableNDVSketchStatistic::getInstance).thenReturn(mockedInstance);

            when(expiredSketch.anyShardExpired()).thenReturn(true);
            sketchUnderTest.updateAllShardParts("schema", "tableName", "columnName", mock(ExecutionContext.class),
                mock(ThreadPoolExecutor.class));
            verify(mockedInstance).deleteByColumn(eq("schema"), eq("tablename"), eq("columnname"));
        }
    }

    private SystemTableNDVSketchStatistic.SketchRow[] createMockSketchRowsWithDuplicates(int count) {
        List<SystemTableNDVSketchStatistic.SketchRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SystemTableNDVSketchStatistic.SketchRow row = mock(SystemTableNDVSketchStatistic.SketchRow.class);
            when(row.getSchemaName()).thenReturn("schema");
            when(row.getTableName()).thenReturn("table");
            when(row.getColumnNames()).thenReturn("column");
            when(row.getShardPart()).thenReturn("part" + i);
            when(row.getIndexName()).thenReturn("index");
            when(row.getDnCardinality()).thenReturn((long) i);
            when(row.getGmtUpdate()).thenReturn((long) (i * 1000));
            when(row.getGmtCreate()).thenReturn((long) (i * 100));
            when(row.getCompositeCardinality()).thenReturn((long) (i + 1));
            rows.add(row);
        }
        return rows.toArray(new SystemTableNDVSketchStatistic.SketchRow[0]);
    }
}
