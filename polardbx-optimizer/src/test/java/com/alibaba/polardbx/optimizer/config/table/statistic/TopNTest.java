package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static com.alibaba.polardbx.optimizer.config.table.statistic.TopN.validateTopNValues;

/**
 * test topn of statistic module
 */
public class TopNTest {

    /**
     * test topn interface in cacheline:
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#getTopN(java.lang.String)
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#getTopNColumns()
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#setTopN(java.lang.String, TopN)
     * <p>
     * test serialize topn:
     * com.alibaba.polardbx.optimizer.config.table.statistic.TopN#serializeToJson(TopN)
     * com.alibaba.polardbx.optimizer.config.table.statistic.TopN#deserializeFromJson(java.lang.String)
     */
    @Test
    public void testTopNSerialize() {
        // test null value
        Assert.assertTrue(null == TopN.serializeToJson(null));
        Assert.assertTrue(null == TopN.deserializeFromJson(null));

        StatisticManager.CacheLine cl = new StatisticManager.CacheLine();
        cl.setTopN("TesTColumn", buildTopNForTest());

        Assert.assertTrue(cl.getTopNColumns().contains("testcolumn"));
        Assert.assertTrue(cl.getTopN("testColumn") != null);

        cl.setTopN("teStColumn", null);
        Assert.assertTrue(!cl.getTopNColumns().contains("testcolumn"));
        Assert.assertTrue(cl.getTopN("testColumn") == null);
    }

    /**
     * build string type topn, only for test
     *
     * @return mock topn
     */
    private TopN buildTopNForTest() {
        return new TopN(DataTypes.StringType, 1.0);
    }

    @Test
    public void testTopNWithMultiTimeType() {
        ImmutableList.of(
                // todo test year and time type
                // DataTypes.YearType,
                // DataTypes.TimeType
                DataTypes.TimestampType,
                DataTypes.DatetimeType,
                DataTypes.DateType
            )
            .stream().forEach(t -> testTopNWithSpecifyTimeType(t));
    }

    @Test
    public void testManualReading() {
        DataType dataType = DataTypes.TimestampType;
        TopN topN = new TopN(dataType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-12-1")), 11);
        topN.buildDefault(3, 1);

        String expect = "type:Timestamp\n"
            + "sampleRate:1.0\n"
            + "2001-11-01 00:00:00:5\n"
            + "2001-12-01 00:00:00:11";

        System.out.println(topN.manualReading());
        assert topN.manualReading().replaceAll("\n", "").equals(expect.replaceAll("\n", ""));
    }

    public void testTopNWithSpecifyTimeType(DataType dataType) {
        System.out.println("test time type:" + dataType);
        TopN topN = new TopN(dataType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-12-1")), 11);

        topN.buildDefault(3, 1);

        String json = TopN.serializeToJson(topN);
        System.out.println(json);
        topN = TopN.deserializeFromJson(json);
        System.out.println(TopN.serializeToJson(topN));
        long rs = topN.rangeCount("2001-09-12", true, "2001-11-12", true);
        System.out.println(rs);
        assert rs == 5;

        rs = topN.rangeCount("2001-09-12", true, null, true);
        assert rs == 16;

        rs = topN.rangeCount("1680171880", true, null, true);
        assert rs == 0;

        rs = topN.rangeCount("20011002030303", true, null, true);
        assert rs == 16;

        rs = topN.rangeCount(null, true, null, true);
        assert rs == 0;

        topN.rangeCount(20011002030303L, true, null, true);
    }

    @Test
    public void testTopNRangeCount() {
        DataType dataType = DataTypes.IntegerType;

        TopN topN = new TopN(dataType, 1.0);
        Integer[] values = {2, 4, 6, 8};
        int[] counts = new int[values.length];
        Random r1 = new Random();
        for (int i = 0; i < values.length; i++) {
            counts[i] = r1.nextInt(30) + 1;
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(values.length + 1, 0, false);

        Integer[] keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, null};
        for (int i = 0; i < keys.length; i++) {
            for (int j = 0; j < keys.length; j++) {
                checkRangeCount(topN, keys[i], keys[j], dataType, values, counts);
            }
        }
    }

    @Test
    public void testValidateTopNValuesValidInput() {
        /**
         * Test Case #1: Valid Input - Ensure the method can handle an array that is non-empty and does not contain null or JSONObject elements.
         */
        Object[] validValues = {1, "string", true};
        assertDoesNotThrow(t -> validateTopNValues(t), validValues);
    }

    private <T> void assertDoesNotThrow(Consumer<T> c, T t) {
        try {
            c.accept(t);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testValidateTopNValuesEmptyArray() {
        /**
         * Test Case #2: Empty Array - When an empty array is passed, an IllegalArgumentException should be thrown.
         */
        Object[] emptyArray = {};
        assertThrows(IllegalArgumentException.class, t -> validateTopNValues(t), emptyArray);
    }

    private <T> void assertThrows(Class<? extends Exception> exceptionClass, Consumer<T> c, T t) {
        try {
            c.accept(t);
            throw new RuntimeException("Expected exception was not thrown.");
        } catch (Exception e) {
            e.printStackTrace();
            if (!e.getClass().equals(exceptionClass)) {
                throw new RuntimeException("Expected exception was not thrown.");
            }
        }
    }

    @Test
    public void testValidateTopNValuesNullArray() {
        /**
         * Test Case #3: Null Array - When a null array is passed, an IllegalArgumentException should be thrown.
         */
        Object[] objs = null;
        assertThrows(IllegalArgumentException.class, t -> validateTopNValues(t), objs);
    }

    @Test
    public void testValidateTopNValuesArrayContainsNull() {
        /**
         * Test Case #4: Array Contains Null Element - When the array contains a null element, an IllegalArgumentException should be thrown.
         */
        Object[] arrayWithNull = {"a", null, "b"};
        assertThrows(IllegalArgumentException.class, t -> validateTopNValues(t), arrayWithNull);
    }

    @Test
    public void testValidateTopNValuesArrayContainsJSONObject() {
        /**
         * Test Case #5: Array Contains JSONObject - When the array contains an element of type JSONObject, an IllegalArgumentException should be thrown.
         */
        JSONObject jsonObject = new JSONObject();
        Object[] arrayWithJSONObject = {"a", jsonObject, "b"};
        assertThrows(IllegalArgumentException.class, t -> validateTopNValues(t), arrayWithJSONObject);
    }

    public void checkRangeCount(TopN topN, Object lower, Object upper, DataType dataType, Object[] values,
                                int[] counts) {
        boolean[][] inclusive = new boolean[][] {
            {true, true},
            {true, false},
            {false, true},
            {false, false},
        };
        for (int i = 0; i < inclusive.length; i++) {
            long topNResult = topN.rangeCount(lower, inclusive[i][0], upper, inclusive[i][1]);
            long groundTruth = rangeCount(dataType, lower, inclusive[i][0], upper, inclusive[i][1], values, counts);
            Assert.assertTrue(topNResult == groundTruth,
                String.format("%s %s x %s %s, truth: %s, topN %s", lower, inclusive[i][0] ? "<=" : "<",
                    inclusive[i][1] ? "<=" : "<", upper,
                    groundTruth, topNResult));
        }
    }

    private long rangeCount(DataType dataType, Object lower, boolean lowerInclusive, Object upper,
                            boolean upperInclusive, Object[] values, int[] counts) {
        if (lower == null && upper == null) {
            return 0;
        }

        long cnt = 0;
        for (int i = 0; i < values.length; i++) {
            Object o = values[i];
            if (o == null) {
                continue;
            }
            int l = 1;
            if (lower != null) {
                l = dataType.compare(o, lower);
            }
            int u = -1;
            if (upper != null) {
                u = dataType.compare(o, upper);
            }

            boolean lowE = (l == 0);
            boolean upperE = (u == 0);
            boolean lowS = (l > 0);
            boolean upperS = (u < 0);
            if (lower == null) {
                if ((upperE && upperInclusive) || upperS) {
                    cnt += counts[i];
                }
                continue;
            }
            if (upper == null) {
                if ((lowE && lowerInclusive) || lowS) {
                    cnt += counts[i];
                }
                continue;
            }
            // both are not null
            // <= x <=
            if (lowerInclusive && upperInclusive) {
                if ((lowE || lowS) && (upperE || upperS)) {
                    cnt += counts[i];
                }
            }
            // <= x <
            if (lowerInclusive && !upperInclusive) {
                if ((lowE || lowS) && (upperS)) {
                    cnt += counts[i];
                }
            }
            // < x <=
            if (!lowerInclusive && upperInclusive) {
                if ((lowS) && (upperE || upperS)) {
                    cnt += counts[i];
                }
            }
            // < x <
            if (!lowerInclusive && !upperInclusive) {
                if ((lowS) && (upperS)) {
                    cnt += counts[i];
                }
            }

        }
        return cnt;
    }

    @Test
    public void testTopNnewBuild() {
        DataType dataType = DataTypes.IntegerType;

        TopN topN;
        Integer[] values = {2, 4, 6, 8};
        int[] counts;

        topN = new TopN(dataType, 1.0);
        counts = new int[] {2, 4, 6, 8};
        for (int i = 0; i < values.length; i++) {
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(3, 1, true);
        assert topN.getValueArr().length == 0;

        topN = new TopN(dataType, 1.0);
        counts = new int[] {1, 1, 11, 100};
        for (int i = 0; i < values.length; i++) {
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(3, 1, true);
        assert topN.getValueArr().length == 2;

        topN = new TopN(dataType, 1.0);
        counts = new int[] {1, 1, 11, 100};
        for (int i = 0; i < values.length; i++) {
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(1, 1, true);
        assert topN.getValueArr().length == 1;

        topN = new TopN(dataType, 1.0);
        counts = new int[] {1, 1, 11, 100};
        for (int i = 0; i < values.length; i++) {
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(3, 12, true);
        assert topN.getValueArr().length == 1;

        topN = new TopN(dataType, 1.0);
        counts = new int[] {1, 1, 11, 100};
        for (int i = 0; i < values.length; i++) {
            topN.offer(values[i], counts[i]);
        }
        topN.buildNew(4, 0, true);
        assert topN.getValueArr().length == 2;
    }
}
