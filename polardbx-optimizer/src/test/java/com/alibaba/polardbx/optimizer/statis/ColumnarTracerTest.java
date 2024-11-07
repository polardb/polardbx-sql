package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ColumnarTracerTest {
    private static String DEFAULT_TABLE = "DEFAULT_TABLE";

    @Test
    public void testColumnarTracerMergeNoOverlap() {
        //both left and right ColumnarTracer are available
        String filter1 = "varchar_test_2 EQUALS feed32feed";
        //only left
        String filter2 = "varchar_test_2 IN (Raw('nihaore'))";
        //only right
        String filter3 = "(integer_test_1 EQUALS 2)";
        ColumnarTracer left = new ColumnarTracer("default");
        ColumnarTracer right = new ColumnarTracer("default");
        left.tracePruneResult(DEFAULT_TABLE, filter1, 1, 1, 1, 1);

        left.tracePruneResult(DEFAULT_TABLE, filter2, 1, 1, 1, 1);

        right.tracePruneResult(DEFAULT_TABLE, filter3, 1, 1, 1, 1);

        left.mergeColumnarTracer(right);
        Assert.assertEquals(3, left.getPruneRecordMap().size());
        Assert.assertEquals(1,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter1)).getFileNum().get());
        Assert.assertEquals(1,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter2)).getFileNum().get());
        Assert.assertEquals(1,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter3)).getFileNum().get());
    }

    @Test
    public void testColumnarTracerMergeOverlap() {
        //both left and right ColumnarTracer are available
        String filter1 = "varchar_test_2 EQUALS feed32feed";
        //only left
        String filter2 = "varchar_test_2 IN (Raw('nihaore'))";
        //only right
        String filter3 = "(integer_test_1 EQUALS 2)";
        ColumnarTracer left = new ColumnarTracer("default");
        ColumnarTracer right = new ColumnarTracer("default");
        left.tracePruneResult(DEFAULT_TABLE, filter1, 1, 1, 1, 1);
        right.tracePruneResult(DEFAULT_TABLE, filter1, 1, 1, 1, 1);

        left.tracePruneResult(DEFAULT_TABLE, filter2, 1, 1, 1, 1);

        right.tracePruneResult(DEFAULT_TABLE, filter3, 1, 1, 1, 1);

        left.mergeColumnarTracer(right);
        Assert.assertEquals(3, left.getPruneRecordMap().size());
        Assert.assertEquals(2,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter1)).getFileNum().get());
        Assert.assertEquals(1,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter2)).getFileNum().get());
        Assert.assertEquals(1,
            left.getPruneRecordMap().get(left.getTracerKey(DEFAULT_TABLE, filter3)).getFileNum().get());
    }

    @Test
    public void serializeColumnarTracerTest() throws JsonProcessingException {
        ColumnarTracer tracer = new ColumnarTracer("default");
        tracer.tracePruneInit(DEFAULT_TABLE, "filter1", 100L);
        tracer.tracePruneTime(DEFAULT_TABLE, "filter1", 200L);
        tracer.tracePruneResult(DEFAULT_TABLE, "filter1", 1, 2, 3, 4);
        tracer.tracePruneIndex(DEFAULT_TABLE, "filter1", 5, 6, 7);
        ObjectMapper objectMapper = new ObjectMapper();
        // 序列化ColumnarTracer
        String json = objectMapper.writeValueAsString(tracer);

        // 反序列化ColumnarTracer
        ColumnarTracer deserializedTracer = objectMapper.readValue(json, ColumnarTracer.class);

        // 验证序列化和反序列化后的对象是否一致
        Assert.assertEquals(tracer.getPruneRecordMap(), deserializedTracer.getPruneRecordMap());
    }

    @Test
    public void columnarPruneRecordSimpleTest() {
        //both left and right ColumnarTracer are available
        String filter1 = "varchar_test_2 EQUALS feed32feed";
        ColumnarPruneRecord left = new ColumnarPruneRecord(DEFAULT_TABLE, filter1);
        Assert.assertTrue(left.equals(left));
        left.setFilter(filter1);
        left.setTableName(DEFAULT_TABLE);
        left.setInitIndexTime(new AtomicLong(1));
        left.setIndexPruneTime(new AtomicLong(1));
        left.setFileNum(new AtomicInteger(1));
        left.setStripeNum(new AtomicInteger(1));
        left.setRgNum(new AtomicInteger(1));
        left.setRgLeftNum(new AtomicInteger(1));
        left.setSortKeyPruneNum(new AtomicInteger(1));
        left.setZoneMapPruneNum(new AtomicInteger(1));
        left.setBitMapPruneNum(new AtomicInteger(1));

        ColumnarPruneRecord right = new ColumnarPruneRecord(DEFAULT_TABLE, filter1);

        right.setFilter(filter1);
        right.setTableName(DEFAULT_TABLE);
        right.setInitIndexTime(new AtomicLong(1));
        right.setIndexPruneTime(new AtomicLong(1));
        right.setFileNum(new AtomicInteger(1));
        right.setStripeNum(new AtomicInteger(1));
        right.setRgNum(new AtomicInteger(1));
        right.setRgLeftNum(new AtomicInteger(1));
        right.setSortKeyPruneNum(new AtomicInteger(1));
        right.setZoneMapPruneNum(new AtomicInteger(1));
        right.setBitMapPruneNum(new AtomicInteger(1));

        Assert.assertTrue(left.equals(right));
    }

}
