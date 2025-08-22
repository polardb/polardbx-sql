package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class AggregateUtilsTest {
    private static final int MAX_HASH_TABLE_INITIAL_SIZE = 1024;
    private ExecutionContext context;

    @Before
    public void setUp() throws Exception {
        HashMap map = new HashMap();
        map.put(ConnectionParams.AGG_MAX_HASH_TABLE_INITIAL_SIZE.getName(), MAX_HASH_TABLE_INITIAL_SIZE);

        context = new ExecutionContext();
        context.setParamManager(new ParamManager(map));
    }

    @Test
    public void test() {
        final int expectedOutputRowCount = 1 << 20;
        int estimateHashTableSize;

        context.setExecuteMode(ExecutorMode.TP_LOCAL);
        estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
        assertEquals(MAX_HASH_TABLE_INITIAL_SIZE, estimateHashTableSize);

        context.setExecuteMode(ExecutorMode.AP_LOCAL);
        estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
        assertEquals(MAX_HASH_TABLE_INITIAL_SIZE, estimateHashTableSize);

        context.setExecuteMode(ExecutorMode.CURSOR);
        estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
        assertEquals(MAX_HASH_TABLE_INITIAL_SIZE, estimateHashTableSize);

        context.setExecuteMode(ExecutorMode.NONE);
        estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
        assertEquals(MAX_HASH_TABLE_INITIAL_SIZE, estimateHashTableSize);

        context.setExecuteMode(ExecutorMode.MPP);
        estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
        assertEquals(131064, estimateHashTableSize);
    }

}