package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.trx.ISyncPointExecutor;
import org.junit.Assert;
import org.junit.Test;

public class ExecutorContextTest {
    @Test
    public void syncPointExecutorTest() {
        ExecutorContext context = new ExecutorContext();
        ISyncPointExecutor syncPointExecutor = new MockSyncPointExecutor();
        context.setSyncPointExecutor(syncPointExecutor);
        Assert.assertEquals(syncPointExecutor, context.getSyncPointExecutor());
    }

    private static class MockSyncPointExecutor implements ISyncPointExecutor {

        @Override
        public boolean execute() {
            return false;
        }
    }
}
