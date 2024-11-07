package com.alibaba.polardbx.executor.ddl.newengine;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

public class DdlEngineSchedulerTest {

    @Test
    public void testCompareAndExecute() {
        try {
            DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();
            ddlEngineScheduler.compareAndExecute(0, () -> null);
            Assert.fail("should trigger timeout exception, but not.");
        } catch (TimeoutException ignored) {
        }
    }
}
