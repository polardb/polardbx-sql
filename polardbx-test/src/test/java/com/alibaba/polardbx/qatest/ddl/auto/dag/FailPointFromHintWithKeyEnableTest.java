package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FailPointFromHintWithKeyEnableTest {

    private ExecutionContext executionContext;

    @Before
    public void before() {
        this.executionContext = new ExecutionContext();
        Map<String, Object> hint = new HashMap<>();
        executionContext.setExtraCmds(hint);
    }

    private void addHint(String key, String value) {
        this.executionContext.getExtraCmds().put(key, value);
    }

    private void clearHint() {
        this.executionContext.getExtraCmds().clear();
    }

    @Test
    public void testInjectExceptionFromHintWithKeyEnableCheck() {
        String key = "key1";
        FailPoint.disable(key);
        FailPoint.injectExceptionFromHintWithKeyEnableCheck(key, executionContext);
        addHint(key, "123");
        FailPoint.injectExceptionFromHintWithKeyEnableCheck(key, executionContext);
    }

    @Test(expected = RuntimeException.class)
    public void testInjectExceptionFromHintWithKeyEnableCheck2() {
        String key = "key1";
        addHint(key, "123");
        FailPoint.enable(key, "true");
        FailPoint.injectExceptionFromHintWithKeyEnableCheck(key, executionContext);
    }

    @Test
    public void testInjectExceptionWithTableName() {
        String key = "key2";
        FailPoint.injectExceptionWithTableName("t1", key, executionContext);
        addHint(key, "t2");
        FailPoint.injectExceptionWithTableName("t1", key, executionContext);
        clearHint();
    }

    @Test(expected = RuntimeException.class)
    public void testInjectExceptionWithTableName2() {
        String key = "key2";
        FailPoint.injectExceptionWithTableName("t1", key, executionContext);
        addHint(key, "t1");
        FailPoint.injectExceptionWithTableName("t1", key, executionContext);
        clearHint();
    }
}
