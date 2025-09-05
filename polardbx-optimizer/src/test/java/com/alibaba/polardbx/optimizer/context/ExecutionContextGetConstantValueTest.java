package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.junit.Assert;
import org.junit.Test;

public class ExecutionContextGetConstantValueTest {
    @Test
    public void test() {
        ExecutionContext context = new ExecutionContext();

        MysqlDateTime t = context.getConstantValue("now", () -> DataTypeUtil.getNow(6, new ExecutionContext()));

        MysqlDateTime t1 = context.getConstantValue("now", () -> DataTypeUtil.getNow(6, new ExecutionContext()));
        Assert.assertTrue(t == t1);

        t1 = context.getConstantValue("now", () -> DataTypeUtil.getNow(6, new ExecutionContext()));
        Assert.assertTrue(t == t1);

        t1 = context.getConstantValue("now", () -> DataTypeUtil.getNow(6, new ExecutionContext()));
        Assert.assertTrue(t == t1);
    }

}