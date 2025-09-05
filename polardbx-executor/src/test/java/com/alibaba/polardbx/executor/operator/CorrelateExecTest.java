package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class CorrelateExecTest extends BaseExecTest {

    @Test(expected = NullPointerException.class)
    public void testCleanUpLeft() {
        ExecutionContext context = new ExecutionContext();
        CacheExec exec = new CacheExec(Lists.newArrayList(DataTypes.StringType), context);
        RelNode plan = mock(RelNode.class);
        DataType outColumnType = DataTypes.StringType;
        CorrelateExec correlateExec = new CorrelateExec(exec, plan, outColumnType,
            null, null, null, null, null, context);
        correlateExec.doClose();
        Assert.assertTrue(correlateExec.getInputs().get(0) == null);
    }

    @Test
    public void testCacheExec() {
        ExecutionContext context = new ExecutionContext();
        List<Chunk> chunks = new ArrayList<>();
        CacheExec exec = new CacheExec(Lists.newArrayList(DataTypes.StringType), chunks, context);
        exec.doClose();
        Assert.assertTrue(exec.getChunks() == chunks);

    }
}
