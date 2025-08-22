package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class IndexPrunerTest {
    Map<Integer, ParameterContext> params;
    Parameters parameters;

    @Before
    public void prepare() {
        params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {
            1, new RawString(
            Lists.newArrayList("a", "b"))}));

        parameters = new Parameters(params);
    }

    @Test
    public void testIndexPrunerContextParams() {
        IndexPruneContext ipc = new IndexPruneContext();
        ipc.setParameters(parameters);
        Assert.assertTrue(ipc.acquireFromParameter(0, null, null) instanceof RawString);
        Assert.assertTrue(ipc.acquireArrayFromParameter(0, null, null)[0].equals("a")
            && ipc.acquireArrayFromParameter(0, null, null)[1].equals("b"));
    }

}