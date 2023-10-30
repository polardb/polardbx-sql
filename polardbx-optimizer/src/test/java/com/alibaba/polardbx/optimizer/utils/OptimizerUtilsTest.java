package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @author fangwu
 */
public class OptimizerUtilsTest {
    @Test
    public void testBuildInExprKey() {
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        List<Integer> args = Lists.newArrayList();
        args.add(1);
        args.add(2);
        args.add(3);
        ParameterContext pc1 = new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, new RawString(args.subList(0, 3))});
        ParameterContext pc2 = new ParameterContext(ParameterMethod.setObject1,
            new Object[] {2, new RawString(args.subList(0, 1))});

        params.put(1, pc1);
        params.put(2, pc2);

        String hashMapParams = OptimizerUtils.buildInExprKey(params);
        params = new Int2ObjectOpenHashMap<>();
        params.put(1, pc1);
        params.put(2, pc2);
        String int2ObjectMapParams = OptimizerUtils.buildInExprKey(params);
        System.out.println(hashMapParams);
        System.out.println(int2ObjectMapParams);
        Assert.assertTrue(hashMapParams.equals(int2ObjectMapParams));
    }
}
