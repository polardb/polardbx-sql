package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.util.Util;
import org.junit.Test;
import org.mockito.Mockito;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * @author fangwu
 */
public class RelUtilsTest {

    /**
     * test no agg found
     */
    @Test
    public void testContainsAggWithoutAgg() {
        RelNode nonAggNode = Mockito.mock(RelNode.class);

        assertFalse(RelOptUtil.containsAgg(nonAggNode));
    }

    /**
     * test agg node found
     */
    @Test
    public void testContainsAggWithAgg() throws Util.FoundOne {
        Aggregate aggNode = Mockito.mock(Aggregate.class);

        assertTrue(RelOptUtil.containsAgg(aggNode));
    }

}
