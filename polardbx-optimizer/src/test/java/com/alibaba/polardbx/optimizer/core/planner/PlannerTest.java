package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author fangwu
 */
public class PlannerTest {

    @Test
    public void testEnableDirectPlanFalse() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.ENABLE_DIRECT_PLAN, "false");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    @Test
    public void testDnHint() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.DN_HINT, "test hint");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    @Test
    public void testForceIndexHint() {
        PlannerContext pc = new PlannerContext();
        pc.setLocalIndexHint(true);
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }
}
