package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.core.rel.GatherReferencedGsiNameRelVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ExecutionPlanTest {
    @Test
    public void testOptimizeCopyPlan() {
        RelNode plan = Mockito.mock(TableScan.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, plan, null);

        Mockito.when(plan.accept(Mockito.any(GatherReferencedGsiNameRelVisitor.class)))
            .thenThrow(new TddlNestableRuntimeException());

        executionPlan.copy(plan);
    }

}
