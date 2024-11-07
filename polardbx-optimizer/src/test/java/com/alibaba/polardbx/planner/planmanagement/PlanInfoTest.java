package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.JsonBuilder;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * @author fangwu
 */
public class PlanInfoTest {

    @Test
    public void testFixHintEncodeDecode() {
        final JsonBuilder jsonBuilder = new JsonBuilder();
        Map<String, Object> extendMap = Maps.newHashMap();
        String fixHint = "ENABLE_BKA_JOIN=FALSE";
        extendMap.put("FIX_HINT", fixHint);
        PlanInfo planInfo = new PlanInfo(1, "fake plan json", -1L, -1L,
            0, 0L, 0L, true, true, "fake trace id", "",
            jsonBuilder.toJsonString(extendMap), -1);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));

        String json = PlanInfo.serializeToJson(planInfo);
        PlanInfo planInfo1 = PlanInfo.deserializeFromJson(json);

        Assert.assertTrue(planInfo1.getFixHint().equals(fixHint));
    }

    @Test
    public void testCanChooseColumnarPlan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            RelNode node = Mockito.mock(RelNode.class);
            ExecutionContext ec = new ExecutionContext("hello");
            ec.setAutoCommit(false);
            PlanInfo planInfo = Mockito.mock(PlanInfo.class);
            Mockito.when(planInfo.isFixed()).thenReturn(true);

            ParamManager pm = Mockito.mock(ParamManager.class);
            ec.setParamManager(pm);
            Mockito.when(pm.getBoolean(any())).thenReturn(false);

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        Mockito.when(pc.isUseColumnar()).thenReturn(false);
                        return pc;
                    });
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        Mockito.when(pc.isUseColumnar()).thenReturn(true);
                        return pc;
                    });
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

            ec.setAutoCommit(true);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            Mockito.when(planInfo.isFixed()).thenReturn(false);
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

            Mockito.when(pm.getBoolean(any())).thenReturn(true);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));
        }
    }
}
