package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;

/**
 * @author fangwu
 */
public class BaselineInfoTest {
    static AtomicInteger planId = new AtomicInteger();

    @Before
    public void prepare() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testMergeFixPlanExceedMaxPlanSize() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.merge("test", b1);
        System.out.println(b2.getAcceptedPlans().size());
        Assert.assertTrue(b2.getFixPlans().size() == 12 && b2.getAcceptedPlans().size() == 12);
    }

    @Test
    public void testMergeExpiredPlan() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.merge("test", b1);
        System.out.println(b2.getAcceptedPlans().size());
        Assert.assertTrue(b2.getFixPlans().size() == 2 && b2.getAcceptedPlans().size() == 6);
    }

    private PlanInfo buildFixPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, true, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private PlanInfo buildPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, false, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private PlanInfo buildExpiredPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000,
                (System.currentTimeMillis() - InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD) - 1000) / 1000, 0, 1D,
                1D,
                true, false, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }
}
