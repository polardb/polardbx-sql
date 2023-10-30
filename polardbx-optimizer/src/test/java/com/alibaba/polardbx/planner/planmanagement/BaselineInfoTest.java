package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
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

    public void testEmptyBaselineClear() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());

        // test clear unfixed plan to empty
        b1.addAcceptedPlan(buildPlan());
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);
        b1.clearAllUnfixedPlan();
        Assert.assertTrue(b1.getAcceptedPlans().size() == 0);

        // test clear unfixed plan with fixed plan left in accepted plans
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildPlan());
        Assert.assertTrue(b1.getAcceptedPlans().size() == 2);
        b1.clearAllUnfixedPlan();
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);

        // test clear unaccepted plans
        b1.addUnacceptedPlan(buildPlan());
        b1.addUnacceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildFixPlan());

        Assert.assertTrue(b1.getAcceptedPlans().size() == 3);
        Assert.assertTrue(b1.getUnacceptedPlans().size() == 2);

        b1.clearAllUnfixedPlan();
        Assert.assertTrue(b1.getUnacceptedPlans().size() == 0);
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);
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

    @Test
    public void testTableSetSerialized() {
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(null, "xxa"));

        String json = BaselineInfo.serializeTableSet(tableSet);
        Set<Pair<String, String>> deserializedTableSet = BaselineInfo.deserializeTableSet(json);
        System.out.println(deserializedTableSet);
        Assert.assertTrue(deserializedTableSet.iterator().next().getKey() == null);
    }

    /**
     * test RebuildAtLoad baseline serialize
     */
    @Test
    public void testRebuildAtLoadBaselineHintSerialized() {
        String hint = "test hint info";
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        b1.setHint(hint);
        b1.setRebuildAtLoad(true);
        b1.setUsePostPlanner(true);
        Assert.assertTrue(b1.isRebuildAtLoad());
        String json = BaselineInfo.serializeToJson(b1, false);
        BaselineInfo b2 = BaselineInfo.deserializeFromJson(json);
        Assert.assertTrue(b2.isRebuildAtLoad() && StringUtils.isNotEmpty(b2.getHint()) && b2.isUsePostPlanner());
    }

    private static PlanInfo buildFixPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, true, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, false, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildExpiredPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000,
                (System.currentTimeMillis() - InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD) - 1000) / 1000, 0, 1D,
                1D,
                true, false, "", "", "", 1);
        p.setId(planId.incrementAndGet());
        return p;
    }

    /**
     * build baseline with specified sql
     * baseline returned contains fixed/unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildFixPlan());
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned contains unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithoutFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned had empty accepted plan list
     */
    public static BaselineInfo buildBaselineInfoWithEmptyAcceptedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }
}
