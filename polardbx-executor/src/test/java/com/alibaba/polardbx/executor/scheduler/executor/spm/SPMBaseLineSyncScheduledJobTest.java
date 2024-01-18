package com.alibaba.polardbx.executor.scheduler.executor.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;

/**
 * ut for com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob
 *
 * @author fangwu
 */
public class SPMBaseLineSyncScheduledJobTest {
    static AtomicInteger planId = new AtomicInteger();

    /**
     * test for method
     * com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob#cleanEmptyBaseline(java.util.Map)
     */
    @Test
    public void testCleanEmptyBaseline() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        // mock baseline map
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();

        String sql1 = "test sql1";
        String sql2 = "test sql2";
        String sql3 = "test sql3";

        baselineInfoMap.put(sql1, buildBaselineInfoWithFixedPlan(sql1, Collections.EMPTY_SET));
        baselineInfoMap.put(sql2, buildBaselineInfoWithoutFixedPlan(sql2, Collections.EMPTY_SET));
        baselineInfoMap.put(sql3, buildBaselineInfoWithEmptyAcceptedPlan(sql3, Collections.EMPTY_SET));

        SPMBaseLineSyncScheduledJob.cleanEmptyBaseline(baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.size() == 2);
        Assert.assertTrue(
            baselineInfoMap.containsKey(sql1) &&
                baselineInfoMap.containsKey(sql2) &&
                !baselineInfoMap.containsKey(sql3));
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
}
