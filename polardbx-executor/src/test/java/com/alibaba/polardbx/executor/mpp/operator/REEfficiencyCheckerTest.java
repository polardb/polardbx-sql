package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.operator.scan.RFEfficiencyChecker;
import com.alibaba.polardbx.executor.operator.scan.impl.RFEfficiencyCheckerImpl;
import org.junit.Assert;
import org.junit.Test;

public class REEfficiencyCheckerTest {

    private int sampleCount;
    private double filterRatioThreshold;

    @Test
    public void test() {
        sampleCount = 5;
        filterRatioThreshold = 0.25d;
        RFEfficiencyChecker efficiencyChecker = new RFEfficiencyCheckerImpl(sampleCount, filterRatioThreshold);

        FragmentRFItemKey itemKey1 = new FragmentRFItemKey("build_col_1", "probe_col_1", 0, 0);
        FragmentRFItemKey itemKey2 = new FragmentRFItemKey("build_col_2", "probe_col_2", 1, 1);
        FragmentRFItemKey itemKey3 = new FragmentRFItemKey("build_col_3", "probe_col_3", 2, 2);

        // 10 batches for sample.
        for (int i = 0; i < 10000 * 10; i++) {
            boolean checkResult1, checkResult2, checkResult3;
            if (checkResult1 = efficiencyChecker.check(itemKey1)) {
                efficiencyChecker.sample(itemKey1, 1000, 300);
            }

            if (checkResult2 = efficiencyChecker.check(itemKey2)) {
                efficiencyChecker.sample(itemKey2, 1000, 800);
            }

            if (checkResult3 = efficiencyChecker.check(itemKey3)) {
                efficiencyChecker.sample(itemKey3, 1000, 900);
            }

            // check efficiency of range (sampleCount + 1, batchSize]
            if (i % 10000 > sampleCount + 1) {
                Assert.assertTrue("i = " + i, checkResult1);
                Assert.assertFalse("i = " + i, checkResult2);
                Assert.assertFalse("i = " + i, checkResult3);
            }
        }
    }
}
