package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;

/**
 * Introduce a dynamic sampling inspection mechanism
 * on the Scan side to check the filter ratio of the first 5 to 10 chunks in each batch;
 * when the filter ratio of the first 5 to 10 chunks is very poor,
 * terminate The filtering calculation of the runtime filter under this batch.
 * Of course, the runtime filter has been built at this time,
 * and the performance loss in this part cannot be avoided.
 */
public interface RFEfficiencyChecker {
    /**
     * Check the filter ratio of the first 5 to 10 chunks in each batch and if the filter ratio of the first 5 to
     * 10 chunks is very poor, terminate The filtering calculation of the runtime filter under this batch.
     *
     * @param rfItemKey item key of runtime filter.
     * @return TRUE if valid.
     */
    boolean check(FragmentRFItemKey rfItemKey);

    /**
     * Sampling and calculating the first 5~10 chunks of each batch to obtain the filter ratio。
     *
     * @param rfItemKey item key of runtime filter.
     * @param originalCount the row count before filtering.
     * @param selectedCount the row count after filtering.
     */
    void sample(FragmentRFItemKey rfItemKey, int originalCount, int selectedCount);
}
