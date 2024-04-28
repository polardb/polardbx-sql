package com.alibaba.polardbx.executor.mpp.planner;

import java.util.Map;

/**
 * PlanFragment-level runtime filter manager
 */
public interface FragmentRFManager {

    enum RFType {
        LOCAL, BROADCAST
    }

    Map<FragmentRFItemKey, FragmentRFItem> getAllItems();

    void addItem(FragmentRFItemKey itemKey, FragmentRFItem rfItem);

    double getDefaultFpp();

    int getTotalPartitionCount();

    int getPartitionsOfNode();

    long getUpperBound();

    long getLowerBound();

    int getSampleCount();

    double getFilterRatioThreshold();
}
