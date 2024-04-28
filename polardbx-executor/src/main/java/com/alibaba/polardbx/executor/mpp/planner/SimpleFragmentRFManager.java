package com.alibaba.polardbx.executor.mpp.planner;

import java.util.HashMap;
import java.util.Map;

public class SimpleFragmentRFManager implements FragmentRFManager {
    /**
     * The total partition number of the table for join key.
     * It's for route calculation.
     */
    private final int totalPartitionCount;

    /**
     * The number of allocated partition on this worker.
     */
    private final int partitionsOfNode;

    private final double defaultFpp;
    private final long rowUpperBound;
    private final long rowLowerBound;

    private final double filterRatioThreshold;
    private final int rfSampleCount;

    private final Map<FragmentRFItemKey, FragmentRFItem> items;

    public SimpleFragmentRFManager(int totalPartitionCount, int partitionsOfNode,
                                   double defaultFpp,
                                   long rowUpperBound, long rowLowerBound, double filterRatioThreshold,
                                   int rfSampleCount) {
        this.totalPartitionCount = totalPartitionCount;
        this.partitionsOfNode = partitionsOfNode;

        this.defaultFpp = defaultFpp;
        this.rowUpperBound = rowUpperBound;
        this.rowLowerBound = rowLowerBound;
        this.filterRatioThreshold = filterRatioThreshold;
        this.rfSampleCount = rfSampleCount;

        this.items = new HashMap<>();
    }

    @Override
    public Map<FragmentRFItemKey, FragmentRFItem> getAllItems() {
        return items;
    }

    @Override
    public void addItem(FragmentRFItemKey itemKey, FragmentRFItem rfItem) {
        items.put(itemKey, rfItem);
    }

    @Override
    public double getDefaultFpp() {
        return defaultFpp;
    }

    @Override
    public int getTotalPartitionCount() {
        return totalPartitionCount;
    }

    @Override
    public int getPartitionsOfNode() {
        return partitionsOfNode;
    }

    @Override
    public long getUpperBound() {
        return rowUpperBound;
    }

    @Override
    public long getLowerBound() {
        return rowLowerBound;
    }

    @Override
    public int getSampleCount() {
        return rfSampleCount;
    }

    @Override
    public double getFilterRatioThreshold() {
        return filterRatioThreshold;
    }

    @Override
    public String toString() {
        return "SimpleFragmentRFManager{" +
            "totalPartitionCount=" + totalPartitionCount +
            ", defaultFpp=" + defaultFpp +
            ", rowThreshold=" + rowUpperBound +
            ", items=" + items +
            '}';
    }
}
