package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.operator.scan.RFEfficiencyChecker;

import java.util.HashMap;
import java.util.Map;

public class RFEfficiencyCheckerImpl implements RFEfficiencyChecker {
    private final static int SAMPLE_RANGE = 10000;
    private final int sampleCount;
    private final double filterRatioThreshold;

    private final Map<FragmentRFItemKey, RFItemRecord> itemRecordMap;

    public RFEfficiencyCheckerImpl(int sampleCount, double filterRatioThreshold) {
        this.sampleCount = sampleCount;
        this.filterRatioThreshold = filterRatioThreshold;
        this.itemRecordMap = new HashMap<>();
    }

    @Override
    public void sample(FragmentRFItemKey rfItemKey, int originalCount, int selectedCount) {
        RFItemRecord itemRecord;
        if ((itemRecord = itemRecordMap.get(rfItemKey)) == null) {
            return;
        }

        // record and calculate filter ratio when record count is less than sample count.
        if (itemRecord.recordCount <= sampleCount) {
            itemRecord.totalOriginalCount += originalCount;
            itemRecord.totalSelectedCount += selectedCount;
        } else if (itemRecord.filterRatio >= 1.0d) {
            itemRecord.filterRatio = itemRecord.totalOriginalCount == 0 ? 1.0d
                : 1.0d - (itemRecord.totalSelectedCount / (itemRecord.totalOriginalCount * 1.0d));
        }
    }

    @Override
    public boolean check(FragmentRFItemKey rfItemKey) {
        RFItemRecord itemRecord;

        // initialize item record.
        if ((itemRecord = itemRecordMap.get(rfItemKey)) == null) {
            itemRecord = new RFItemRecord();
            itemRecordMap.put(rfItemKey, itemRecord);
            return true;
        }

        // set record count of this rf item.
        itemRecord.recordCount++;
        if (itemRecord.recordCount > SAMPLE_RANGE) {

            // reset
            itemRecord.recordCount -= SAMPLE_RANGE;
            itemRecord.totalOriginalCount = 0;
            itemRecord.totalSelectedCount = 0;
            itemRecord.filterRatio = 1d;
        }

        // Allow runtime filter directly.
        if (itemRecord.recordCount <= sampleCount) {
            return true;
        }

        // Check filter ratio.
        return itemRecord.filterRatio >= filterRatioThreshold;
    }

    private static class RFItemRecord {
        long recordCount = 0;
        long totalOriginalCount = 0;
        long totalSelectedCount = 0;
        double filterRatio = 1.0d;
    }
}
