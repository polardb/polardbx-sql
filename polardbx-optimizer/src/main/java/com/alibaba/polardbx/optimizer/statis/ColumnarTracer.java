package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

/**
 * @author fangwu
 */
public class ColumnarTracer {

    private Map<String, ColumnarPruneRecord> pruneRecordMap = Maps.newConcurrentMap();

    public ColumnarTracer() {

    }

    @JsonCreator
    public ColumnarTracer(@JsonProperty("pruneRecordMap") Map<String, ColumnarPruneRecord> pruneRecordMap) {
        this.pruneRecordMap = pruneRecordMap;
    }

    public void tracePruneInit(String table, String filter, long initTime) {
        pruneRecordMap.compute(table + "_" + filter,
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.initIndexTime.addAndGet(initTime);

                return columnarPruneRecord;
            });
    }

    public void tracePruneTime(String table, String filter, long pruneTime) {
        pruneRecordMap.compute(table + "_" + filter,
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.indexPruneTime.addAndGet(pruneTime);

                return columnarPruneRecord;
            });
    }

    public void tracePruneResult(String table, String filter, int fileNum, int stripeNum, int rgNum, int rgLeftNum) {
        pruneRecordMap.compute(table + "_" + filter,
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.fileNum.addAndGet(fileNum);
                columnarPruneRecord.stripeNum.addAndGet(stripeNum);
                columnarPruneRecord.rgNum.addAndGet(rgNum);
                columnarPruneRecord.rgLeftNum.addAndGet(rgLeftNum);
                return columnarPruneRecord;
            });
    }

    public void tracePruneIndex(String table, String filter, int sortKeyPruneNum, int zoneMapPruneNum,
                                int bitMapPruneNum) {
        pruneRecordMap.compute(table + "_" + filter,
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.sortKeyPruneNum.addAndGet(sortKeyPruneNum);
                columnarPruneRecord.zoneMapPruneNum.addAndGet(zoneMapPruneNum);
                columnarPruneRecord.bitMapPruneNum.addAndGet(bitMapPruneNum);
                return columnarPruneRecord;
            });
    }

    public Collection<ColumnarPruneRecord> pruneRecords() {
        return this.pruneRecordMap.values();
    }

    @JsonProperty
    public Map<String, ColumnarPruneRecord> getPruneRecordMap() {
        return pruneRecordMap;
    }

    @JsonProperty
    public void setPruneRecordMap(
        Map<String, ColumnarPruneRecord> pruneRecordMap) {
        this.pruneRecordMap = pruneRecordMap;
    }
}
