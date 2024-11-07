/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fangwu
 */
public class ColumnarTracer {

    /**
     * note: we need to specify the map type here, because Map<String, ColumnarPruneRecord> pruneRecordMap =
     * Maps.newConcurrentMap(); will be incorrectly parsed to LinkedHashMap in jackson
     */
    private ConcurrentHashMap<String, ColumnarPruneRecord> pruneRecordMap = new ConcurrentHashMap<>();
    private String instanceId = "default";

    //only for test
    public ColumnarTracer() {
    }

    public ColumnarTracer(String instanceId) {
        this.instanceId = instanceId;
    }

    @JsonCreator
    public ColumnarTracer(
        @JsonProperty("pruneRecordMap") ConcurrentHashMap<String, ColumnarPruneRecord> pruneRecordMap,
        @JsonProperty("instanceId") String instanceId) {
        this.instanceId = instanceId;
        this.pruneRecordMap = pruneRecordMap;
    }

    public void tracePruneInit(String table, String filter, long initTime) {
        pruneRecordMap.compute(getTracerKey(table, filter),
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.initIndexTime.addAndGet(initTime);

                return columnarPruneRecord;
            });
    }

    public void tracePruneTime(String table, String filter, long pruneTime) {
        pruneRecordMap.compute(getTracerKey(table, filter),
            (s, columnarPruneRecord) -> {
                if (columnarPruneRecord == null) {
                    columnarPruneRecord = new ColumnarPruneRecord(table, filter);
                }
                columnarPruneRecord.indexPruneTime.addAndGet(pruneTime);

                return columnarPruneRecord;
            });
    }

    public void tracePruneResult(String table, String filter, int fileNum, int stripeNum, int rgNum, int rgLeftNum) {
        pruneRecordMap.compute(getTracerKey(table, filter),
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
        pruneRecordMap.compute(getTracerKey(table, filter),
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

    //merge slave instance ColumnarTracer to master instance
    public void mergeColumnarTracer(ColumnarTracer right) {
        if (right == null || right.getPruneRecordMap() == null) {
            return;
        }

        right.getPruneRecordMap().forEach((key, rightRecord) -> {
            pruneRecordMap.merge(key, rightRecord, (leftRecord, newRecord) -> {
                if (leftRecord == null) {
                    return newRecord;
                }

                leftRecord.initIndexTime.addAndGet(newRecord.initIndexTime.get());
                leftRecord.indexPruneTime.addAndGet(newRecord.indexPruneTime.get());
                leftRecord.fileNum.addAndGet(newRecord.fileNum.get());
                leftRecord.stripeNum.addAndGet(newRecord.stripeNum.get());
                leftRecord.rgNum.addAndGet(newRecord.rgNum.get());
                leftRecord.rgLeftNum.addAndGet(newRecord.rgLeftNum.get());
                leftRecord.sortKeyPruneNum.addAndGet(newRecord.sortKeyPruneNum.get());
                leftRecord.zoneMapPruneNum.addAndGet(newRecord.zoneMapPruneNum.get());
                leftRecord.bitMapPruneNum.addAndGet(newRecord.bitMapPruneNum.get());

                return leftRecord;
            });
        });
    }

    public String getTracerKey(String tableName, String filter) {
        return instanceId + "_" + tableName + "_" + filter;
    }

    public Collection<ColumnarPruneRecord> pruneRecords() {
        return this.pruneRecordMap.values();
    }

    @JsonProperty
    public ConcurrentHashMap<String, ColumnarPruneRecord> getPruneRecordMap() {
        return pruneRecordMap;
    }

    @JsonProperty
    public void setPruneRecordMap(
        ConcurrentHashMap<String, ColumnarPruneRecord> pruneRecordMap) {
        this.pruneRecordMap = pruneRecordMap;
    }

    @JsonProperty
    public String getInstanceId() {
        return instanceId;
    }

    @JsonProperty
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }
}
