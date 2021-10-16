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

package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.util.bloomfilter.BloomFilterInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueryBloomFilter {
    private static final Logger logger = LoggerFactory.getLogger(QueryBloomFilter.class);
    private final Map<Integer, BloomFilterNode> filterIdToProduceFilters = new HashMap<>();

    private final Map<Integer, Map<Integer, BloomFilterNode>> stageIdToProduceFilters = new HashMap<>();

    public QueryBloomFilter(Collection<PlanFragment> planFragments) {
        for (PlanFragment planFragment : planFragments) {
            Map<Integer, BloomFilterNode> filterIdToProduceFilters = new HashMap<>();
            for (Integer filterId : planFragment.getProduceFilterIds()) {
                if (!filterIdToProduceFilters.containsKey(filterId)) {
                    BloomFilterNode bloomFilterNode = new BloomFilterNode(filterId,
                        planFragment.getPartitioning().getPartitionCount());
                    filterIdToProduceFilters.put(filterId, bloomFilterNode);
                }
            }
            if (filterIdToProduceFilters.size() > 0) {
                this.stageIdToProduceFilters.put(planFragment.getId(), filterIdToProduceFilters);
                this.filterIdToProduceFilters.putAll(filterIdToProduceFilters);
            }
        }
    }

    public void finishStage(Integer stageId) {
        Map<Integer, BloomFilterNode> bloomFilters = stageIdToProduceFilters.get(stageId);
        if (bloomFilters != null) {
            for (BloomFilterNode filterNode : bloomFilters.values()) {
                filterNode.finish();
            }
        }
    }

    public void updateTaskParallelism(Integer stageId, int parallelism) {
        Map<Integer, BloomFilterNode> bloomFilters = stageIdToProduceFilters.get(stageId);
        if (bloomFilters != null) {
            for (BloomFilterNode filterNode : bloomFilters.values()) {
                filterNode.updateParallelism(parallelism);
            }
        }
    }

    public void mergeBuildBloomFilter(List<BloomFilterInfo> bloomFilterInfos) {
        for (BloomFilterInfo info : bloomFilterInfos) {
            filterIdToProduceFilters.get(info.getId()).mergeBloomFilter(info);
        }
    }

    public SettableFuture<BloomFilterInfo> getFuture(Integer filterId) {
        return filterIdToProduceFilters.get(filterId).getFuture();
    }

    public void close() {
        stageIdToProduceFilters.clear();
        filterIdToProduceFilters.clear();
    }

    protected static class BloomFilterNode {
        private SettableFuture<BloomFilterInfo> future = SettableFuture.create();
        private BloomFilterInfo mergeInfo;
        private int id;

        private int mergeCount;

        private int bloomFilterNum;

        public BloomFilterNode(int id, int bloomFilterNum) {
            this.id = id;
            this.mergeInfo = null;
            this.bloomFilterNum = bloomFilterNum;
        }

        public void updateParallelism(int bloomFilterNum) {
            this.bloomFilterNum = bloomFilterNum;
        }

        private void flushFuture() {
            if (mergeCount >= bloomFilterNum) {
                finish();
            }
        }

        public synchronized void finish() {
            logger.info("bloom filter finished: " + id
                + ", merge count: " + mergeCount
                + ", first value: " + String.format("%x", mergeInfo.getData()[0]));
            if (!this.future.isDone()) {
                this.future.set(mergeInfo);
            }
        }

        public SettableFuture<BloomFilterInfo> getFuture() {
            return future;
        }

        public synchronized void mergeBloomFilter(BloomFilterInfo other) {
            this.mergeCount++;
            if (this.mergeInfo == null) {
                this.mergeInfo = other;
            } else {
                this.mergeInfo.mergeBloomFilter(other);
            }

            logger.info(String.format("Merge bloom filter id: %d, merge count: %d", other.getId(), mergeCount));
            flushFuture();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BloomFilterNode filterNode = (BloomFilterNode) o;
            return id == filterNode.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
