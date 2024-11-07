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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.RFEfficiencyChecker;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

public class RFLazyEvaluator implements LazyEvaluator<Chunk, BitSet> {

    private final FragmentRFManager manager;
    private final OperatorStatistics operatorStatistics;
    private final RFEfficiencyChecker efficiencyChecker;
    private final Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap;

    private final int itemSize;
    private final FragmentRFItemKey[] itemKeys;
    private final FragmentRFItem[] items;

    public RFLazyEvaluator(FragmentRFManager manager, OperatorStatistics operatorStatistics,
                           Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap) {
        this.manager = manager;
        this.operatorStatistics = operatorStatistics;
        this.efficiencyChecker = new RFEfficiencyCheckerImpl(
            manager.getSampleCount(), manager.getFilterRatioThreshold());
        this.rfBloomFilterMap = rfBloomFilterMap;

        // Put all fragment items and their keys into array.
        Map<FragmentRFItemKey, FragmentRFItem> allItems = manager.getAllItems();
        this.itemSize = allItems.size();
        this.itemKeys = new FragmentRFItemKey[itemSize];
        this.items = new FragmentRFItem[itemSize];

        int index = 0;
        for (Map.Entry<FragmentRFItemKey, FragmentRFItem> entry : manager.getAllItems().entrySet()) {
            itemKeys[index] = entry.getKey();
            items[index] = entry.getValue();
            index++;
        }
    }

    @Override
    public VectorizedExpression getCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BitSet eval(Chunk chunk, int startPosition, int positionCount, RoaringBitmap deletion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int eval(Chunk chunk, int startPosition, int positionCount, RoaringBitmap deletion, boolean[] bitmap) {
        long cardinality = deletion.rangeCardinality(startPosition, startPosition + positionCount);
        Preconditions.checkArgument(cardinality <= positionCount);

        if (cardinality != 0) {
            // mark the position as TRUE that not deleted in the RoaringBitmap.
            for (int i = 0; i < positionCount; i++) {
                bitmap[i] = !deletion.contains(i + startPosition);
            }
        } else {
            // clear bitmap
            Arrays.fill(bitmap, 0, chunk.getPositionCount(), true);
        }
        // clear bitmap in the area that out of bound.
        if (chunk.getPositionCount() < bitmap.length) {
            Arrays.fill(bitmap, chunk.getPositionCount(), bitmap.length, false);
        }

        if (chunk == null || chunk.getPositionCount() == 0) {
            return 0;
        }

        final int totalPartitionCount = manager.getTotalPartitionCount();
        final int initialSelectedCount = chunk.getPositionCount() - (int) cardinality;
        int selectedCount = initialSelectedCount;
        for (int i = 0; i < itemSize; i++) {
            FragmentRFItem item = items[i];
            int filterChannel = item.getSourceFilterChannel();
            boolean useXXHashInFilter = item.useXXHashInFilter();

            FragmentRFItemKey itemKey = itemKeys[i];
            RFBloomFilter[] rfBloomFilters = rfBloomFilterMap.get(itemKey);

            // We have not received the runtime filter of this item key from build side.
            if (rfBloomFilters == null) {
                continue;
            }

            // check runtime filter efficiency.
            if (!efficiencyChecker.check(itemKey)) {
                continue;
            }

            final int originalCount = selectedCount;
            switch (item.getRFType()) {
            case BROADCAST: {
                selectedCount = chunk.getBlock(filterChannel).mightContainsLong(rfBloomFilters[0], bitmap, true);
                break;
            }
            case LOCAL: {
                if (useXXHashInFilter) {
                    selectedCount =
                        chunk.getBlock(filterChannel).mightContainsLong(totalPartitionCount, rfBloomFilters, bitmap,
                            true, true);
                } else {
                    selectedCount =
                        chunk.getBlock(filterChannel).mightContainsInt(totalPartitionCount, rfBloomFilters, bitmap,
                            false, true);
                }
                break;
            }
            }

            // sample the filter ratio of runtime filter.
            efficiencyChecker.sample(itemKey, originalCount, selectedCount);
        }

        // statistics for filtered rows by runtime filter.
        operatorStatistics.addRuntimeFilteredCount(initialSelectedCount - selectedCount);

        return selectedCount;
    }

    @Override
    public boolean isConstantExpression() {
        return false;
    }
}
