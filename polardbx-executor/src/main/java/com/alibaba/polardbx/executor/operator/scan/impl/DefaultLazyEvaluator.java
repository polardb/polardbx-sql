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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.RFEfficiencyChecker;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RelativeRangeConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Specialized for expression evaluation.
 */
public class DefaultLazyEvaluator implements LazyEvaluator<Chunk, BitSet> {
    private final VectorizedExpression condition;
    private final MutableChunk preAllocatedChunk;
    private final boolean zeroCopy;

    /**
     * The count of input columns.
     */
    private final int inputVectorCount;

    /**
     * The bitmap of columns for filter evaluation, and it's a subset of project columns.
     */
    private final boolean[] filterVectorBitmap;

    /**
     * Record some session-level or global variables related to evaluation.
     */
    private final ExecutionContext context;

    private final List<DataType> inputTypes;

    private final double ratio;

    private final boolean reuseVector;
    private EvaluationContext evaluationContext;

    /**
     * for constant expression.
     */
    private final boolean isConstant;

    private volatile FragmentRFManager fragmentRFManager;
    private OperatorStatistics operatorStatistics;
    private RFEfficiencyChecker efficiencyChecker;
    private Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap;

    public DefaultLazyEvaluator(VectorizedExpression condition, MutableChunk preAllocatedChunk, boolean zeroCopy,
                                int inputVectorCount, boolean[] filterVectorBitmap, ExecutionContext context,
                                List<DataType> inputTypes, double ratio) {
        this.condition = condition;
        this.preAllocatedChunk = preAllocatedChunk;
        this.zeroCopy = zeroCopy;
        this.inputVectorCount = inputVectorCount;
        this.filterVectorBitmap = filterVectorBitmap;
        this.context = context;
        this.inputTypes = inputTypes;
        this.ratio = ratio;
        this.reuseVector = context.getParamManager().getBoolean(ConnectionParams.ENABLE_REUSE_VECTOR);
        this.evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        this.isConstant = VectorizedExpressionUtils.isConstantExpression(condition);
    }

    enum EvaluationStrategy {
        /**
         * No element is selected for evaluation.
         */
        NO_SELECTION,
        /**
         * All elements are selected for evaluation.
         */
        FULL_SELECTION,
        /**
         * Only part of elements are selected for evaluation.
         */
        PARTIAL_SELECTION,
        /**
         * All elements are selected for evaluation.
         * And then, elements marked as deleted (in deletion bitmap) will be removed.
         */
        POST_INTERSECTION;

        static EvaluationStrategy get(int positionCount, int cardinality, double ratio) {
            EvaluationStrategy evaluationStrategy;
            if (cardinality == positionCount) {
                evaluationStrategy = EvaluationStrategy.NO_SELECTION;
            } else if (cardinality == 0) {
                evaluationStrategy = EvaluationStrategy.FULL_SELECTION;
            } else if (cardinality < positionCount * ratio) {
                evaluationStrategy = EvaluationStrategy.POST_INTERSECTION;
            } else {
                evaluationStrategy = EvaluationStrategy.PARTIAL_SELECTION;
            }
            return evaluationStrategy;
        }
    }

    public void registerRF(FragmentRFManager fragmentRFManager, OperatorStatistics operatorStatistics,
                           Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap) {
        this.fragmentRFManager = fragmentRFManager;
        this.operatorStatistics = operatorStatistics;
        this.efficiencyChecker = new RFEfficiencyCheckerImpl(
            fragmentRFManager.getSampleCount(), fragmentRFManager.getFilterRatioThreshold());
        this.rfBloomFilterMap = rfBloomFilterMap;
    }

    public static DefaultLazyEvaluatorBuilder builder() {
        return new DefaultLazyEvaluatorBuilder();
    }

    public VectorizedExpression getCondition() {
        return condition;
    }

    private int[] genPreSelection(int startPosition, int positionCount, RoaringBitmap deletion, int cardinality) {
        final int[] preSelection = new int[positionCount - cardinality];

        // remove deleted positions.
        deletion.forAllInRange(startPosition, positionCount, new RelativeRangeConsumer() {
            private int selectionIndex = 0;

            @Override
            public void acceptPresent(int relativePos) {
                // for present index, nothing to do
            }

            @Override
            public void acceptAbsent(int relativePos) {
                preSelection[selectionIndex++] = relativePos;
            }

            @Override
            public void acceptAllPresent(int relativeFrom, int relativeTo) {
                // for present index, nothing to do
            }

            @Override
            public void acceptAllAbsent(int relativeFrom, int relativeTo) {
                for (int pos = relativeFrom; pos < relativeTo; pos++) {
                    preSelection[selectionIndex++] = pos;
                }
            }
        });

        return preSelection;
    }

    @Override
    public int eval(Chunk chunk, int startPosition, int positionCount, RoaringBitmap deletion, boolean[] bitmap) {
        // check existence in given range.
        long cardinality = deletion.rangeCardinality(startPosition, startPosition + positionCount);
        Preconditions.checkArgument(cardinality <= positionCount);

        EvaluationStrategy evaluationStrategy = EvaluationStrategy.get(
            positionCount, (int) cardinality, ratio
        );

        // preprocessing of evaluation.
        switch (evaluationStrategy) {
        case NO_SELECTION: {
            // all positions are marked as deleted.
            return 0;
        }
        case PARTIAL_SELECTION: {
            // partial positions are selected.
            final int[] preSelection = genPreSelection(startPosition, positionCount, deletion, (int) cardinality);

            // Allocate or reuse memory for intermediate results
            final int resultRows = chunk.getPositionCount();
            // Clean the chunk since last round may use selection
            preAllocatedChunk.setSelectionInUse(false);
            if (reuseVector) {
                preAllocatedChunk.allocateWithReuse(resultRows, inputVectorCount);
            } else {
                preAllocatedChunk.reallocate(resultRows, inputVectorCount, false);
            }

            // Prepare selection array for evaluation.
            preAllocatedChunk.setBatchSize(preSelection.length);
            preAllocatedChunk.setSelection(preSelection);
            preAllocatedChunk.setSelectionInUse(true);

            break;
        }
        case POST_INTERSECTION:
        case FULL_SELECTION: {
            // Allocate or reuse memory for intermediate results
            final int resultRows = chunk.getPositionCount();
            // Clean the chunk since last round may use selection
            preAllocatedChunk.setSelectionInUse(false);
            if (reuseVector) {
                preAllocatedChunk.allocateWithReuse(resultRows, inputVectorCount);
            } else {
                preAllocatedChunk.reallocate(resultRows, inputVectorCount, false);
            }

            break;
        }
        default:
        }

        if (zeroCopy) {
            // If in zero copy mode, set reference of original block into pre-allocated chunk.
            for (int i = 0; i < inputVectorCount; i++) {
                if (filterVectorBitmap[i]) {
                    Block block = chunk.getBlock(i);
                    preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
                }
            }
        } else {
            // If not in zero copy mode, copy all blocks for filter evaluation.
            for (int i = 0; i < inputVectorCount; i++) {
                if (filterVectorBitmap[i]) {
                    Block cachedBlock = chunk.getBlock(i);
                    DataType dataType = inputTypes.get(i);
                    BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

                    for (int j = 0; j < chunk.getPositionCount(); j++) {
                        cachedBlock.writePositionTo(j, blockBuilder);
                    }

                    RandomAccessBlock copied = (RandomAccessBlock) blockBuilder.build();
                    preAllocatedChunk.setSlotAt(copied, i);
                }
            }
        }

        // Do evaluation
        condition.eval(evaluationContext);

        // clear bitmap
        Arrays.fill(bitmap, false);

        // get filtered selection result
        int selectedCount = 0;
        switch (evaluationStrategy) {
        case FULL_SELECTION: {
            selectedCount = handleFullSelection(bitmap, positionCount);
            break;
        }
        case PARTIAL_SELECTION: {
            selectedCount = handlePartialSelection(bitmap, positionCount);
            break;
        }
        case POST_INTERSECTION: {
            selectedCount = handlePostIntersection(bitmap, positionCount, deletion, startPosition);
            break;
        }
        case NO_SELECTION:
        default: {
            throw GeneralUtil.nestedException("invalid strategy for post processing.");
        }
        }

        if (fragmentRFManager == null) {
            return selectedCount;
        }

        // handle RF
        final int totalPartitionCount = fragmentRFManager.getTotalPartitionCount();
        final int selectedCountBeforeRF = selectedCount;
        for (Map.Entry<FragmentRFItemKey, FragmentRFItem> entry : fragmentRFManager.getAllItems().entrySet()) {

            FragmentRFItem item = entry.getValue();
            int filterChannel = item.getSourceFilterChannel();
            boolean useXXHashInFilter = item.useXXHashInFilter();
            FragmentRFManager.RFType rfType = item.getRFType();

            FragmentRFItemKey itemKey = entry.getKey();
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
            Block filterBlock = chunk.getBlock(filterChannel).cast(Block.class);
            switch (rfType) {
            case BROADCAST: {
                selectedCount = filterBlock.mightContainsLong(rfBloomFilters[0], bitmap, true);
                break;
            }
            case LOCAL: {
                if (useXXHashInFilter) {
                    // The partition of this chunk is consistent.
                    selectedCount =
                        filterBlock.mightContainsLong(totalPartitionCount, rfBloomFilters, bitmap, true, true);
                } else {
                    // For local test.
                    int hitCount = 0;
                    for (int pos = 0; pos < chunk.getPositionCount(); pos++) {

                        if (bitmap[pos]) {
                            int partition = getPartition(filterBlock, pos, totalPartitionCount);

                            int hashCode = filterBlock.hashCode(pos);
                            bitmap[pos] &= rfBloomFilters[partition].mightContainInt(hashCode);
                            if (bitmap[pos]) {
                                hitCount++;
                            }
                        }

                    }

                    selectedCount = hitCount;
                }
                break;
            }
            }
            // sample the filter ratio of runtime filter.
            efficiencyChecker.sample(itemKey, originalCount, selectedCount);

        }
        // statistics for filtered rows by runtime filter.
        operatorStatistics.addRuntimeFilteredCount(selectedCountBeforeRF - selectedCount);

        return selectedCount;
    }

    private static int getPartition(Block block, int position, int partitionCount) {

        // Convert the searchVal from field space to hash space
        long hashVal = block.hashCodeUseXxhash(position);
        int partition = (int) ((hashVal & Long.MAX_VALUE) % partitionCount);

        return partition;
    }

    @Override
    public boolean isConstantExpression() {
        return isConstant;
    }

    @Override
    public BitSet eval(Chunk chunk, int startPosition, int positionCount, RoaringBitmap deletion) {

        // check existence in given range.
        long cardinality = deletion.rangeCardinality(startPosition, startPosition + positionCount);
        Preconditions.checkArgument(cardinality <= positionCount);

        final EvaluationStrategy evaluationStrategy = EvaluationStrategy.get(
            positionCount, (int) cardinality, ratio
        );

        // preprocessing of evaluation.
        switch (evaluationStrategy) {
        case NO_SELECTION: {
            // all positions are marked as deleted.
            return new BitSet(0);
        }
        case PARTIAL_SELECTION: {
            // partial positions are selected.
            final int[] preSelection = genPreSelection(startPosition, positionCount, deletion, (int) cardinality);

            // Allocate or reuse memory for intermediate results
            final int resultRows = chunk.getPositionCount();
            // Clean the chunk since last round may use selection
            preAllocatedChunk.setSelectionInUse(false);
            if (reuseVector) {
                preAllocatedChunk.allocateWithReuse(resultRows, inputVectorCount);
            } else {
                preAllocatedChunk.reallocate(resultRows, inputVectorCount, false);
            }

            // Prepare selection array for evaluation.
            preAllocatedChunk.setBatchSize(preSelection.length);
            preAllocatedChunk.setSelection(preSelection);
            preAllocatedChunk.setSelectionInUse(true);

            break;
        }
        case POST_INTERSECTION:
        case FULL_SELECTION: {
            // Allocate or reuse memory for intermediate results
            final int resultRows = chunk.getPositionCount();
            // Clean the chunk since last round may use selection
            preAllocatedChunk.setSelectionInUse(false);
            if (reuseVector) {
                preAllocatedChunk.allocateWithReuse(resultRows, inputVectorCount);
            } else {
                preAllocatedChunk.reallocate(resultRows, inputVectorCount, false);
            }

            break;
        }
        default:
        }

        if (zeroCopy) {
            // If in zero copy mode, set reference of original block into pre-allocated chunk.
            for (int i = 0; i < inputVectorCount; i++) {
                if (filterVectorBitmap[i]) {
                    Block block = chunk.getBlock(i);
                    preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
                }
            }
        } else {
            // If not in zero copy mode, copy all blocks for filter evaluation.
            for (int i = 0; i < inputVectorCount; i++) {
                if (filterVectorBitmap[i]) {
                    Block cachedBlock = chunk.getBlock(i);
                    DataType dataType = inputTypes.get(i);
                    BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

                    for (int j = 0; j < chunk.getPositionCount(); j++) {
                        cachedBlock.writePositionTo(j, blockBuilder);
                    }

                    RandomAccessBlock copied = (RandomAccessBlock) blockBuilder.build();
                    preAllocatedChunk.setSlotAt(copied, i);
                }
            }
        }

        // Do evaluation
        condition.eval(evaluationContext);

        // get filtered selection result
        switch (evaluationStrategy) {
        case FULL_SELECTION: {
            return handleFullSelection(positionCount);
        }
        case PARTIAL_SELECTION: {
            return handlePartialSelection(positionCount);
        }
        case POST_INTERSECTION: {
            return handlePostIntersection(positionCount, deletion, startPosition);
        }
        case NO_SELECTION:
        default: {
            throw GeneralUtil.nestedException("invalid strategy for post processing.");
        }
        }
    }

    @NotNull
    private BitSet handlePostIntersection(int positionCount, RoaringBitmap deletion, int startPosition) {
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();
        int batchSize = preAllocatedChunk.batchSize();
        BitSet result = new BitSet(positionCount);
        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                if (longInputArray[i] != 0 &&
                    (nulls == null || !nulls[i]) &&
                    !deletion.contains(i + startPosition)) {
                    result.set(i);
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                if (intInputArray[i] != 0 &&
                    (nulls == null || !nulls[i]) &&
                    !deletion.contains(i + startPosition)) {
                    result.set(i);
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return result;
    }

    @NotNull
    private BitSet handlePartialSelection(int positionCount) {
        // Get pre-selection array for position mapping
        Preconditions.checkArgument(preAllocatedChunk.isSelectionInUse());
        int batchSize = preAllocatedChunk.batchSize();
        int[] preSelection = preAllocatedChunk.selection();
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();
        BitSet result = new BitSet(positionCount);
        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                int j = preSelection[i];
                if (longInputArray[j] != 0 &&
                    (nulls == null || !nulls[i])) {
                    result.set(j);
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                int j = preSelection[i];
                if (intInputArray[j] != 0 &&
                    (nulls == null || !nulls[i])) {
                    result.set(j);
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return result;
    }

    @NotNull
    private BitSet handleFullSelection(int positionCount) {
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();
        BitSet result = new BitSet(positionCount);
        int batchSize = preAllocatedChunk.batchSize();
        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                if (longInputArray[i] != 0 &&
                    (nulls == null || !nulls[i])) {
                    result.set(i);
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                if (intInputArray[i] != 0 &&
                    (nulls == null || !nulls[i])) {
                    result.set(i);
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return result;
    }

    @NotNull
    private int handlePostIntersection(boolean[] bitmap, int positionCount, RoaringBitmap deletion, int startPosition) {
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();
        int batchSize = preAllocatedChunk.batchSize();

        int selectCount = 0;

        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                if (longInputArray[i] != 0 &&
                    (nulls == null || !nulls[i]) &&
                    !deletion.contains(i + startPosition)) {
                    bitmap[i] = true;
                    selectCount++;
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                if (intInputArray[i] != 0 &&
                    (nulls == null || !nulls[i]) &&
                    !deletion.contains(i + startPosition)) {
                    bitmap[i] = true;
                    selectCount++;
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return selectCount;
    }

    @NotNull
    private int handlePartialSelection(boolean[] bitmap, int positionCount) {
        // Get pre-selection array for position mapping
        Preconditions.checkArgument(preAllocatedChunk.isSelectionInUse());
        int batchSize = preAllocatedChunk.batchSize();
        int[] preSelection = preAllocatedChunk.selection();
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();

        int selectCount = 0;

        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                int j = preSelection[i];
                if (longInputArray[j] != 0 &&
                    (nulls == null || !nulls[i])) {
                    bitmap[j] = true;
                    selectCount++;
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                int j = preSelection[i];
                if (intInputArray[j] != 0 &&
                    (nulls == null || !nulls[i])) {
                    bitmap[j] = true;
                    selectCount++;
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return selectCount;
    }

    @NotNull
    private int handleFullSelection(boolean[] bitmap, int positionCount) {
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        boolean[] nulls = resultBlock.nulls();

        int selectCount = 0;

        int batchSize = preAllocatedChunk.batchSize();
        if (resultBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) resultBlock).longArray();
            for (int i = 0; i < batchSize; i++) {
                if (longInputArray[i] != 0 &&
                    (nulls == null || !nulls[i])) {
                    bitmap[i] = true;
                    selectCount++;
                }
            }
        } else if (resultBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) resultBlock).intArray();
            for (int i = 0; i < batchSize; i++) {
                if (intInputArray[i] != 0 &&
                    (nulls == null || !nulls[i])) {
                    bitmap[i] = true;
                    selectCount++;
                }
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + resultBlock.getClass());
        }

        return selectCount;
    }
}
