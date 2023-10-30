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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.ArrayList;
import java.util.List;

public class AggOpenHashMap extends GroupOpenHashMap implements AggHashMap {

    private final List<Aggregator> aggregators;

    protected List<Chunk> groupChunks;

    protected List<Chunk> valueChunks;

    protected final BlockBuilder[] valueBlockBuilders;

    private int[] filterArgs;

    private DistinctSet[] distinctSets;

    private DataType[] aggValueType;

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, int chunkSize, ExecutionContext context) {
        this(groupKeyType, aggregators, aggValueType, inputType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize, context);
    }

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, float loadFactor, int chunkSize,
                          ExecutionContext context) {
        super(groupKeyType, expectedSize, loadFactor, chunkSize, context);

        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.aggregators = aggregators;

        this.filterArgs = new int[aggregators.size()];
        this.distinctSets = new DistinctSet[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            final AbstractAggregator aggregator = (AbstractAggregator) aggregators.get(i);
            aggregator.open(expectedSize);

            filterArgs[i] = aggregator.getFilterArg();
            int[] aggIndexInChunk = aggregator.getOriginTargetIndexes();
            if (aggregator.isDistinct()) {
                distinctSets[i] =
                    new DistinctSet(inputType, aggIndexInChunk, expectedSize, chunkSize, context);
            }
        }

        this.aggValueType = aggValueType;
        this.valueBlockBuilders = new BlockBuilder[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            valueBlockBuilders[i] = BlockBuilders.create(aggValueType[i], context);
        }

        if (noGroupBy()) {
            // add an empty chunk
            appendGroup(new Chunk(1), 0);
        }
    }

    @Override
    public int[] putChunk(Chunk keyChunk, Chunk inputChunk) {

        final int[] groupIds = new int[inputChunk.getPositionCount()];
        final boolean noGroupBy = noGroupBy();
        if (noGroupBy) {
            for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                groupIds[pos] = 0;
            }
        } else {
            for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                groupIds[pos] = innerPut(keyChunk, pos, -1);
            }
        }
        final Block groupIdBlock = IntegerBlock.wrap(groupIds);
        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
            boolean[] isDistinct = null;
            if (distinctSets[aggIndex] != null) {
                isDistinct = distinctSets[aggIndex].checkDistinct(groupIdBlock, inputChunk);
            }
            for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                boolean noFilter = true;
                if (filterArgs[aggIndex] > -1) {
                    Object obj = inputChunk.getBlock(filterArgs[aggIndex]).getObject(pos);
                    if (obj instanceof Boolean) {
                        noFilter = (Boolean) obj;
                    } else if (obj instanceof Long) {
                        long lVal = (Long) obj;
                        if (lVal < 1) {
                            noFilter = false;
                        }
                    }
                }
                if (noFilter) {
                    if (isDistinct == null || isDistinct[pos]) {
                        aggregators.get(aggIndex).accumulate(groupIds[pos], inputChunk, pos);
                    }
                }
            }
        }
        return groupIds;
    }

    @Override
    int appendGroup(Chunk chunk, int position) {
        int groupId = super.appendGroup(chunk, position);

        // Also add an initial value to accumulators
        aggregators.forEach(Aggregator::appendInitValue);
        return groupId;
    }

    @Override
    public List<Chunk> getGroupChunkList() {
        return groupChunks;
    }

    @Override
    public List<Chunk> getValueChunkList() {
        return valueChunks;
    }

    protected List<Chunk> buildValueChunks() {
        List<Chunk> chunks = new ArrayList<>();
        int offset = 0;
        for (int groupId = 0; groupId < getGroupCount(); groupId++) {
            for (int i = 0; i < aggregators.size(); i++) {
                aggregators.get(i).writeResultTo(groupId, valueBlockBuilders[i]);
            }

            // value chunks split by chunk size
            // hash window depend on this feature for simplicity, not change this part
            if (++offset == chunkSize) {
                chunks.add(buildValueChunk());
                offset = 0;
            }
        }
        if (offset > 0) {
            chunks.add(buildValueChunk());
        }

        return chunks;
    }

    private Chunk buildValueChunk() {
        Block[] blocks = new Block[valueBlockBuilders.length];
        for (int i = 0; i < valueBlockBuilders.length; i++) {
            blocks[i] = valueBlockBuilders[i].build();
            valueBlockBuilders[i] = valueBlockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    @Override
    public AggResultIterator buildChunks() {
        groupChunks = buildGroupChunks();
        valueChunks = buildValueChunks();
        return new HashAggResultIterator(groupChunks, valueChunks);
    }

    public WorkProcessor<Chunk> buildHashSortedResult() {
        return buildResult(hashSortedGroupIds());
    }

    private IntIterator hashSortedGroupIds() {
        this.groupChunks = groupKeyBuffer.buildChunks();
        this.keys = null;
        this.map = null;
        IntComparator comparator = new AbstractIntComparator() {
            @Override
            public int compare(int position1, int position2) {
                int chunkId1 = position1 / chunkSize;
                int offset1 = position1 % chunkSize;
                int chunkId2 = position2 / chunkSize;
                int offset2 = position2 % chunkSize;
                for (int i = 0; i < groupKeyType.length; i++) {
                    Object o1 = groupChunks.get(chunkId1).getBlock(i).getObjectForCmp(offset1);
                    Object o2 = groupChunks.get(chunkId2).getBlock(i).getObjectForCmp(offset2);
                    if (o1 == null && o2 == null) {
                        continue;
                    }

                    int n = ExecUtils.comp(o1, o2, groupKeyType[i], true);
                    if (n != 0) {
                        return n;
                    }
                }
                return 0;
            }

        };

        final int[] index = new int[getGroupCount()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        // sort
        IntArrays.quickSort(index, comparator);

        return new AbstractIntIterator() {

            private int position;

            @Override
            public boolean hasNext() {
                return position < index.length;
            }

            @Override
            public int nextInt() {
                return index[position++];
            }
        };
    }

    private WorkProcessor<Chunk> buildResult(IntIterator groupIds) {
        List<DataType> types = new ArrayList(groupKeyType.length + aggValueType.length);
        for (DataType dataType : groupKeyType) {
            types.add(dataType);
        }

        for (DataType dataType : aggValueType) {
            types.add(dataType);
        }

        final ChunkBuilder pageBuilder = new ChunkBuilder(types, chunkSize, context);
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return WorkProcessor.ProcessState.finished();
            }

            pageBuilder.reset();
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                pageBuilder.declarePosition();
                int groupId = groupIds.nextInt();
                groupKeyBuffer.appendValuesTo(groupId, pageBuilder);
                for (int i = 0; i < aggregators.size(); i++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(groupKeyType.length + i);
                    aggregators.get(i).writeResultTo(groupId, output);
                }
            }

            return WorkProcessor.ProcessState.ofResult(pageBuilder.build());
        });
    }

    @Override
    public void close() {
    }

    @Override
    public long estimateSize() {
        long size = super.estimateSize();
        if (groupChunks != null) {
            for (Chunk chunk : groupChunks) {
                size += chunk.estimateSize();
            }
        }
        if (valueChunks != null) {
            for (Chunk chunk : valueChunks) {
                size += chunk.estimateSize();
            }
        } else {
            size += aggregators.stream().mapToLong(Aggregator::estimateSize).sum();
        }
        return size;
    }
}
