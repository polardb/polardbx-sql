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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.NullBlock;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.operator.util.DistinctSet;
import com.alibaba.polardbx.executor.operator.util.ElementaryChunksIndex;
import com.alibaba.polardbx.executor.operator.util.HashAggResultIterator;
import com.alibaba.polardbx.executor.operator.util.TypedBuffer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.FastIntBloomFilter;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Hash Join Executor
 *
 * @author hongxi.chx
 */
public class HashGroupJoinExec extends AbstractJoinExec implements ConsumerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HashGroupJoinExec.class);

    /**
     * A placeholder to mark there is no more element in this position link
     */
    static final int LIST_END = ConcurrentRawHashTable.NOT_EXISTS;

    protected ConcurrentRawHashTable hashTable;
    protected int[] positionLinks;
    protected FastIntBloomFilter bloomFilter;

    private TypedBuffer groupKeyBuffer;
    private final DataType[] aggValueType;

    //agg
    protected final int[] groups;
    protected final List<Aggregator> aggregators;
    private List<Chunk> valueChunks;

    private final BlockBuilder[] valueBlockBuilders;

    private DistinctSet[] distinctSets;

    private HashAggResultIterator resultIterator;

    private Chunk nullChunk = new Chunk(new NullBlock(1));

    private int groupId = 0;

    private ElementaryChunksIndex outerChunks;
    private ElementaryChunksIndex outKeyChunks;

    private MemoryPool memoryPool;
    private MemoryAllocatorCtx memoryAllocator;

    private boolean passNothing;

    private int[] keys;
    private BitSet usedKeys;

    private boolean isFinished = false;
    private ListenableFuture<?> blocked = ProducerExecutor.NOT_BLOCKED;

    private List<DataType> outputRelDataTypes;

    private long needMemoryAllocated = 0;

    public HashGroupJoinExec(Executor outerInput,
                             Executor innerInput,
                             JoinRelType joinType,
                             List<DataType> outputRelDataTypes,
                             boolean maxOneRow,
                             List<EquiJoinKey> joinKeys,
                             IExpression otherCondition,
                             IExpression antiCondition,
                             int[] groups,
                             List<Aggregator> aggregators,
                             ExecutionContext context,
                             int expectedOutputRowCount) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, null, null, context);
        Preconditions.checkArgument(!semiJoin, "Don't support semiJoin!");
        this.groups = groups;
        this.aggregators = aggregators;
        this.outputRelDataTypes = outputRelDataTypes;
        createBlockBuilders();
        DataType[] aggInputType = innerInput.getDataTypes().toArray(new DataType[] {});
        DataType[] groupTypes = new DataType[groups.length];
        int i = 0;
        for (; i < groupTypes.length; i++) {
            groupTypes[i] = outputRelDataTypes.get(i);
        }
        DataType[] aggTypes = new DataType[aggregators.size()];
        for (int j = 0; i < outputRelDataTypes.size(); i++, j++) {
            aggTypes[j] = outputRelDataTypes.get(i);
        }

        this.groupKeyBuffer = TypedBuffer.create(groupTypes, chunkLimit, context);

        this.aggValueType = aggTypes;
        this.distinctSets = new DistinctSet[aggregators.size()];
        for (i = 0; i < aggregators.size(); i++) {
            final AbstractAggregator aggregator = (AbstractAggregator) aggregators.get(i);
            aggregator.open(expectedOutputRowCount);
            int[] aggIndexInChunk = aggregator.getOriginTargetIndexes();
            if (aggregator.isDistinct()) {
                distinctSets[i] =
                    new DistinctSet(aggInputType, aggIndexInChunk, expectedOutputRowCount, chunkLimit,
                        context);
            }
        }
        this.valueBlockBuilders = new BlockBuilder[aggregators.size()];
        for (i = 0; i < aggregators.size(); i++) {
            this.valueBlockBuilders[i] = BlockBuilders.create(aggValueType[i], context);
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputRelDataTypes;
    }

    @Override
    public void doOpen() {
        getProbeInput().open();
    }

    @Override
    public void openConsume() {
        outerChunks = new ElementaryChunksIndex(chunkLimit);
        outKeyChunks = new ElementaryChunksIndex(chunkLimit);

        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    @Override
    public void buildConsume() {
        if (outerChunks != null) {
            if (outerChunks.isEmpty() && joinType == JoinRelType.INNER) {
                passNothing = true;
            }
            doBuildHashTable();
            outerChunks.buildRow();
            outKeyChunks.buildRow();
        }
    }

    void doBuildHashTable() {
        final int size = outKeyChunks.getPositionCount();
        hashTable = new ConcurrentRawHashTable(size);
        keys = new int[size];
        Arrays.fill(keys, LIST_END);
        usedKeys = new BitSet(size);
        memoryAllocator.allocateReservedMemory(usedKeys.length());
        positionLinks = new int[size];
        Arrays.fill(positionLinks, LIST_END);

        if (size <= BLOOM_FILTER_ROWS_LIMIT) {
            bloomFilter = FastIntBloomFilter.create(size);
            memoryAllocator.allocateReservedMemory(bloomFilter.sizeInBytes());
        }
        int position = 0;

        for (int chunkId = 0; chunkId < outKeyChunks.getChunkCount(); ++chunkId) {
            final Chunk keyChunk = outKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks, bloomFilter, aggregators);
            position += keyChunk.getPositionCount();
        }
        assert position == size;

        // Allocate memory for the hash-table
        memoryAllocator.allocateReservedMemory(hashTable.estimateSize());
        memoryAllocator.allocateReservedMemory(SizeOf.sizeOf(positionLinks));

        logger.info("complete building hash table");
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        outerChunks.addChunk(inputChunk);
        memoryAllocator.allocateReservedMemory(inputChunk.estimateSize());

        Chunk keyChunk = outerKeyChunkGetter.apply(inputChunk);
        outKeyChunks.addChunk(keyChunk);
        memoryAllocator.allocateReservedMemory(keyChunk.estimateSize());

    }

    @Override
    void doClose() {
        getProbeInput().close();
        closeConsume(true);

        this.hashTable = null;
        this.positionLinks = null;
    }

    @Override
    public void closeConsume(boolean force) {
        if (outerChunks != null) {
            outerChunks = null;
            outKeyChunks = null;
            if (memoryPool != null) {
                collectMemoryUsage(memoryPool);
                memoryPool.destroy();
            }
        }
    }

    private int matchInit(int hashCode, Chunk keyChunk, int position) {
        if (bloomFilter != null && !bloomFilter.mightContain(hashCode)) {
            return LIST_END;
        }

        int matchedPosition = hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (outKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    private int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (outKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    private boolean matchValid(int current) {
        return current != LIST_END;
    }

    private void buildOneChunk(Chunk keyChunk, int position, ConcurrentRawHashTable hashTable,
                               int[] positionLinks,
                               FastIntBloomFilter bloomFilter, List<Aggregator> aggregators) {
        // Calculate hash codes of the whole chunk
        int[] hashes = keyChunk.hashCodeVector();

        for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
            int next = hashTable.put(position, hashes[offset]);
            positionLinks[position] = next;
            if (bloomFilter != null) {
                bloomFilter.put(hashes[offset]);
            }
            aggregators.forEach(Aggregator::appendInitValue);
        }
    }

    @Override
    Chunk doNextChunk() {
        if (passNothing) {
            isFinished = true;
            return null;
        }
        if (resultIterator == null) {
            if (needMemoryAllocated != 0) {
                memoryAllocator.allocateReservedMemory(needMemoryAllocated);
                needMemoryAllocated = 0;
            }
            Chunk inputChunk = nextProbeChunk();
            boolean inputIsFinished = false;
            if (inputChunk != null) {
                // Process outer rows in this input chunk
                long beforeEstimateSize = aggregators.stream().mapToLong(Aggregator::estimateSize).sum();
                calculateJoinAgg(inputChunk);
                long afterEstimateSize = aggregators.stream().mapToLong(Aggregator::estimateSize).sum();
                needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize, 0);
            } else {
                inputIsFinished = getProbeInput().produceIsFinished();
            }
            if (inputIsFinished) {
                //input is already finished.
                if (joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT) {
                    final Chunk nullChunk = this.nullChunk;
                    for (int i = usedKeys.nextClearBit(0), size = outKeyChunks.getPositionCount(); i >= 0 && i < size;
                         i = usedKeys.nextClearBit(i + 1)) {
                        buildNullRow(nullChunk, 0, i);
                    }
                }
                resultIterator = buildChunks();
            } else {
                isFinished = false;
                if (inputChunk == null) {
                    blocked = getProbeInput().produceIsBlocked();
                }
                return null;
            }
        }
        Chunk ret = resultIterator.nextChunk();

        if (ret == null || ret.getPositionCount() == 0) {
            isFinished = true;
        }
        return ret;
    }

    private Chunk nextProbeChunk() {
        return getProbeInput().nextChunk();
    }

    private void calculateJoinAgg(Chunk inputChunk) {
        final int positionCount = inputChunk.getPositionCount();
        Chunk inputJoinKeyChunk = getProbeKeyChunkGetter().apply(inputChunk);
        int[] currInputJoinKeyChunks = inputJoinKeyChunk.hashCodeVector();
        final int[] ints = currInputJoinKeyChunks;
        int position = 0;
        boolean isMatching = false;
        boolean matched = false;
        int matchedPosition = LIST_END;
        for (; position < positionCount; position++) {

            // reset matched flag unless it's still during matching
            if (!isMatching) {
                matched = false;
                matchedPosition = matchInit(ints[position], inputJoinKeyChunk, position);
            } else {
                // continue from the last processed match
                matchedPosition = matchNext(matchedPosition, inputJoinKeyChunk, position);
                isMatching = false;
            }

            for (; matchValid(matchedPosition);
                 matchedPosition = matchNext(matchedPosition, inputJoinKeyChunk, position)) {
                if (!checkJoinCondition(inputChunk, position, matchedPosition)) {
                    continue;
                }
                buildJoinRow(position, matchedPosition, inputChunk);
                // checks max1row generated from scalar subquery
                if (singleJoin && matched && !(ConfigDataMode.isFastMock())) {
                    throw GeneralUtil.nestedException("Scalar subquery returns more than 1 row");
                }
                // set matched flag
                matched = true;
            }
        }
    }

    private boolean checkJoinCondition(Chunk outerChunk, int outerPosition, int innerPosition) {
        if (condition == null) {
            return true;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = outerChunks.rowAt(innerPosition);

        final Row leftRow = joinType.leftSide(outerRow, innerRow);
        final Row rightRow = joinType.rightSide(outerRow, innerRow);
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);

        return checkJoinCondition(joinRow);
    }

    public HashAggResultIterator buildChunks() {
        final List<Chunk> groupChunks = buildGroupChunks();
        valueChunks = buildValueChunks();
        return new HashAggResultIterator(groupChunks, valueChunks);
    }

    private List<Chunk> buildValueChunks() {
        List<Chunk> chunks = new ArrayList<>();
        switch (joinType) {
        case INNER: //for Inner , the groupId is grow from zero
            int offset = 0;
            for (int groupId = 0, i = usedKeys.nextSetBit(0), size = outKeyChunks.getPositionCount();
                 i >= 0 && i < size;
                 i = usedKeys.nextSetBit(i + 1)) {
                for (int j = 0; j < aggregators.size(); j++) {
                    aggregators.get(j).writeResultTo(groupId++, valueBlockBuilders[j]);
                }
                if (++offset == chunkLimit) {
                    chunks.add(buildValueChunk());
                    offset = 0;
                }
            }
            if (offset > 0) {
                chunks.add(buildValueChunk());
            }
            break;
        case LEFT:
        case RIGHT:
            int offsetLeft = 0;
            for (int keyIndex = 0; keyIndex < this.outKeyChunks.getPositionCount(); keyIndex++) {
                for (int j = 0; j < aggregators.size(); j++) {
                    aggregators.get(j).writeResultTo(keyIndex, valueBlockBuilders[j]);
                }
                if (++offsetLeft == chunkLimit) {
                    chunks.add(buildValueChunk());
                    offsetLeft = 0;
                }
            }
            if (offsetLeft > 0) {
                chunks.add(buildValueChunk());
            }
            break;
        default:
            throw new UnsupportedOperationException("Don't support joinType: " + joinType);

        }

        return chunks;
    }

    List<Chunk> buildGroupChunks() {
        List<Chunk> chunks = groupKeyBuffer.buildChunks();
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

    public void buildJoinRow(int position, int matchedPosition, Chunk inputChunk) {
        usedKeys.set(matchedPosition);
        final Chunk.ChunkRow chunkRow = outerChunks.rowAt(matchedPosition);
        //build group keys
        final int key = keys[matchedPosition];
        if (key == LIST_END) {
            keys[matchedPosition] = appendAndIncrementGroup(chunkRow.getChunk(), chunkRow.getPosition());
        }

        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
            boolean[] isDistinct = null;
            if (distinctSets[aggIndex] != null) {
                final IntegerBlockBuilder integerBlockBuilder = new IntegerBlockBuilder(1);
                integerBlockBuilder.writeInt(keys[matchedPosition]);
                isDistinct = distinctSets[aggIndex]
                    .checkDistinct(integerBlockBuilder.build(), inputChunk, position);
            }
            if (isDistinct == null || isDistinct[0]) {
                //to accumulate the Aggregate function result，here need 'aggregate convert' and 'aggregate accumulator'
                doAggregate(inputChunk, aggIndex, position, keys[matchedPosition]);
            }
        }
    }

    public void buildNullRow(Chunk inputChunk, int position, int matchedPosition) {
        final Chunk.ChunkRow chunkRow = outerChunks.rowAt(matchedPosition);
        final int key = keys[matchedPosition];
        if (key == LIST_END) {
            keys[matchedPosition] = appendAndIncrementGroup(chunkRow.getChunk(), chunkRow.getPosition());
        }
        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
            //对右表计算Agg值，这里需要 aggregate convert 和 aggregate accumulator
            doAggregate(inputChunk, aggIndex, position, keys[matchedPosition]);
        }

    }

    private void doAggregate(Chunk aggInputChunk, int aggIndex, int position, int groupId) {
        assert aggInputChunk != null;
        aggregators.get(aggIndex).accumulate(groupId, aggInputChunk, position);
    }

    private int appendAndIncrementGroup(Chunk chunk, int position) {
        groupKeyBuffer.appendRow(chunk, position);
        return groupId++;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    private Executor getBuildInput() {
        return outerInput;
    }

    private Executor getProbeInput() {
        return innerInput;
    }

    private ChunkConverter getBuildKeyChunkGetter() {
        return outerKeyChunkGetter;
    }

    private ChunkConverter getProbeKeyChunkGetter() {
        return innerKeyChunkGetter;
    }
}
