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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.accumulator.Accumulator;
import com.alibaba.polardbx.executor.accumulator.AccumulatorBuilders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.NullBlock;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.executor.operator.util.DistinctSet;
import com.alibaba.polardbx.executor.operator.util.HashAggResultIterator;
import com.alibaba.polardbx.executor.operator.util.TypedBuffer;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static com.alibaba.polardbx.executor.utils.ExecUtils.checkJoinKeysAllNullSafe;
import static com.alibaba.polardbx.executor.utils.ExecUtils.checkJoinKeysNulSafe;

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
    protected ConcurrentIntBloomFilter bloomFilter;

    private TypedBuffer groupKeyBuffer;
    private final DataType[] aggValueType;

    //agg
    protected final int[] groups;
    protected final List<Aggregator> aggregators;
    private List<Chunk> valueChunks;

    private final BlockBuilder[] valueBlockBuilders;

    private ChunkConverter[] valueConverters;

    protected Accumulator[] valueAccumulators;

    private DistinctSet[] distinctSets;

    private HashAggResultIterator resultIterator;

    private Chunk nullChunk = new Chunk(new NullBlock(chunkLimit));

    private int groupId = 0;

    private ChunksIndex outerChunks;
    private ChunksIndex outKeyChunks;

    private MemoryPool memoryPool;
    private MemoryAllocatorCtx memoryAllocator;

    private boolean passNothing;

    private int[] keys;
    private BitSet usedKeys;
    private Chunk[] inputAggregatorInputs;

    private boolean isFinished = false;
    private ListenableFuture<?> blocked = ProducerExecutor.NOT_BLOCKED;

    private List<DataType> outputRelDataTypes;

    private GroupJoinProbeOperator probeOperator;

    public HashGroupJoinExec(Executor outerInput,
                             Executor innerInput,
                             JoinRelType joinType,
                             List<DataType> outputRelDataTypes,
                             List<DataType> joinOutputTypes,
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
        this.inputAggregatorInputs = new Chunk[aggregators.size()];
        this.outputRelDataTypes = outputRelDataTypes;
        createBlockBuilders();

        DataType[] aggInputType = joinOutputTypes.toArray(new DataType[joinOutputTypes.size()]);
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
        //init aggregate accumulator
        this.valueAccumulators = new Accumulator[aggregators.size()];
        this.valueConverters = new ChunkConverter[aggregators.size()];
        this.distinctSets = new DistinctSet[aggregators.size()];
        for (i = 0; i < aggregators.size(); i++) {
            final Aggregator aggregator = aggregators.get(i);
            this.valueAccumulators[i] =
                AccumulatorBuilders
                    .create(aggregator, aggValueType[i], aggInputType, expectedOutputRowCount, this.context);

            DataType[] originalInputTypes = DataTypeUtils.gather(aggInputType, aggregator.getInputColumnIndexes());
            DataType[] accumulatorInputTypes = Util.first(valueAccumulators[i].getInputTypes(), originalInputTypes);
            this.valueConverters[i] =
                Converters.createChunkConverter(aggregator.getInputColumnIndexes(), aggInputType, accumulatorInputTypes,
                    joinType == JoinRelType.RIGHT ? 0 : outerInput.getDataTypes().size(), context);

            if (aggregator.isDistinct()) {
                int[] distinctIndexes = aggregator.getNewForAccumulator().getAggTargetIndexes();
                this.distinctSets[i] =
                    new DistinctSet(accumulatorInputTypes, distinctIndexes, expectedOutputRowCount, chunkLimit,
                        context);
            }
        }
        this.valueBlockBuilders = new BlockBuilder[aggregators.size()];
        for (i = 0; i < aggregators.size(); i++) {
            this.valueBlockBuilders[i] = BlockBuilders.create(aggValueType[i], context);
        }

        this.outerChunks = new ChunksIndex();
        this.outKeyChunks = new ChunksIndex();

        boolean enableVecJoin = context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_JOIN);

        boolean hasNoDistinct = true;
        for (int index = 0; index < distinctSets.length; index++) {
            hasNoDistinct &= distinctSets[index] == null;
        }

        final boolean isSingleIntegerType =
            joinKeys.size() == 1 && (joinKeys.get(0).getUnifiedType() instanceof IntegerType) && !joinKeys.get(0)
                .isNullSafeEqual();

        if (enableVecJoin && isSingleIntegerType && hasNoDistinct && condition == null) {
            this.probeOperator = new IntGroupJoinProbeOperator();

            // for fast group key output
            this.groupKeyBuffer = TypedBuffer.createTypeSpecific(DataTypes.IntegerType, chunkLimit, context);

            // for fast probe
            this.outKeyChunks.setTypedHashTable(new TypedListHandle() {
                private TypedList[] typedLists;

                @Override
                public long estimatedSize(int fixedSize) {
                    return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
                }

                @Override
                public TypedList[] getTypedLists(int fixedSize) {
                    if (typedLists == null) {
                        typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                    }
                    return typedLists;
                }

                @Override
                public void consume(Chunk chunk, int sourceIndex) {
                    chunk.getBlock(0).cast(Block.class)
                        .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
                }
            });
        } else {
            this.probeOperator = new DefaultGroupJoinProbeOperator();
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

        // create type list
        outKeyChunks.openTypedHashTable();

        if (size <= BLOOM_FILTER_ROWS_LIMIT) {
            bloomFilter = ConcurrentIntBloomFilter.create(size);
            memoryAllocator.allocateReservedMemory(bloomFilter.sizeInBytes());
        }
        int position = 0;

        for (int chunkId = 0; chunkId < outKeyChunks.getChunkCount(); ++chunkId) {
            outKeyChunks.addChunkToTypedList(chunkId);

            final Chunk keyChunk = outKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks, bloomFilter, aggregators, valueAccumulators);
            position += keyChunk.getPositionCount();
        }
        assert position == size;

        // Allocate memory for the hash-table
        memoryAllocator.allocateReservedMemory(hashTable.estimateSizeInBytes());
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

    private void buildOneChunk(Chunk keyChunk, int position, ConcurrentRawHashTable hashTable,
                               int[] positionLinks,
                               ConcurrentIntBloomFilter bloomFilter, List<Aggregator> aggregators,
                               Accumulator[] valueAccumulators) {
        // Calculate hash codes of the whole chunk
        int[] hashes = keyChunk.hashCodeVector();

        if (checkJoinKeysAllNullSafe(keyChunk, ignoreNullBlocks)) {
            // If all keys are not null, we can leave out the null-check procedure
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                int next = hashTable.put(position, hashes[offset]);
                positionLinks[position] = next;
                if (bloomFilter != null) {
                    bloomFilter.putInt(hashes[offset]);
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    valueAccumulators[i].appendInitValue();
                }
            }

        } else {
            // Otherwise we have to check nullability for each row
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                if (checkJoinKeysNulSafe(keyChunk, offset, ignoreNullBlocks)) {
                    int next = hashTable.put(position, hashes[offset]);
                    positionLinks[position] = next;
                    if (bloomFilter != null) {
                        bloomFilter.putInt(hashes[offset]);
                    }
                    for (int i = 0; i < aggregators.size(); i++) {
                        valueAccumulators[i].appendInitValue();
                    }
                }
            }
        }
    }

    @Override
    Chunk doNextChunk() {
        if (passNothing) {
            isFinished = true;
            return null;
        }
        if (resultIterator == null) {
            Chunk inputChunk = nextProbeChunk();
            boolean inputIsFinished = false;
            if (inputChunk != null) {
                for (int i = 0; i < aggregators.size(); i++) {
                    inputAggregatorInputs[i] = valueConverters[i].apply(inputChunk);
                }
                // Process outer rows in this input chunk
                probeOperator.calcJoinAgg(inputChunk);
            } else {
                inputIsFinished = getProbeInput().produceIsFinished();
            }
            if (inputIsFinished) {
                //input is already finished.
                if (joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT) {
                    probeOperator.handleNull();
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
                for (int j = 0; j < valueAccumulators.length; j++) {
                    valueAccumulators[j].writeResultTo(groupId++, valueBlockBuilders[j]);
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
                for (int j = 0; j < valueAccumulators.length; j++) {
                    valueAccumulators[j].writeResultTo(keyIndex, valueBlockBuilders[j]);
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

        //set null to deallocate memory
        this.valueAccumulators = null;

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

    private void doAggregate(Chunk[] aggInputChunks, int aggIndex, int position, int groupId) {
        assert aggInputChunks != null;
        valueAccumulators[aggIndex].accumulate(groupId, aggInputChunks[aggIndex], position);
    }

    private void doAggregate(Chunk aggInputChunk, int aggIndex, int position, int groupId) {
        assert aggInputChunk != null;
        valueAccumulators[aggIndex].accumulate(groupId, aggInputChunk, position);
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

    interface GroupJoinProbeOperator {
        void calcJoinAgg(Chunk inputChunk);

        void handleNull();
    }

    class DefaultGroupJoinProbeOperator implements GroupJoinProbeOperator {

        @Override
        public void calcJoinAgg(Chunk inputChunk) {
            final int positionCount = inputChunk.getPositionCount();
            Chunk inputJoinKeyChunk = getProbeKeyChunkGetter().apply(inputChunk);
            final int[] hashVector = inputJoinKeyChunk.hashCodeVector();

            int position = 0;
            boolean isMatching = false;
            boolean matched = false;
            int matchedPosition = LIST_END;

            for (; position < positionCount; position++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(hashVector[position], inputJoinKeyChunk, position);
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
                    buildJoinRow(position, matchedPosition);
                    // checks max1row generated from scalar subquery
                    if (singleJoin && matched && !(ConfigDataMode.isFastMock())) {
                        throw GeneralUtil.nestedException("Scalar subquery returns more than 1 row");
                    }
                    // set matched flag
                    matched = true;
                }
            }
        }

        @Override
        public void handleNull() {
            for (int i = usedKeys.nextClearBit(0), size = outKeyChunks.getPositionCount(); i >= 0 && i < size;
                 i = usedKeys.nextClearBit(i + 1)) {
                buildNullRow(nullChunk, 0, i);
            }
        }

        private void buildNullRow(Chunk inputChunk, int position, int matchedPosition) {
            final Chunk.ChunkRow chunkRow = outerChunks.rowAt(matchedPosition);
            final int key = keys[matchedPosition];
            if (key == LIST_END) {
                keys[matchedPosition] = appendAndIncrementGroup(chunkRow.getChunk(), chunkRow.getPosition());
            }
            for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                doAggregate(inputChunk, aggIndex, position, keys[matchedPosition]);
            }

        }

        private int matchInit(int hashCode, Chunk keyChunk, int position) {
            if (bloomFilter != null && !bloomFilter.mightContainInt(hashCode)) {
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

        private void buildJoinRow(int position, int matchedPosition) {
            usedKeys.set(matchedPosition);
            Chunk[] aggregatorInputs = inputAggregatorInputs;
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
                        .checkDistinct(integerBlockBuilder.build(), aggregatorInputs[aggIndex], position);
                }
                if (isDistinct == null || isDistinct[0]) {
                    //to accumulate the Aggregate function result，here need 'aggregate convert' and 'aggregate accumulator'
                    doAggregate(aggregatorInputs, aggIndex, position, keys[matchedPosition]);
                }
            }
        }
    }

    // Join condition = null
    // distinct set = null
    // singleJoin = false
    class IntGroupJoinProbeOperator implements GroupJoinProbeOperator {
        private int[] sourceArray = new int[chunkLimit];

        // for join
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];

        // for null value
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected boolean hasNull = false;

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];

        // for agg
        protected int[] groupIds = new int[chunkLimit];

        // for GroupKeyBuffer append
        protected int groupKeyBufferArrayIndex = 0;
        protected int[] groupKeyBufferArray = new int[chunkLimit];

        @Override
        public void calcJoinAgg(Chunk inputChunk) {
            // clear state for null values
            hasNull = false;
            nullBitmap.clear();

            final int positionCount = inputChunk.getPositionCount();
            Chunk inputJoinKeyChunk = getProbeKeyChunkGetter().apply(inputChunk);

            Preconditions.checkArgument(inputJoinKeyChunk.getBlockCount() == 1
                && inputJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);

            boolean useHashVector = inputJoinKeyChunk.getBlock(0).cast(IntegerBlock.class).getSelection() != null;
            if (useHashVector) {
                inputJoinKeyChunk.hashCodeVector(probeKeyHashCode, null, null, positionCount);
            }

            // copy array from integer block
            IntegerBlock integerBlock = inputJoinKeyChunk.getBlock(0).cast(IntegerBlock.class);
            integerBlock.copyToIntArray(0, positionCount, sourceArray, 0, null);
            if (integerBlock.mayHaveNull()) {
                // collect nulls if block may have null value.
                integerBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            // clear matched rows for each input chunk.
            matchedRows = 0;

            boolean isMatching = false;
            int matchedPosition = LIST_END;
            for (int position = 0; position < positionCount; position++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = useHashVector
                        ? matchInit(probeKeyHashCode[position], position)
                        : matchInit(position);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, position);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, position)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = position;
                    matchedRows++;
                }
            }

            buildJoinRow();
        }

        @Override
        public void handleNull() {
            // clear group key buffer
            groupKeyBufferArrayIndex = 0;

            int position = 0;
            for (int unmatchedPosition = usedKeys.nextClearBit(0), size = outKeyChunks.getPositionCount();
                 unmatchedPosition >= 0 && unmatchedPosition < size;
                 unmatchedPosition = usedKeys.nextClearBit(unmatchedPosition + 1)) {

                // find and allocate group id
                if (keys[unmatchedPosition] == LIST_END) {
                    // allocate group id.

                    // we should get value in matched position of out key chunks.
                    // It is equal to value in probe position of probe side chunk.
                    int unmatchedValue = outKeyChunks.getInt(0, unmatchedPosition);
                    groupKeyBufferArray[groupKeyBufferArrayIndex++] = unmatchedValue;

                    keys[unmatchedPosition] = groupId++;
                }

                groupIds[position] = keys[unmatchedPosition];
                position++;

                if (position >= chunkLimit) {
                    flushNullChunk(position);
                    position = 0;
                }
            }

            if (position > 0) {
                flushNullChunk(position);
            }
        }

        private void flushNullChunk(int positionCount) {
            // flush group key buffer.
            groupKeyBuffer.appendRow(groupKeyBufferArray, -1, groupKeyBufferArrayIndex);
            groupKeyBufferArrayIndex = 0;

            // accumulate with given group ids.
            for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                valueAccumulators[aggIndex].accumulate(groupIds, nullChunk, positionCount);
            }
        }

        private int matchInit(int position) {
            // not null safe
            if (hasNull && nullBitmap.get(position)) {
                return LIST_END;
            }

            int value = sourceArray[position];
            int matchedPosition = hashTable.get(value);
            while (matchedPosition != LIST_END) {
                if (outKeyChunks.getInt(0, matchedPosition) == value) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchInit(int hashCode, int position) {
            // not null safe
            if (hasNull && nullBitmap.get(position)) {
                return LIST_END;
            }

            int matchedPosition = hashTable.get(hashCode);
            while (matchedPosition != LIST_END) {
                if (outKeyChunks.getInt(0, matchedPosition) == sourceArray[position]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // not null safe
            if (hasNull && nullBitmap.get(position)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (outKeyChunks.getInt(0, matchedPosition) == sourceArray[position]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private void buildJoinRow() {
            for (int i = 0; i < matchedRows; i++) {
                final int position = probePositions[i];
                final int matchedPosition = matchedPositions[i];

                // for outer join output
                usedKeys.set(matchedPosition);

                // find and allocate group id
                if (keys[matchedPosition] == LIST_END) {
                    // allocate group id.

                    // we should get value in matched position of out key chunks.
                    // It is equal to value in probe position of probe side chunk.
                    int matchedValue = sourceArray[position];
                    groupKeyBufferArray[groupKeyBufferArrayIndex++] = matchedValue;

                    keys[matchedPosition] = groupId++;
                }

                groupIds[position] = keys[matchedPosition];
            }

            // flush group key buffer.
            groupKeyBuffer.appendRow(groupKeyBufferArray, -1, groupKeyBufferArrayIndex);
            groupKeyBufferArrayIndex = 0;

            // A special agg for group join
            // the probe positions array may have repeated elements like {0, 0, 1, 1, 1, 2, 5, 5, 7 ...}
            for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                valueAccumulators[aggIndex].accumulate(groupIds, inputAggregatorInputs[aggIndex], probePositions,
                    matchedRows);
            }
        }
    }
}
