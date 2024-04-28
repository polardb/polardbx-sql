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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.accumulator.Accumulator;
import com.alibaba.polardbx.executor.accumulator.AccumulatorBuilders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class AggOpenHashMap extends GroupOpenHashMap implements AggHashMap {

    private final List<Aggregator> aggregators;

    protected List<Chunk> groupChunks;

    protected List<Chunk> valueChunks;

    protected final BlockBuilder[] valueBlockBuilders;

    private ChunkConverter[] valueConverters;

    private Accumulator[] valueAccumulators;

    private int[] filterArgs;

    private DistinctSet[] distinctSets;

    private DataType[] aggValueType;

    private GroupBy groupBy;

    private final OperatorMemoryAllocatorCtx memoryAllocator;

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, int chunkSize, ExecutionContext context,
                          OperatorMemoryAllocatorCtx memoryAllocator) {
        this(groupKeyType, aggregators, aggValueType, inputType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize, context,
            memoryAllocator);
    }

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, float loadFactor, int chunkSize,
                          ExecutionContext context, OperatorMemoryAllocatorCtx memoryAllocator) {
        super(groupKeyType, expectedSize, loadFactor, chunkSize, context);

        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.aggregators = aggregators;
        this.memoryAllocator = memoryAllocator;

        this.valueAccumulators = new Accumulator[aggregators.size()];

        this.valueConverters = new ChunkConverter[aggregators.size()];
        this.filterArgs = new int[aggregators.size()];
        this.distinctSets = new DistinctSet[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            final Aggregator aggregator = aggregators.get(i);
            valueAccumulators[i] =
                AccumulatorBuilders.create(aggregator, aggValueType[i], inputType, expectedSize, context);

            DataType[] originalInputTypes = DataTypeUtils.gather(inputType, aggregator.getInputColumnIndexes());
            DataType[] accumulatorInputTypes = Util.first(valueAccumulators[i].getInputTypes(), originalInputTypes);
            valueConverters[i] =
                Converters.createChunkConverter(aggregator.getInputColumnIndexes(), inputType, accumulatorInputTypes,
                    context);
            filterArgs[i] = aggregator.getFilterArg();
            if (aggregator.isDistinct()) {
                int[] distinctIndexes = aggregator.getNewForAccumulator().getAggTargetIndexes();
                distinctSets[i] =
                    new DistinctSet(accumulatorInputTypes, distinctIndexes, expectedSize, chunkSize, context);
            }
        }

        this.aggValueType = aggValueType;
        this.valueBlockBuilders = new BlockBuilder[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            valueBlockBuilders[i] = BlockBuilders.create(aggValueType[i], context);
        }

        if (noGroupBy()) {
            appendGroup(new Chunk(1), 0); // add an empty chunk
        }

        // Prerequisites:
        // 1. ENABLE_VEC_ACCUMULATOR=true
        // 2. group by column is not empty.
        // 3. has no distinct keyword in any aggregator.
        // 4. has no filter args in any aggregator.
        boolean enableVecAccumulator = context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_ACCUMULATOR)
            && groupKeyType.length > 0
            && aggregators.stream().allMatch(aggregator -> !aggregator.isDistinct() && aggregator.getFilterArg() < 0);

        // check if group keys consist of (int, int), (varchar, varchar), (int, varchar), (varchar ,int)
        // and don't use compatible mode.
        boolean groupKeyIntegerAndSlice = groupKeyType != null
            && groupKeyType.length == 2 && !context.isEnableOssCompatible()
            && ((groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof SliceType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof SliceType)
        );

        // group by long
        boolean singleGroupKeyLong = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof LongType;

        // group by int
        boolean singleGroupKeyInteger = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof IntegerType;

        if (enableVecAccumulator && groupKeyIntegerAndSlice) {
            this.groupBy = new SliceIntBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyLong) {
            this.groupBy = new LongBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyInteger) {
            this.groupBy = new IntBatchGroupBy();
        } else {
            this.groupBy = new DefaultGroupBy();
        }
        // for fixed memory cost of GroupBy objects.
        memoryAllocator.allocateReservedMemory(groupBy.fixedEstimatedSize());
    }

    protected final static int GROUP_ID_COUNT_THRESHOLD = 16;
    protected final static long SERIALIZED_MASK = ((long) 0x7fffffff) << 1 | 1;

    private class IntBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected int[] sourceArray = new int[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected boolean inOrder;

        protected int[] key;
        protected int[] value;
        protected int mask;

        // The key=0 is stored in the last position in hash table.
        protected boolean containsZeroKey;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected int groupIdOfNull;

        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        public IntBatchGroupBy() {
            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.groupIdOfNull = -1;

            this.f = loadFactor;
            final int expected = expectedSize;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be non-negative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(n + 1));
                    this.key = new int[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;

            // get input chunks for aggregators.
            Chunk[] aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into int/long array
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(0, positionCount, sourceArray, 0, null);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            // 2.1 build hash table
            // 2.2 check in-order
            putHashTable(positionCount);

            // step 3. accumulator with selection array.
            int groupCount = getGroupCount();
            if (inOrder) {
                // CASE 1. (best case) the long value of key block is in order.
                int groupId = groupIds[0];
                int startIndex = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (groupIds[i] != groupId) {
                        // accumulate in range.
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, i);
                        }

                        // update for the next range
                        startIndex = i;
                        groupId = groupIds[i];
                    }
                }

                // for the rest range
                if (startIndex < positionCount) {
                    for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, positionCount);
                    }
                }

            } else if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                // CASE 2. (good case) the ndv is small.

                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                        }
                    }
                }
            } else {
                // Normal execution mode.
                for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                    valueAccumulators[aggIndex]
                        .accumulate(groupIds, aggregatorInputs[aggIndex], positionCount);
                }
            }

            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return (chunkSize * Integer.BYTES) * 3
                + SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3 + Float.BYTES;
        }

        public void putHashTable(int positionCount) {
            if (!hasNull) {
                long lastVal = Long.MIN_VALUE;
                for (int position = 0; position < positionCount; position++) {
                    inOrder &= (sourceArray[position] >= lastVal);
                    lastVal = sourceArray[position];
                }
            }

            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(sourceArray, position);
            }
        }

        private int findGroupId(int[] sourceArray, final int position) {

            if (hasNull && nullBitmap.get(position)) {
                if (groupIdOfNull == -1) {
                    // put null value in the first time.
                    groupIdOfNull = allocateGroupId(sourceArray, position, true);
                }
                return groupIdOfNull;
            }

            int k = sourceArray[position];
            int pos;
            if (k == 0) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                int[] key = this.key;
                int curr;
                if ((curr = key[pos = HashCommon.mix(k) & this.mask]) != 0) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(sourceArray, position, false);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        // It's OK if element in this position is null.
        private int allocateGroupId(int[] sourceArray, final int position, boolean isNull) {
            // use groupCount as group value array index.
            if (isNull) {
                groupKeyBuffer.appendNull(0);
            } else {
                groupKeyBuffer.appendInteger(0, sourceArray[position]);
            }

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregators.size(); i++) {
                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            int[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(newN + 1));

            int[] newKey = new int[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0);

                if (newKey[pos = HashCommon.mix(key[i]) & mask] != 0) {
                    while (newKey[pos = pos + 1 & mask] != 0) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
            this.value = newValue;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            sourceArray = null;
            groupIdSelection = null;
        }
    }

    private class LongBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected long[] sourceArray = new long[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected boolean inOrder;

        protected long[] key;
        protected int[] value;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected int groupIdOfNull;

        public LongBatchGroupBy() {
            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.groupIdOfNull = -1;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(n + 1));
                    this.key = new long[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;

            Chunk[] aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into long arrays
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(0, positionCount, sourceArray, 0);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            // 2.1 build hash table
            // 2.2 check in-order
            putHashTable(positionCount);

            // step 3. accumulator with selection array.
            int groupCount = getGroupCount();
            if (inOrder) {
                // CASE 1. (best case) the long value of key block is in order.

                int groupId = groupIds[0];
                int startIndex = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (groupIds[i] != groupId) {
                        // accumulate in range.
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, i);
                        }

                        // update for the next range
                        startIndex = i;
                        groupId = groupIds[i];
                    }
                }

                // for the rest range
                if (startIndex < positionCount) {
                    for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, positionCount);
                    }
                }

            } else if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                // CASE 2. (good case) the ndv is small.

                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                        }
                    }
                }
            } else {
                // Normal execution mode.
                for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                    valueAccumulators[aggIndex]
                        .accumulate(groupIds, aggregatorInputs[aggIndex], positionCount);
                }
            }
            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        public void putHashTable(int positionCount) {
            if (!hasNull) {
                long lastVal = Long.MIN_VALUE;
                for (int position = 0; position < positionCount; position++) {
                    inOrder &= (sourceArray[position] >= lastVal);
                    lastVal = sourceArray[position];
                }
            }

            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(sourceArray, position);
            }
        }

        private int findGroupId(long[] sourceArray, final int position) {
            if (hasNull && nullBitmap.get(position)) {
                if (groupIdOfNull == -1) {
                    // put null value in the first time.
                    groupIdOfNull = allocateGroupId(sourceArray, position, true);
                }
                return groupIdOfNull;
            }

            long k = sourceArray[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(sourceArray, position, false);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        private int allocateGroupId(long[] sourceArray, int position, boolean isNull) {
            // use groupCount as group value array index.
            if (isNull) {
                groupKeyBuffer.appendNull(0);
            } else {
                groupKeyBuffer.appendLong(0, sourceArray[position]);
            }

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregators.size(); i++) {
                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(newN + 1));
            long[] newKey = new long[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
            this.value = newValue;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            sourceArray = null;
            groupIdSelection = null;
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Integer.BYTES) * 2
                + (chunkSize * Long.BYTES)
                + +SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES;
        }
    }

    /**
     * Handle slice-slice, slice-int or int-slice type group by.
     * It can fall back to DefaultGroupBy if some blocks are not in dictionary.
     */
    private class SliceIntBatchGroupBy implements GroupBy {
        IntIntBatchGroupBy dictIntBatchGroupBy;
        DefaultGroupBy normalGroupBy;

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            if (normalGroupBy != null) {
                // Already fall back to normal group by.
                normalGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            } else if ((keyChunk.getBlock(0) instanceof SliceBlock
                && ((SliceBlock) keyChunk.getBlock(0)).getDictionary() == null)
                || (keyChunk.getBlock(1) instanceof SliceBlock
                && ((SliceBlock) keyChunk.getBlock(1)).getDictionary() == null)) {
                // if any block is a slice block but don't have dictionary.

                if (normalGroupBy == null) {
                    normalGroupBy = new DefaultGroupBy();
                }

                if (dictIntBatchGroupBy != null) {
                    // fall back IntIntBatchGroupBy to DefaultGroupBy
                    normalGroupBy.fillGroupKeyBuffer();
                    dictIntBatchGroupBy.close();
                    dictIntBatchGroupBy = null;
                }

                // Just put into DefaultGroupBy.
                normalGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            } else {
                // Good Case: use dictionary for slice block group-by.
                if (dictIntBatchGroupBy == null) {
                    dictIntBatchGroupBy = new IntIntBatchGroupBy();
                }
                dictIntBatchGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            if (normalGroupBy != null) {
                return normalGroupBy.fixedEstimatedSize();
            } else if (dictIntBatchGroupBy != null) {
                return dictIntBatchGroupBy.fixedEstimatedSize();
            }
            return 0;
        }

        @Override
        public void close() {
            if (normalGroupBy != null) {
                normalGroupBy.close();
            }
            if (dictIntBatchGroupBy != null) {
                dictIntBatchGroupBy.close();
            }
        }
    }

    private class IntIntBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected int[] intBlock1 = new int[chunkSize];
        protected int[] intBlock2 = new int[chunkSize];
        protected long[] serializedBlock = new long[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected DictionaryMapping dictionaryMapping1 = DictionaryMapping.create();
        protected DictionaryMapping dictionaryMapping2 = DictionaryMapping.create();

        protected long[] key;
        protected int[] value;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // handle null value.
        // It's for pair of key: (null, xxx) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        Int2IntOpenHashMap keyMap1;
        boolean hasNull1;
        BitSet nullBitmap1;

        // It's for pair of key: (xxx, null) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        Int2IntOpenHashMap keyMap2;
        boolean hasNull2;
        BitSet nullBitmap2;

        // The groupId of key: (null, null).
        int groupIdOfDoubleNull;

        public IntIntBatchGroupBy() {
            keyMap1 = null;
            keyMap2 = null;
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1 = new BitSet(chunkSize);
            nullBitmap2 = new BitSet(chunkSize);
            groupIdOfDoubleNull = -1;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(
                        SizeOf.sizeOfIntArray(n + 1) + SizeOf.sizeOfLongArray(n + 1));
                    this.key = new long[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            // clear null state
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1.clear();
            nullBitmap2.clear();

            Chunk[] aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into arrays
            // step 2. serialize to long
            // step 3. long type-specific hash, and put group value when first hit.
            // step 4. accumulator with selection array.

            // step 1. copy blocks into arrays
            Block keyBlock1 = keyChunk.getBlock(0).cast(Block.class);
            Block keyBlock2 = keyChunk.getBlock(1).cast(Block.class);

            keyBlock1.copyToIntArray(0, positionCount, intBlock1, 0, dictionaryMapping1);
            keyBlock2.copyToIntArray(0, positionCount, intBlock2, 0, dictionaryMapping2);

            // collect null value for all key blocks.
            if (keyBlock1.mayHaveNull()) {
                keyBlock1.collectNulls(0, positionCount, nullBitmap1, 0);
                hasNull1 = !nullBitmap1.isEmpty();
            }
            if (keyBlock2.mayHaveNull()) {
                keyBlock2.collectNulls(0, positionCount, nullBitmap2, 0);
                hasNull2 = !nullBitmap2.isEmpty();
            }

            // DictMapping.merge(dict)
            // int[] remapping = DictMapping.get(hashCode)
            // int newDictId = remapping[dictId]

            // step 2. serialize to long
            // ((long) key1 << 32) | (key2 & serializedMask);
            for (int i = 0; i < positionCount; i++) {
                serializedBlock[i] = ((long) (intBlock1[i]) << 32) | ((intBlock2[i]) & SERIALIZED_MASK);
            }

            // step 3. long type-specific hash
            putHashTable(keyChunk, positionCount);

            // step 4. accumulator with selection array.
            int groupCount = getGroupCount();
            if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                        }
                    }
                }
            } else {
                // Fall back to row-by-row execution mode.
                for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                    for (int pos = 0; pos < positionCount; pos++) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupIds[pos], aggregatorInputs[aggIndex], pos);
                    }
                }
            }
            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return (chunkSize * Integer.BYTES) * 4
                + (chunkSize * Long.BYTES)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES
                + dictionaryMapping1.estimatedSize()
                + dictionaryMapping2.estimatedSize();
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            intBlock1 = null;
            intBlock2 = null;
            serializedBlock = null;
            groupIdSelection = null;
            dictionaryMapping1.close();
            dictionaryMapping2.close();
            dictionaryMapping1 = null;
            dictionaryMapping2 = null;

            if (keyMap1 != null) {
                keyMap1.clear();
                keyMap1 = null;
            }

            if (keyMap2 != null) {
                keyMap2.clear();
                keyMap2 = null;
            }
        }

        public void putHashTable(Chunk keyChunk, int positionCount) {
            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(serializedBlock, keyChunk, position);
            }
        }

        private int findGroupId(long[] serializedBlock, Chunk keyChunk, final int position) {

            if (hasNull1 || hasNull2) {
                // handle null.
                if (hasNull1 && hasNull2 && nullBitmap1.get(position) && nullBitmap2.get(position)) {
                    // case1: both keys are null.
                    if (groupIdOfDoubleNull == -1) {
                        // allocate new group id
                        groupIdOfDoubleNull = allocateGroupId(keyChunk, position);
                    }
                    return groupIdOfDoubleNull;
                } else if (hasNull1 && nullBitmap1.get(position)) {
                    // case2: key of (null, xxx)
                    if (keyMap1 == null) {
                        // initialize keyMap1.
                        keyMap1 = new Int2IntOpenHashMap();
                        keyMap1.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock2[position];
                    int groupId;
                    if ((groupId = keyMap1.get(intVal)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap1.put(intVal, groupId);
                    }
                    return groupId;
                } else if (hasNull2 && nullBitmap2.get(position)) {
                    // case3: key of (xxx, null)
                    if (keyMap2 == null) {
                        // initialize keyMap1.
                        keyMap2 = new Int2IntOpenHashMap();
                        keyMap2.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock1[position];
                    int groupId;
                    if ((groupId = keyMap2.get(intVal)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap2.put(intVal, groupId);
                    }
                    return groupId;
                }

                // case 4: the key pair in this position is not (null, null), (null, xxx) or (xxx, null)
            }

            long k = serializedBlock[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(keyChunk, position);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        private int allocateGroupId(Chunk keyChunk, int position) {
            // use groupCount as group value array index.
            groupKeyBuffer.appendRow(keyChunk, position);

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregators.size(); i++) {
                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(newN + 1) + SizeOf.sizeOfLongArray(newN + 1));
            long[] newKey = new long[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
            this.value = newValue;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

    }

    private class DefaultGroupBy implements GroupBy {
        /**
         * The array of keys (buckets)
         */
        protected int[] keys;
        /**
         * The mask for wrapping a position counter
         */
        protected int mask;
        /**
         * The current table size.
         */
        protected int n;
        /**
         * Number of entries in the set (including the key zero, if present).
         */
        protected int size;
        /**
         * The acceptable load factor.
         */
        protected float f;
        /**
         * Threshold after which we rehash. It must be the table size times {@link #f}.
         */
        protected int maxFill;

        public DefaultGroupBy() {
            this.f = loadFactor;
            this.n = HashCommon.arraySize(expectedSize, loadFactor);
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, loadFactor);
            this.size = 0;

            // large memory allocation: hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Chunk[] aggregatorInputs;
            aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

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
                Chunk aggInputChunk = aggregatorInputs[aggIndex];
                boolean[] isDistinct = null;
                if (distinctSets[aggIndex] != null) {
                    isDistinct = distinctSets[aggIndex].checkDistinct(groupIdBlock, aggregatorInputs[aggIndex]);
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
                            valueAccumulators[aggIndex].accumulate(groupIds[pos], aggInputChunk, pos);
                        }
                    }
                }
            }

            if (groupIdResult != null) {
                for (int i = 0; i < groupIds.length; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The default implementation of GroupBy use dynamic memory allocation.
            return Integer.BYTES * 4 + Float.BYTES;
        }

        @Override
        public void close() {
            this.keys = null;
        }

        /**
         * @param groupId if groupId == -1 means need to generate a new groupid
         */
        int innerPut(Chunk chunk, int position, int groupId) {
            return doInnerPutArray(chunk, position, groupId);
        }

        /**
         * Fill the elements from GroupKeyBuffer into hash table of this object,
         * but don't allocate new Group ID.
         * <p>
         * This is only for fall-back of other implementation of group-by.
         */
        public void fillGroupKeyBuffer() {
            List<Chunk> groupKeyChunks = groupKeyBuffer.buildChunks();

            // avoid rehash.
            final int currentSize = groupCount;
            if (currentSize > expectedSize) {
                this.n = HashCommon.arraySize(currentSize, loadFactor);
                this.mask = n - 1;
                this.maxFill = HashCommon.maxFill(n, loadFactor);
                this.size = 0;

                int[] keys = new int[n];
                Arrays.fill(keys, NOT_EXISTS);
                this.keys = keys;
            }

            // avoid to allocate group id.
            int groupId = 0;
            for (Chunk chunk : groupKeyChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }

        private int doInnerPutArray(Chunk chunk, int position, int groupId) {
            int h = HashCommon.mix(chunk.hashCode(position)) & mask;
            int k = keys[h];

            if (k != NOT_EXISTS) {
                if (groupKeyBuffer.equals(k, chunk, position)) {
                    return k;
                }
                // Open-address probing
                while ((k = keys[h = (h + 1) & mask]) != NOT_EXISTS) {
                    if (groupKeyBuffer.equals(k, chunk, position)) {
                        return k;
                    }
                }
            }

            // 去重，仅在保留第一次命中时的group value
            if (groupId == -1) {
                groupId = appendGroup(chunk, position);
            }

            // otherwise, insert this position
            keys[h] = groupId;

            if (size++ >= maxFill) {
                rehash();
            }
            return groupId;
        }

        protected void rehash() {
            this.n *= 2;
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, this.f);
            this.size = 0;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;

            List<Chunk> groupChunks = groupKeyBuffer.buildChunks();
            int groupId = 0;
            for (Chunk chunk : groupChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }
    }

    @Override
    public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
        groupBy.putChunk(keyChunk, inputChunk, groupIdResult);
    }

    @Override
    int appendGroup(Chunk chunk, int position) {
        int groupId = super.appendGroup(chunk, position);

        // Also add an initial value to accumulators
        for (int i = 0; i < aggregators.size(); i++) {
            valueAccumulators[i].appendInitValue();
        }
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
            for (int i = 0; i < valueAccumulators.length; i++) {
                valueAccumulators[i].writeResultTo(groupId, valueBlockBuilders[i]);
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

        // set null to deallocate memory
        this.valueAccumulators = null;

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

    @Override
    List<Chunk> buildGroupChunks() {
        List<Chunk> result = super.buildGroupChunks();
        if (groupBy != null) {
            groupBy.close();
            groupBy = null;
        }

        return result;
    }

    public WorkProcessor<Chunk> buildHashSortedResult() {
        return buildResult(hashSortedGroupIds());
    }

    private IntIterator hashSortedGroupIds() {
        this.groupChunks = groupKeyBuffer.buildChunks();
        if (this.groupBy != null) {
            this.groupBy.close();
        }
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
                for (int i = 0; i < valueAccumulators.length; i++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(groupKeyType.length + i);
                    valueAccumulators[i].writeResultTo(groupId, output);
                }
            }

            return WorkProcessor.ProcessState.ofResult(pageBuilder.build());
        });
    }

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
            for (int i = 0; i < aggregators.size(); i++) {
                size += valueAccumulators[i].estimateSize();
            }
        }
        return size;
    }
}
