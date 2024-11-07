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

import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.BitSet;
import java.util.List;

/**
 * Hash Join Executor
 *
 */
public abstract class AbstractHashJoinExec extends AbstractBufferedJoinExec implements ConsumerExecutor {

    /**
     * A placeholder to mark there is no more element in this position link
     */
    public static final int LIST_END = ConcurrentRawHashTable.NOT_EXISTS;
    private static final Logger logger = LoggerFactory.getLogger(AbstractHashJoinExec.class);
    ConcurrentRawHashTable hashTable;
    int[] positionLinks;
    ConcurrentIntBloomFilter bloomFilter;

    public AbstractHashJoinExec(Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeys,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                ExecutionContext context) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null,
            context);
    }

    @Override
    protected void createBlockBuilders() {
        if (!useVecJoin || !enableVecBuildJoinRow) {
            super.createBlockBuilders();
            return;
        }
        // Create all block builders by default
        final List<DataType> columns = getDataTypes();
        blockBuilders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blockBuilders[i] = BatchBlockWriter.create(columns.get(i), context, chunkLimit);
        }
    }

    @Override
    public void closeConsume(boolean force) {
        buildChunks = null;
        buildKeyChunks = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    void doClose() {
        getProbeInput().close();
        closeConsume(true);

        this.hashTable = null;
        this.positionLinks = null;

        if (probeOperator != null) {
            probeOperator.close();
        }
    }

    @Override
    int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
        int hashCode = hashCodes[position];
        if (bloomFilter != null && !bloomFilter.mightContainInt(hashCode)) {
            return LIST_END;
        }

        int matchedPosition = hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    boolean matchValid(int current) {
        return current != LIST_END;
    }

    /**
     * get the condition index of BuildChunk for TypedHashTable,
     * should check isScalarInputRefCondition before calling this method
     */
    protected int getBuildChunkConditionIndex() {
        List<IExpression> args = ((ScalarFunctionExpression) condition).getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();

        if (buildOuterInput) {
            // since this is outer build, build chunk is on the left side and has a smaller index
            return Math.min(idx1, idx2);
        } else {
            int buildIndex = Math.max(idx1, idx2);
            // since this is inner build, build chunk is on the right side
            return buildIndex - outerInput.getDataTypes().size();
        }
    }

    /**
     * get the condition index of ProbeChunk for TypedHashTable,
     * should check isScalarInputRefCondition before calling this method
     */
    protected int getProbeChunkConditionIndex() {
        List<IExpression> args = ((ScalarFunctionExpression) condition).getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();

        if (buildOuterInput) {
            // since this is outer build, the probe index is on the right side
            int probeIndex = Math.max(idx1, idx2);
            // since this is outer build (reverse), the probe index is on the right side
            return probeIndex - outerInput.getDataTypes().size();
        } else {
            // since this is inner build, the probe index is on the left side
            return Math.min(idx1, idx2);
        }
    }

    class MultiIntProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        protected final int keySize;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[][] valueArray;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected long[] serializedValues;
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected MultiIntProbeOperator(int keySize, boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            this.keySize = keySize;
            this.valueArray = new int[keySize][chunkLimit];
            // the width for comparison is in (keySize + 1) / 2 * 64 bit.
            this.serializedValues = new long[chunkLimit * ((keySize + 1) / 2)];
        }

        @Override
        public void close() {
            valueArray = null;
            serializedValues = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * keySize + Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == keySize);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;

            // copy array from long block, and collect null values.
            nullBitmap.clear();
            for (int keyCol = 0; keyCol < keySize; keyCol++) {
                Block keyBlock = probeJoinKeyChunk.getBlock(keyCol).cast(Block.class);

                keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition,
                    valueArray[keyCol], 0, null);

                // collect null from all blocks.
                hasNull |= keyBlock.mayHaveNull();
                if (keyBlock.mayHaveNull()) {
                    keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
                }
            }

            // Build serialized value:
            // for example, if keySize = 4, the serialized value array =
            // {(array1[0], array2[0]),  (array3[0], array4[0]), (array1[1], array2[1]),  (array3[1], array4[1]) ... }
            int serializedValuesIndex = 0;
            for (int i = 0; i < positionCount - startProbePosition; i++) {
                if (keySize % 2 == 0) {
                    // when the count of key columns is even number
                    for (int keyCol = 0; keyCol < keySize; keyCol++) {
                        serializedValues[serializedValuesIndex++] =
                            TypedListHandle.serialize(valueArray[keyCol][i], valueArray[keyCol + 1][i]);
                        keyCol++;
                    }
                } else {
                    // when the count of key columns is odd number
                    for (int keyCol = 0; keyCol < keySize - 1; keyCol++) {
                        serializedValues[serializedValuesIndex++] =
                            TypedListHandle.serialize(valueArray[keyCol][i], valueArray[keyCol + 1][i]);
                        keyCol++;
                    }
                    // for the last column
                    serializedValues[serializedValuesIndex++] =
                        TypedListHandle.serialize(valueArray[keySize - 1][i], 0);
                }

            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(buildChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(buildChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(buildChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }

            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();

            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {
                    int keyColumnIndex = innerKeyMapping[i];
                    IntegerBlock integerBlock = new IntegerBlock(0, chunkLimit, null, valueArray[keyColumnIndex]);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(integerBlock, probePositions,
                        -startProbePosition, matchedRows);
                } else {

                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }

                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            if (keySize == 1 || keySize == 2) {
                // find matched positions for each row.
                int matchedPosition = hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size = 1 or 2, the number for comparison is in 64bit.
                    if (serializedValues[position - startProbePosition] == buildKeyChunks.getLong(0, matchedPosition)) {
                        break;
                    }

                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else if (keySize == 3 || keySize == 4) {
                // find matched positions for each row.
                int matchedPosition = hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size = 3 or 4, the number for comparison is in 128bit.
                    if (serializedValues[(position - startProbePosition) * 2]
                        == buildKeyChunks.getLong(0, matchedPosition * 2)
                        && serializedValues[(position - startProbePosition) * 2 + 1]
                        == buildKeyChunks.getLong(0, matchedPosition * 2 + 1)) {
                        break;
                    }

                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else {

                // find matched positions for each row.
                int matchedPosition = hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size > 4, the number for comparison is in (keySize + 1) / 2 * 64 bit.
                    int multiple = (keySize + 1) / 2;
                    boolean matched = true;

                    for (int i = 0; i < multiple; i++) {
                        matched &= serializedValues[(position - startProbePosition) * multiple + i]
                            == buildKeyChunks.getLong(0, matchedPosition * multiple + i);
                    }

                    if (matched) {
                        break;
                    }

                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;

            }

        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            if (keySize == 1 || keySize == 2) {
                int matchedPosition = positionLinks[current];
                while (matchedPosition != LIST_END) {
                    // for key size = 1 or 2, the number for comparison is in 64bit.
                    if (serializedValues[position - startProbePosition] == buildKeyChunks.getLong(0, matchedPosition)) {
                        break;
                    }

                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else if (keySize == 3 || keySize == 4) {
                int matchedPosition = positionLinks[current];
                while (matchedPosition != LIST_END) {
                    if (serializedValues[(position - startProbePosition) * 2]
                        == buildKeyChunks.getLong(0, matchedPosition * 2)
                        && serializedValues[(position - startProbePosition) * 2 + 1]
                        == buildKeyChunks.getLong(0, matchedPosition * 2 + 1)) {
                        break;
                    }
                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else {

                // find matched positions for each row.
                int matchedPosition = positionLinks[current];
                while (matchedPosition != LIST_END) {
                    // for key size > 4, the number for comparison is in (keySize + 1) / 2 * 64 bit.
                    int multiple = (keySize + 1) / 2;
                    boolean matched = true;

                    for (int i = 0; i < multiple; i++) {
                        matched &= serializedValues[(position - startProbePosition) * multiple + i]
                            == buildKeyChunks.getLong(0, matchedPosition * multiple + i);
                    }

                    if (matched) {
                        break;
                    }

                    matchedPosition = positionLinks[matchedPosition];
                }
                return matchedPosition;

            }
        }
    }

    class IntProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        // for probe keys
        protected int[] valueArray = new int[chunkLimit];
        // for null values of probe keys.
        protected boolean hasNull = false;

        // protected int[] matchedValues = new int[chunkLimit];
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected IntProbeOperator(boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
        }

        @Override
        public void close() {
            valueArray = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 5;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            boolean useHashCodeVector = probeJoinKeyChunk.getBlock(0).cast(IntegerBlock.class).getSelection() != null;
            if (useHashCodeVector) {
                probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);
            }

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, valueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = useHashCodeVector
                        ? matchInit(probeKeyHashCode, probePosition)
                        : matchInit(probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(buildChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(buildChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(buildChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {

                    IntegerBlock integerBlock = new IntegerBlock(0, chunkLimit, null, valueArray);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(integerBlock, probePositions,
                        -startProbePosition, matchedRows);

                } else {
                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int value = valueArray[position - startProbePosition];
            // find matched positions for each row.
            int matchedPosition = hashTable.get(value);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == value) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }
    }

    class LongProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] valueArray = new long[chunkLimit];
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected LongProbeOperator(boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
        }

        @Override
        public void close() {
            valueArray = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;

            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, valueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(buildChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(buildChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(buildChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }
            // single join only output the first row of right side

            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {

                    LongBlock longBlock = new LongBlock(0, chunkLimit, null, valueArray);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(longBlock, probePositions,
                        -startProbePosition, matchedRows);
                } else {
                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }

                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }

                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }
    }

    class ReverseAntiProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];

        protected ReverseAntiProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);

                for (;
                     matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {
                    if (!checkJoinCondition(buildChunks, probeChunk, probePosition, matchedPosition)) {
                        continue;
                    }

                    synchronizer.getMatchedPosition().rawMark(matchedPosition);
                }
            }
        }

        int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
            int hashCode = hashCodes[position];

            int matchedPosition = hashTable.get(hashCode);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (!synchronizer.getMatchedPosition().hasSet(matchedPosition) && buildKeyChunks.equals(matchedPosition,
                    keyChunk, position)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, Chunk keyChunk, int position) {
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (!synchronizer.getMatchedPosition().hasSet(matchedPosition) && buildKeyChunks.equals(matchedPosition,
                    keyChunk, position)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {

        }

        @Override
        public int estimateSize() {
            // no extra memory usage.
            return 0;
        }
    }

    class SemiLongProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongProbeOperator(boolean enableVecBuildJoinRow) {
            Preconditions.checkArgument(antiJoinOperands == null);
            Preconditions.checkArgument(condition == null);

            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            this.longValueArray = new long[chunkLimit];
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                if (matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 3 + Long.BYTES * chunkLimit;
        }
    }

    /**
     * 1. semi/anti join
     * 2. long = long and int <> int
     */
    class SemiLongNotEqIntegerProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        protected final boolean isAnti;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongNotEqIntegerProbeOperator(boolean enableVecBuildJoinRow) {
            if (joinType == JoinRelType.SEMI) {
                this.isAnti = false;
            } else if (joinType == JoinRelType.ANTI) {
                this.isAnti = true;
            } else {
                throw new UnsupportedOperationException("JoinType not supported: " + joinType);
            }
            Preconditions.checkArgument(antiJoinOperands == null);
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;

            this.longValueArray = new long[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < outerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                // (!isAnti && matched) || (isAnti && !matched)
                if (isAnti ^ matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }
    }

    /**
     * 1. semi/anti join
     * 2. long = long and long <> long
     */
    class SemiLongNotEqLongProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        protected final boolean isAnti;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected long[] longValueArray2;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongNotEqLongProbeOperator(boolean enableVecBuildJoinRow) {
            if (joinType == JoinRelType.SEMI) {
                this.isAnti = false;
            } else if (joinType == JoinRelType.ANTI) {
                this.isAnti = true;
            } else {
                throw new UnsupportedOperationException("JoinType not supported: " + joinType);
            }
            Preconditions.checkArgument(antiJoinOperands == null);
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;

            this.longValueArray = new long[chunkLimit];
            this.longValueArray2 = new long[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < outerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof LongBlock,
                "Probe condition block should be LongBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray2, 0);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                // (!isAnti && matched) || (isAnti && !matched)
                if (isAnti ^ matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getLong(0, matchedPosition) != longValueArray2[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            longValueArray2 = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }
    }

    class ReverseSemiLongProbeOperator implements ProbeOperator {
        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiLongProbeOperator(Synchronizer synchronizer) {
            Preconditions.checkArgument(condition == null,
                "simple reverse semi probe operator not support other join condition");
            this.synchronizer = synchronizer;
            this.longValueArray = new long[chunkLimit];
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match or mark failed, just return
                if (!matchValid(matchedPosition) || !synchronizer.getMatchedPosition().markAndGet(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    buildReverseSemiJoinRow(buildChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit + Long.BYTES * chunkLimit;
        }
    }

    class ReverseSemiIntProbeOperator implements ProbeOperator {
        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiIntProbeOperator(Synchronizer synchronizer) {
            Preconditions.checkArgument(condition == null,
                "simple reverse semi probe operator not support other join condition");
            this.synchronizer = synchronizer;
            this.intValueArray = new int[chunkLimit];
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match or mark failed, just return
                if (!matchValid(matchedPosition) || !synchronizer.getMatchedPosition().markAndGet(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    buildReverseSemiJoinRow(buildChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit + Long.BYTES * chunkLimit;
        }
    }

    class ReverseSemiLongNotEqIntegerProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiLongNotEqIntegerProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
            Preconditions.checkArgument(antiJoinOperands == null);

            this.longValueArray = new long[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match, just return
                if (!matchValid(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // if cas failed, another thread has output this record, but cannot stop
                    if (!synchronizer.getMatchedPosition().markAndGet(matchedPosition)) {
                        continue;
                    }

                    buildReverseSemiJoinRow(buildChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }
    }

    class ReverseSemiIntNotEqIntegerProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray2;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiIntNotEqIntegerProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
            Preconditions.checkArgument(antiJoinOperands == null);

            this.intValueArray2 = new int[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray2, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match, just return
                if (!matchValid(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // if cas failed, another thread has output this record, but cannot stop
                    if (!synchronizer.getMatchedPosition().markAndGet(matchedPosition)) {
                        continue;
                    }

                    buildReverseSemiJoinRow(buildChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray2[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray2[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray2 = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }
    }

    class ReverseAntiIntegerProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiIntegerProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
            this.intValueArray = new int[chunkLimit];
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);
                // if cas failed, another thread has marked all matched records
                if (!matchValid(matchedPosition) || !synchronizer.getMatchedPosition().markAndGet(matchedPosition)) {
                    continue;
                }

                for (;
                     matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    synchronizer.getMatchedPosition().rawMark(matchedPosition);
                }
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (buildKeyChunks.getInt(0, matchedPosition) == intValueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4;
        }

    }

    class ReverseAntiLongNotEqIntegerProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiLongNotEqIntegerProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
            Preconditions.checkArgument(antiJoinOperands == null);

            this.longValueArray = new long[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    synchronizer.getMatchedPosition().rawMark(matchedPosition);
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition] &&
                    !synchronizer.getMatchedPosition().hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (buildKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition] &&
                    !synchronizer.getMatchedPosition().hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }
    }

    class ReverseAntiIntNotEqIntegerProbeOperator implements ProbeOperator {

        protected final Synchronizer synchronizer;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] int2ValueArray;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiIntNotEqIntegerProbeOperator(Synchronizer synchronizer) {
            this.synchronizer = synchronizer;
            Preconditions.checkArgument(antiJoinOperands == null);

            this.int2ValueArray = new int[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, int2ValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    synchronizer.getMatchedPosition().rawMark(matchedPosition);
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return buildChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (buildKeyChunks.getInt(0, matchedPosition) == int2ValueArray[position - startProbePosition] &&
                    !synchronizer.getMatchedPosition().hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (buildKeyChunks.getInt(0, matchedPosition) == int2ValueArray[position - startProbePosition] &&
                    !synchronizer.getMatchedPosition().hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            int2ValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

    }
}