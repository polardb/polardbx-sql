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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract Join Executor
 *
 */
abstract class AbstractJoinExec extends AbstractExecutor {

    // Limit the size of bloom filter to ~9MB
    protected static final int BLOOM_FILTER_ROWS_LIMIT_FOR_PARALLEL = 10_500_000;
    // Limit the size of bloom filter to ~2MB
    protected static final int BLOOM_FILTER_ROWS_LIMIT = 2_500_000;

    final Executor outerInput;
    final Executor innerInput;
    final List<DataType> dataTypes;

    final JoinRelType joinType;
    final boolean outerJoin;
    final boolean semiJoin; // is semi or anti-semi join?
    final boolean singleJoin; // for inner semi-join and left semi-join
    final List<EquiJoinKey> joinKeys;
    final IExpression condition;
    final List<IExpression> antiJoinOperands;
    final IExpression antiCondition; // for null-aware anti-join

    final ChunkConverter outerKeyChunkGetter;
    final ChunkConverter innerKeyChunkGetter;

    final List<Integer> ignoreNullBlocks = new ArrayList<>();

    protected int[] innerKeyMapping;

    protected boolean useVecJoin;
    protected boolean enableVecBuildJoinRow;

    protected boolean shouldRecycle;

    AbstractJoinExec(Executor outerInput,
                     Executor innerInput,
                     JoinRelType joinType,
                     boolean maxOneRow,
                     List<EquiJoinKey> joinKeys,
                     IExpression condition,
                     List<IExpression> antiJoinOperands,
                     IExpression antiCondition,
                     ExecutionContext context) {
        super(context);
        this.outerInput = outerInput;
        this.innerInput = innerInput;
        this.joinType = joinType;
        this.singleJoin = maxOneRow;
        this.joinKeys = joinKeys;
        this.condition = condition;
        this.antiJoinOperands = antiJoinOperands;
        this.antiCondition = antiCondition;

        this.outerJoin = joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT;
        this.semiJoin = (joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI) && !maxOneRow;

        this.useVecJoin = context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_JOIN);
        this.enableVecBuildJoinRow = context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_BUILD_JOIN_ROW);
        this.shouldRecycle = context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVER_OBJECT_POOL);

        // mapping from key columns to its ref index in chunk.
        this.innerKeyMapping = new int[this.innerInput.getDataTypes().size()];
        Arrays.fill(innerKeyMapping, -1);

        if (joinKeys != null) {
            DataType[] keyColumnTypes = joinKeys.stream().map(t -> t.getUnifiedType()).toArray(DataType[]::new);
            int[] outerKeyColumns = joinKeys.stream().mapToInt(t -> t.getOuterIndex()).toArray();
            int[] innerKeyColumns = joinKeys.stream().mapToInt(t -> t.getInnerIndex()).toArray();

            for (int i = 0; i < innerKeyColumns.length; i++) {
                innerKeyMapping[innerKeyColumns[i]] = i;
            }

            outerKeyChunkGetter = Converters.createChunkConverter(
                outerInput.getDataTypes(), outerKeyColumns, keyColumnTypes, context);
            innerKeyChunkGetter = Converters.createChunkConverter(
                innerInput.getDataTypes(), innerKeyColumns, keyColumnTypes, context);
        } else {
            outerKeyChunkGetter = null;
            innerKeyChunkGetter = null;
        }

        if (semiJoin) {
            dataTypes = outerInput.getDataTypes();
        } else if (singleJoin) {
            final List<DataType> outer = outerInput.getDataTypes();
            final List<DataType> inner = innerInput.getDataTypes();
            dataTypes = ImmutableList.<DataType>builder()
                .addAll(outer)
                .addAll(inner.subList(0, 1)) // only keep the first column for single join
                .build();
        } else {
            final List<DataType> outer = outerInput.getDataTypes();
            final List<DataType> inner = innerInput.getDataTypes();
            dataTypes = ImmutableList.<DataType>builder()
                .addAll(joinType.leftSide(outer, inner))
                .addAll(joinType.rightSide(outer, inner))
                .build();
        }

        if (joinKeys != null) {
            for (int i = 0; i < joinKeys.size(); i++) {
                if (!joinKeys.get(i).isNullSafeEqual()) {
                    ignoreNullBlocks.add(i);
                }
            }
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    boolean checkAntiJoinOperands(Row outerRow) {
        if (antiJoinOperands == null) {
            return true;
        }
        for (IExpression operand : antiJoinOperands) {
            if (operand.eval(outerRow) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(innerInput, outerInput);
    }

    protected static boolean checkContainsNull(Chunk chunk) {
        for (int j = 0; j < chunk.getBlockCount(); j++) {
            Block block = chunk.getBlock(j);
            for (int k = 0; k < block.getPositionCount(); k++) {
                if (block.isNull(k)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected static boolean checkContainsNull(ChunksIndex chunks) {
        for (int i = 0; i < chunks.getChunkCount(); i++) {
            if (checkContainsNull(chunks.getChunk(i))) {
                return true;
            }
        }
        return false;
    }

    protected List<Integer> getIgnoreNullsInJoinKey() {
        return ignoreNullBlocks;
    }

    protected void buildJoinRow(ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first outer side, then inner side
        int col = 0;
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
        }
        // single join only output the first row of right side
        final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
        for (int i = 0; i < rightColumns; i++) {
            chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
        }
        assert col == blockBuilders.length;
    }

    protected void buildRightJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first inner side, then outer side
        int col = 0;
        for (int i = 0; i < innerInput.getDataTypes().size(); i++) {
            chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
        }
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
        }
        assert col == blockBuilders.length;
    }

    protected void buildLeftNullRow(Chunk inputChunk, int position) {
        // first outer side, then inner side
        int col = 0;
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            inputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
        }
        // single join only output the first row of right side
        final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
        for (int i = 0; i < rightColumns; i++) {
            blockBuilders[col++].appendNull();
        }
        assert col == blockBuilders.length;
    }

    protected void buildRightNullRow(Chunk inputChunk, int position) {
        // first inner side, then outer side
        int col = 0;
        for (int i = 0; i < innerInput.getDataTypes().size(); i++) {
            blockBuilders[col++].appendNull();
        }
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            inputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
        }
        assert col == blockBuilders.length;
    }

    protected boolean checkJoinCondition(ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition,
                                         int innerPosition) {
        if (condition == null) {
            return true;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = chunksIndex.rowAt(innerPosition);

        Row leftRow = joinType.leftSide(outerRow, innerRow);
        Row rightRow = joinType.rightSide(outerRow, innerRow);
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);
        return checkJoinCondition(joinRow);
    }

    boolean checkJoinCondition(Row joinRow) {
        Object obj = condition.eval(joinRow);
        if (obj instanceof Number) {
            return DataTypes.ULongType.convertFrom(obj).compareTo(UInt64.UINT64_ZERO) != 0;
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        return false; // to deal with null value in condition evaluation
    }

    protected boolean checkAntiJoinConditionHasNull(
        ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition, int innerPosition) {
        return false;
    }

    interface ProbeOperator {
        void nextRows();

        void close();

        int estimateSize();
    }

}
