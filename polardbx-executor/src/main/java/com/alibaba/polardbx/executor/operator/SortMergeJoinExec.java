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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.Chunk.ChunkRow;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.NullBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort-Merge Join Executor
 *
 */
public class SortMergeJoinExec extends AbstractJoinExec {

    private final OneJoinSide outerSide;
    private final OneJoinSide innerSide;
    private final ChunkRow nullInnerRow;
    private final boolean outerOrAntiJoin;
    private final int compareCoeffients[];

    private ChunkRow outerKey;
    private ChunkRow innerKey;
    private boolean continueAdvanceCurrentInner;
    private boolean continueAdvanceCurrentOuter;
    private List<ChunkRow> outerSubset;
    private List<ChunkRow> innerSubset;
    private boolean needAdvanceOuter;
    private boolean needAdvanceInner;
    private boolean outerFinished;
    private boolean innerFinished;
    private ResultsIterator resultsIter;
    private boolean innerEmpty;
    private boolean passNothing;
    private boolean bStartConsumeInner; //标记MergeJoinExec 是否成功开始接受到inner第一条数据
    private boolean bStartConsumeOuter;  //标记MergeJoinExec 是否成功开始接受到outer第一条数据

    private ListenableFuture<?> blocked;

    public SortMergeJoinExec(Executor outerInput,
                             Executor innerInput,
                             JoinRelType joinType,
                             boolean maxOneRow,
                             List<EquiJoinKey> joinKeys,
                             List<Boolean> keyColumnIsAscending,
                             IExpression otherCondition,
                             List<IExpression> antiJoinOperands,
                             ExecutionContext context) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null, context);

        createBlockBuilders();
        this.outerSide = new OneJoinSide(outerInput, outerKeyChunkGetter);
        this.innerSide = new OneJoinSide(innerInput, innerKeyChunkGetter);

        this.nullInnerRow = buildNullRow(innerInput.getDataTypes());

        this.outerOrAntiJoin = outerJoin || joinType == JoinRelType.ANTI;
        this.blocked = NOT_BLOCKED;

        this.compareCoeffients = new int[joinKeys.size()];
        for (int i = 0; i < joinKeys.size(); i++) {
            this.compareCoeffients[i] = keyColumnIsAscending.get(i) ? 1 : -1;
        }
    }

    @Override
    void doOpen() {
        outerInput.open();
        innerInput.open();

        // Reset inner states
        outerFinished = false;
        innerFinished = false;
        outerSubset = new ArrayList<>();
        innerSubset = new ArrayList<>();
        this.bStartConsumeInner = false;
        this.bStartConsumeOuter = false;
    }

    @Override
    Chunk doNextChunk() {
        if (!bStartConsumeInner || !bStartConsumeOuter) {
            if (!bStartConsumeInner) {
                boolean exist = innerSide.next();
                if (exist || innerSide.isDone()) {
                    innerEmpty = innerSide.isDone();
                    if (joinType == JoinRelType.ANTI) {
                        doSpecialCheckForAntiJoin();
                    }
                    consumeInner();
                    bStartConsumeInner = true;
                }
            }

            if (!bStartConsumeOuter) {
                boolean exist = outerSide.next();
                if (exist || outerSide.isDone()) {
                    consumeOuter();
                    bStartConsumeOuter = true;
                }
            }
        }

        if (!bStartConsumeInner || !bStartConsumeOuter) {
            return null;
        }

        if (passNothing) {
            return null;
        }

        while (currentPosition() < chunkLimit) {
            Row row = nextRow();
            if (row == null) {
                break;
            }
            if (semiJoin) {
                appendChunkRow((ChunkRow) row);
            } else {
                final JoinRow r = (JoinRow) row;
                appendJoinedChunkRows((ChunkRow) r.getLeftRowSet(), (ChunkRow) r.getRightRowSet());
            }
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private void appendChunkRow(ChunkRow row) {
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            row.getChunk().getBlock(i).writePositionTo(row.getPosition(), blockBuilders[i]);
        }
    }

    private void appendJoinedChunkRows(ChunkRow left, ChunkRow right) {
        int col = 0;
        for (int i = 0; i < left.getChunk().getBlockCount(); i++) {
            left.getChunk().getBlock(i).writePositionTo(left.getPosition(), blockBuilders[col++]);
        }
        // Single join only output the first row of right side
        final int rightColumns = singleJoin ? 1 : right.getChunk().getBlockCount();
        for (int i = 0; i < rightColumns; i++) {
            right.getChunk().getBlock(i).writePositionTo(right.getPosition(), blockBuilders[col++]);
        }
        assert col == blockBuilders.length;
    }

    @Override
    void doClose() {
        outerInput.close();
        innerInput.close();
    }

    private Row nextRow() {
        while (true) {
            // First of all, consume all the joined results in results iterator if exists
            if (resultsIter != null) {
                Row record = resultsIter.next();
                if (record != null) {
                    return record;
                } else {
                    resultsIter = null;
                }
            }

            if (outerFinished || innerFinished) {
                if (!outerOrAntiJoin) {
                    // for inner join, if any of inner/outer is finished, join is finished.
                    passNothing = true;
                }
                break;
            }

            if (needAdvanceOuter) {
                advanceOuter();
            }
            if (needAdvanceInner) {
                advanceInner();
            }

            if (!needAdvanceInner && !needAdvanceOuter) {
                //搜集完两边数据后，可以进行下一阶段计算
            } else {
                //未搜集全两边数据，无法进行计算，跳出当前循环
                break;
            }

            int compare = compare(outerKey, innerKey);

            if (compare == 0) {
                if (checkJoinKeysNotNull(outerKey)) {
                    resultsIter = new JoinResultsIterator(outerSubset, innerSubset);
                } else if (outerOrAntiJoin) {
                    resultsIter = new JoinNotMatchedResultsIterator(outerSubset);
                }

                consumeInner();
                consumeOuter();
            } else if (compare < 0) {
                // For outer-join or anti-join, we have to generate some null rows even if no inner rows are matched
                if (outerOrAntiJoin) {
                    resultsIter = new JoinNotMatchedResultsIterator(outerSubset);
                }

                consumeOuter();
            } else { // compare > 0
                consumeInner();
            }
        }

        if (outerOrAntiJoin) {
            // for outer-join or anti-join, we have to drain out the outer side
            while (true) {
                // consumes all the joined results in results iterator if exists
                if (resultsIter != null) {
                    Row record = resultsIter.next();
                    if (record != null) {
                        return record;
                    } else {
                        resultsIter = null;
                    }
                }

                if (outerFinished) {
                    passNothing = true;
                    break;
                }

                if (needAdvanceOuter) {
                    advanceOuter();
                }

                if (!needAdvanceOuter) {
                    //搜集完数据后，可以进行下一阶段计算
                } else {
                    //未搜集全数据，无法进行计算，跳出当前循环
                    break;
                }

                resultsIter = new JoinNotMatchedResultsIterator(outerSubset);
                consumeOuter();
            }
        }

        return null;
    }

    private void advanceInner() {
        if (continueAdvanceCurrentInner) {
            boolean finishCurrentProbe = continueAdvanceOneSide(innerSide, innerSubset, innerKey);
            continueAdvanceCurrentInner = !finishCurrentProbe;
            needAdvanceInner = !finishCurrentProbe;
        } else {
            innerKey = advanceOneSide(innerSide, innerSubset);
            if (innerSide.currentChunk == null && !innerSide.isDone()) {
                continueAdvanceCurrentInner = true;
                needAdvanceInner = true;
            } else {
                needAdvanceInner = false;
            }
        }
    }

    private void advanceOuter() {
        if (continueAdvanceCurrentOuter) {
            boolean finishCurrentProbe = continueAdvanceOneSide(outerSide, outerSubset, outerKey);
            continueAdvanceCurrentOuter = !finishCurrentProbe;
            needAdvanceOuter = !finishCurrentProbe;
        } else {
            outerKey = advanceOneSide(outerSide, outerSubset);
            if (outerSide.currentChunk == null && !outerSide.isDone()) {
                continueAdvanceCurrentOuter = true;
                needAdvanceOuter = true;
            } else {
                needAdvanceOuter = false;
            }
        }
    }

    private void consumeInner() {
        if (innerSide.isDone()) {
            innerFinished = true;
            innerSubset = null;
        } else {
            needAdvanceInner = true;
        }
    }

    private void consumeOuter() {
        if (outerSide.isDone()) {
            outerFinished = true;
            outerSubset = null;
        } else {
            needAdvanceOuter = true;
        }
    }

    private ChunkRow advanceOneSide(OneJoinSide input, List<ChunkRow> subset) {
        assert !input.isDone() : "input is done";
        // add current row to product subset
        subset.clear();

        subset.add(input.currentRow());

        ChunkRow joinKey = input.currentJoinKey();

        // ... along with all successive rows with same join key
        while (input.next() && compare(joinKey, input.currentJoinKey()) == 0) {
            subset.add(input.currentRow());
        }

        return joinKey;
    }

    private boolean continueAdvanceOneSide(OneJoinSide input, List<ChunkRow> subset, ChunkRow currentJoinKey) {
        assert !input.isDone() : "input is done";
        boolean finishCurrentProbe = false;
        while (input.next() && compare(currentJoinKey, input.currentJoinKey()) == 0) {
            subset.add(input.currentRow());
        }

        finishCurrentProbe = input.currentChunk != null || input.isDone();

        return finishCurrentProbe;
    }

    @SuppressWarnings("unchecked")
    private int compare(Row row1, Row row2) {
        for (int i = 0; i < joinKeys.size(); i++) {
            int result =
                joinKeys.get(i).getUnifiedType().compare(row1.getObject(i), row2.getObject(i)) * compareCoeffients[i];
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private interface ResultsIterator {
        Row next();

        boolean isDone();
    }

    private class JoinResultsIterator implements ResultsIterator {

        private final List<ChunkRow> outerRows;
        private final List<ChunkRow> innerRows;

        private int outerIndex = 0;
        private int innerIndex = 0;
        private int matchedCount = 0;

        JoinResultsIterator(List<ChunkRow> outerRows, List<ChunkRow> innerRows) {
            this.outerRows = outerRows;
            this.innerRows = innerRows;
        }

        @Override
        public boolean isDone() {
            return !(outerIndex < outerRows.size() && innerIndex < innerRows.size());
        }

        @Override
        public Row next() {
            while (outerIndex < outerRows.size() && innerIndex < innerRows.size()) {
                final ChunkRow innerRow = innerRows.get(innerIndex);
                final ChunkRow outerRow = outerRows.get(outerIndex);

                Row result = doNext(outerRow, innerRow);

                // Move to next pair of (outer row, inner row)
                if (++innerIndex == innerRows.size()) {
                    nextOuterRow();
                }

                if (result != null) {
                    return result;
                }
            }
            return null;
        }

        private Row doNext(ChunkRow outerRow, ChunkRow innerRow) {
            final ChunkRow leftRow = joinType.leftSide(outerRow, innerRow);
            final ChunkRow rightRow = joinType.rightSide(outerRow, innerRow);
            Row joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);

            if (condition != null && !checkJoinCondition(joinRow)) {
                // Specially, for outer-join and anti-join we have to emit a row even if not matched
                if (outerOrAntiJoin && matchedCount == 0 && innerIndex == innerRows.size() - 1) {
                    if (outerJoin) {
                        return makeNullRow(outerRow);
                    } else if (checkAntiJoinOperands(outerRow) || innerEmpty) {
                        return outerRow;
                    }
                }
                // skip the row if condition check is failed
                return null;
            }

            // check max1row
            if ((!(ConfigDataMode.isFastMock())) && singleJoin && ++matchedCount > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW);
            }

            if (joinType == JoinRelType.SEMI) {
                // for semi-join, emit result and move forward once we found one match
                skipInnerRows();
                return outerRow;
            } else if (joinType == JoinRelType.ANTI) {
                // for anti-semi-join, move forward once we found a match
                skipInnerRows();
                return null;
            }

            return joinRow;
        }

        private void nextOuterRow() {
            innerIndex = 0;
            outerIndex++;
            matchedCount = 0;
        }

        private void skipInnerRows() {
            // just pretend to be the last inner row
            innerIndex = innerRows.size() - 1;
        }
    }

    private class JoinNotMatchedResultsIterator implements ResultsIterator {

        private final List<ChunkRow> outerRows;

        private int outerIndex = 0;

        JoinNotMatchedResultsIterator(List<ChunkRow> outerRows) {
            this.outerRows = outerRows;
        }

        @Override
        public Row next() {
            while (outerIndex < outerRows.size()) {
                final Row outerRow = outerRows.get(outerIndex++);

                if (outerJoin) {
                    return makeNullRow(outerRow);
                } else if (joinType == JoinRelType.ANTI) {
                    if (checkAntiJoinOperands(outerRow) || innerEmpty) {
                        return outerRow;
                    }
                } else {
                    throw new AssertionError("should be anti-join or outer-join");
                }
            }
            return null;
        }

        @Override
        public boolean isDone() {
            return !(outerIndex < outerRows.size());
        }
    }

    private Row makeNullRow(Row outerRow) {
        Row leftRow = joinType.leftSide(outerRow, nullInnerRow);
        Row rightRow = joinType.rightSide(outerRow, nullInnerRow);
        return new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);
    }

    private class OneJoinSide {

        private final Executor executor;
        private final ChunkConverter keyGetter;

        private Chunk currentChunk;
        private Chunk currentKeyChunk;
        private int currentPosition;
        private boolean done = false;

        OneJoinSide(Executor executor, ChunkConverter keyGetter) {
            this.executor = executor;
            this.keyGetter = keyGetter;
        }

        boolean next() {
            currentPosition += 1;
            if (currentChunk == null || currentPosition == currentChunk.getPositionCount()) {
                currentChunk = executor.nextChunk();
                currentPosition = 0;
            }
            if (currentChunk == null) {
                if (executor.produceIsFinished()) {
                    done = true;
                }
                blocked = executor.produceIsBlocked();
                return false;
            } else {
                currentKeyChunk = keyGetter.apply(currentChunk);
            }
            return true;
        }

        ChunkRow currentRow() {
            return currentChunk.rowAt(currentPosition);
        }

        ChunkRow currentJoinKey() {
            return currentKeyChunk.rowAt(currentPosition);
        }

        boolean isDone() {
            return done;
        }
    }

    private ChunkRow buildNullRow(List<DataType> columns) {
        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new NullBlock(1);
        }
        Chunk chunk = new Chunk(blocks);
        return chunk.rowAt(0);
    }

    private boolean checkJoinKeysNotNull(ChunkRow joinKey) {
        for (int i = 0; i < joinKeys.size(); i++) {
            if (joinKey.getChunk().getBlock(i).isNull(joinKey.getPosition())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Anti-Joins such as 'x NOT IN (... NULL ...)' should always produce nothing
     */
    private void doSpecialCheckForAntiJoin() {
        passNothing = false;
        if (joinType == JoinRelType.ANTI && antiJoinOperands != null && !innerEmpty) {
            if (!checkJoinKeysNotNull(innerSide.currentJoinKey())) {
                // If first row is not null, then there is not null rows
                passNothing = true;
            }
        }
    }

    @Override
    public boolean produceIsFinished() {
        return passNothing || (outerSide.isDone() && innerSide.isDone() && (resultsIter != null &&
            resultsIter.isDone()));
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
