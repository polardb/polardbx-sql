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
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

/**
 * Base Executor for Hash Join and Nested-Loop Join
 * <p>
 * If Outer Driver, March left/right outer join, use this Executor to calculate when the HashJoin operator`s attribute 'buildOuter' is true
 *
 * @author hongxi.chx
 */
abstract class AbstractBufferedJoinExec extends AbstractJoinExec {

    private static final Logger logger = LoggerFactory.getLogger(AbstractBufferedJoinExec.class);

    protected final boolean isEquiJoin;

    ChunksIndex buildChunks;
    ChunksIndex buildKeyChunks;

    MemoryPool memoryPool;
    MemoryAllocatorCtx memoryAllocator;

    // Internal states
    protected Chunk probeChunk;
    protected int[] probeKeyHashCode;
    private int probePosition;
    private Chunk probeJoinKeyChunk; // for equi-join

    // Special mode only for semi/anti-join
    protected boolean passThrough;
    protected boolean passNothing;

    private boolean isMatching;
    private int matchedPosition;
    private boolean matched;
    protected boolean streamJoin;

    // TODO all anti join use anticondition instead of antiJoinOperands
    private boolean useAntiCondition = false;

    AbstractBufferedJoinExec(Executor outerInput,
                             Executor innerInput,
                             JoinRelType joinType,
                             boolean maxOneRow,
                             List<EquiJoinKey> joinKeys,
                             IExpression condition,
                             List<IExpression> antiJoinOperands,
                             IExpression antiCondition,
                             boolean useAntiCondition,
                             ExecutionContext context) {
        this(outerInput, innerInput, joinType, maxOneRow, joinKeys, condition, antiJoinOperands, antiCondition,
            context);
        this.useAntiCondition = useAntiCondition;
    }

    AbstractBufferedJoinExec(Executor outerInput,
                             Executor innerInput,
                             JoinRelType joinType,
                             boolean maxOneRow,
                             List<EquiJoinKey> joinKeys,
                             IExpression condition,
                             List<IExpression> antiJoinOperands,
                             IExpression antiCondition,
                             ExecutionContext context) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, condition, antiJoinOperands, antiCondition,
            context);
        createBlockBuilders();
        this.isEquiJoin = joinKeys != null;
    }

    @Override
    public void doOpen() {
        getProbeInput().open();
    }

    abstract int matchInit(Chunk keyChunk, int[] hashCodes, int position);

    abstract int matchNext(int current, Chunk keyChunk, int position);

    abstract boolean matchValid(int current);

    @Override
    Chunk doNextChunk() {
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            return nextProbeChunk();
        } else if (passNothing) {
            return null;
        }

        if (!streamJoin) {
            long start = System.currentTimeMillis();
            while (currentPosition() < chunkLimit) {
                if (probeChunk == null || probePosition == probeChunk.getPositionCount()) {
                    if (System.currentTimeMillis() - start >= MppConfig.getInstance().getSplitRunQuanta()) {
                        //exceed 1 second
                        probeJoinKeyChunk = null;
                        probeKeyHashCode = null;
                        probeChunk = null;
                        break;
                    }
                    probeChunk = nextProbeChunk();
                    if (probeChunk == null) {
                        probeJoinKeyChunk = null;
                        probeKeyHashCode = null;
                        break;
                    } else {
                        if (isEquiJoin) {
                            probeJoinKeyChunk = getProbeKeyChunkGetter().apply(probeChunk);
                            probeKeyHashCode = probeJoinKeyChunk.hashCodeVector();
                        }
                        probePosition = 0;
                    }
                }
                // Process outer rows in this input chunk
                nextRows();
            }
        } else {
            if (probeChunk == null || probePosition == probeChunk.getPositionCount()) {
                probeChunk = nextProbeChunk();
                if (probeChunk == null) {
                    probeJoinKeyChunk = null;
                    probeKeyHashCode = null;
                } else {
                    if (isEquiJoin) {
                        probeJoinKeyChunk = getProbeKeyChunkGetter().apply(probeChunk);
                        probeKeyHashCode = probeJoinKeyChunk.hashCodeVector();
                    }
                    probePosition = 0;
                    nextRows();
                }
            } else {
                nextRows();
            }
        }

        if (outerJoin && !outputNullRowInTime()) {
            //output the join null rows
            while (currentPosition() < chunkLimit) {
                if (!nextJoinNullRows()) {
                    break;
                }
            }
        }
        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private void nextRows() {
        final int positionCount = probeChunk.getPositionCount();
        for (; probePosition < positionCount; probePosition++) {

            // reset matched flag unless it's still during matching
            if (!isMatching) {
                matched = false;
                matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);
            } else {
                // continue from the last processed match
                matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition);
                isMatching = false;
            }

            boolean hasAntiNull = false;
            for (; matchValid(matchedPosition);
                 matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {
                if (hasAntiNull == false) {
                    hasAntiNull =
                        checkAntiJoinConditionHasNull(buildChunks, probeChunk, probePosition, matchedPosition);
                }
                if (!checkJoinCondition(buildChunks, probeChunk, probePosition, matchedPosition)) {
                    continue;
                }

                if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
                    buildJoinRow(buildChunks, probeChunk, probePosition, matchedPosition);
                } else if (joinType == JoinRelType.RIGHT) {
                    buildRightJoinRow(buildChunks, probeChunk, probePosition, matchedPosition);
                }

                // checks max1row generated from scalar subquery
                if ((!ConfigDataMode.isFastMock()) && singleJoin && matched) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW);
                }

                // set matched flag
                matched = true;

                // semi/anti-joins do not care multiple matches
                if (semiJoin) {
                    break;
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    isMatching = true;
                    return;
                }
            }

            // generates a null result while using outer join
            if (outerJoin && outputNullRowInTime() && !matched) {
                if (joinType != JoinRelType.RIGHT) {
                    buildLeftNullRow(probeChunk, probePosition);
                } else {
                    buildRightNullRow(probeChunk, probePosition);
                }
            }

            // generates a semi-row result while using semi or anti-semi join
            if (semiJoin) {
                if (joinType == JoinRelType.SEMI && matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                } else if (joinType == JoinRelType.ANTI && !matched) {
                    if (useAntiCondition) {
                        if (!hasAntiNull) {
                            buildSemiJoinRow(probeChunk, probePosition);
                        }
                    } else if (checkAntiJoinOperands(probeChunk, probePosition)) {
                        buildSemiJoinRow(probeChunk, probePosition);
                    }
                }
            }

            // check buffered data is full
            if (currentPosition() >= chunkLimit) {
                probePosition++;
                return;
            }
        }
    }

    protected boolean nextJoinNullRows() {
        return false;
    }

    private void buildSemiJoinRow(Chunk inputChunk, int position) {
        // outer side only
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            inputChunk.getBlock(i).writePositionTo(position, blockBuilders[i]);
        }
    }

    private boolean checkAntiJoinOperands(Chunk outerChunk, int outerPosition) {
        return checkAntiJoinOperands(outerChunk.rowAt(outerPosition));
    }

    Chunk nextProbeChunk() {
        return getProbeInput().nextChunk();
    }

    /**
     * For semi/anti-join, enter pass-through or pass-nothing mode in some cases
     */
    void doSpecialCheckForSemiJoin() {
        passThrough = false;
        passNothing = false;
        if (buildChunks.isEmpty() && semiJoin) {
            if (joinType == JoinRelType.SEMI) {
                passNothing = true;
            } else if (joinType == JoinRelType.ANTI) {
                // Note that even for 'NOT IN' anti-join, we should not check operator anymore
                passThrough = true;
            } else {
                throw new AssertionError();
            }
        } else if (joinType == JoinRelType.ANTI && antiJoinOperands != null
            && buildChunks.getChunk(0).getBlockCount() == 1) {
            // Special case for x NOT IN (... NULL ...) which results in NULL
            if (checkContainsNull(buildChunks)) {
                passNothing = true;
            }
        }
    }

    Executor getBuildInput() {
        return innerInput;
    }

    Executor getProbeInput() {
        return outerInput;
    }

    ChunkConverter getBuildKeyChunkGetter() {
        return innerKeyChunkGetter;
    }

    ChunkConverter getProbeKeyChunkGetter() {
        return outerKeyChunkGetter;
    }

    protected boolean outputNullRowInTime() {
        return true;
    }

    public void setStreamJoin(boolean streamJoin) {
        this.streamJoin = streamJoin;
    }
}
