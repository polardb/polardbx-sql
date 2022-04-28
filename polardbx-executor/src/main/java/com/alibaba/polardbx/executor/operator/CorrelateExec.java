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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.SubqueryApply;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Apply executor for LogicalCorrelate
 */
public class CorrelateExec extends AbstractExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CorrelateExec.class);

    private Chunk currentChunk;

    private boolean isFinish;
    private ListenableFuture<?> blocked;

    private SubqueryApply curSubqueryApply = null;
    private int applyRowIndex = 0;
    private boolean closed;

    // Fields for iterative execution for subquery
    protected CorrelationId correlateId;

    private Executor left;
    private RelNode plan;
    private SemiJoinType semiJoinType;
    private List<RexNode> leftConditions;
    private SqlKind opKind;
    private RelDataType correlateDataRowType;
    private DataType outColumnType;
    protected List<DataType> columnMetas;

    private Object constantValue;
    private boolean hasConstantValue = false;
    private boolean isValueConstant = false;

    public CorrelateExec(Executor left, RelNode plan, DataType outColumnType,
                         RelDataType correlateDataRowType, CorrelationId correlateId,
                         List<RexNode> leftConditions, SqlKind opKind,
                         SemiJoinType semiJoinType, ExecutionContext context) {
        super(context);
        this.left = left;
        this.plan = plan;
        this.leftConditions = leftConditions;
        this.opKind = opKind;
        this.outColumnType = outColumnType;
        this.correlateDataRowType = correlateDataRowType;
        this.semiJoinType = semiJoinType;
        this.columnMetas = ImmutableList.<DataType>builder().addAll(left.getDataTypes()).add(outColumnType).build();
        this.correlateId = correlateId;
        this.blocked = NOT_BLOCKED;
        this.isValueConstant = !RelOptUtil.getVariablesUsed(plan).contains(this.correlateId);
    }

    @Override
    Chunk doNextChunk() {
        if (closed || isFinish) {
            forceClose();
            return null;
        }
        if (currentChunk == null) {
            // read next chunk from input
            currentChunk = left.nextChunk();
            if (currentChunk == null) {
                isFinish = left.produceIsFinished();
                blocked = left.produceIsBlocked();
                return null;
            }
            applyRowIndex = 0;
        }

        if (hasConstantValue) {
            for (; applyRowIndex < currentChunk.getPositionCount(); applyRowIndex++) {
                // apply of applyRowIndex's row is finished, compute result
                for (int i = 0; i < left.getDataTypes().size(); i++) {
                    currentChunk.getBlock(i).writePositionTo(applyRowIndex, blockBuilders[i]);
                }
                if (constantValue == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
                    blockBuilders[getDataTypes().size() - 1].writeObject(null);
                } else {
                    blockBuilders[getDataTypes().size() - 1].writeObject(outColumnType.convertFrom(constantValue));
                }
            }
            currentChunk = null;
            return buildChunkAndReset();
        }

        if (curSubqueryApply == null) {
            curSubqueryApply = createSubqueryApply(applyRowIndex);
            curSubqueryApply.prepare();
        }
        curSubqueryApply.process();
        if (curSubqueryApply.isFinished()) {
            curSubqueryApply.close();

            if ((leftConditions == null || leftConditions.size() == 0) && isValueConstant) {
                constantValue = curSubqueryApply.getResultValue();
                hasConstantValue = true;
            }

            // apply of applyRowIndex's row is finished, compute result
            for (int i = 0; i < left.getDataTypes().size(); i++) {
                currentChunk.getBlock(i).writePositionTo(applyRowIndex, blockBuilders[i]);
            }

            if (curSubqueryApply.getResultValue() == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
                blockBuilders[getDataTypes().size() - 1].writeObject(null);
            } else {
                blockBuilders[getDataTypes().size() - 1].writeObject(
                        getDataTypes().get(getDataTypes().size() - 1)
                                .convertFrom(curSubqueryApply.getResultValue()));
            }

            curSubqueryApply = null;
            if (++applyRowIndex == currentChunk.getPositionCount()) {
                applyRowIndex = 0;
                currentChunk = null;
                return buildChunkAndReset();
            }
        } else {
            blocked = curSubqueryApply.isBlocked();
        }
        return null;
    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetas;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        left.open();
    }

    @Override
    void doClose() {
        try {
            left.close();
        } catch (Exception ex) {
            logger.error("Failed to close correlate left node", ex);
        }
        closed = true;
        isFinish = true;
        forceClose();
    }

    @Override
    public synchronized void forceClose() {
        currentChunk = null;
        if (curSubqueryApply != null) {
            curSubqueryApply.close();
            curSubqueryApply = null;
        }
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(left);
    }

    private SubqueryApply createSubqueryApply(int rowIndex) {
        Chunk.ChunkRow chunkRow = currentChunk.rowAt(rowIndex);
        // handle apply subquerys
        return SubqueryUtils
                .createSubqueryApply(
                        correlateId + "_" + Thread.currentThread().getName() + "_" + SubqueryUtils.nextSubqueryId
                                .getAndIncrement(), chunkRow,
                        plan, leftConditions, opKind, context,
                        correlateId, correlateDataRowType, semiJoinType, true);
    }
}
