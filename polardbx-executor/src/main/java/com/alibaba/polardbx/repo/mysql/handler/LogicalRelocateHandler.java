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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ALLOW_EXTRA_READ_CONN;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

/**
 * @author chenmo.cm
 */
public class LogicalRelocateHandler extends HandlerCommon {

    public LogicalRelocateHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalRelocate relocate = (LogicalRelocate) logicalPlan;
        RelNode input = relocate.getInput();

        // The limit of records
        final int groupCount =
            ExecutorContext.getContext(relocate.getSchemaName()).getTopologyHandler().getMatrix().getGroups().size();
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.UPDATE_DELETE_SELECT_BATCH_SIZE) * groupCount;
        final Object history = executionContext.getExtraCmds().get(ALLOW_EXTRA_READ_CONN);

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext relocateEc = executionContext.copy();
        relocateEc.setModifySelect(true);
        final ExecutionContext selectEc = relocateEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, false);

        // Parameters for spill out
        final long maxMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = maxMemoryLimit / batchSize;
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, Math.max(BLOCK_SIZE, maxMemoryLimit), MemoryType.OPERATOR);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Integer> autoIncColumns = relocate.getAutoIncColumns();
        int offset = input.getRowType().getFieldList().size() - relocate.getUpdateColumnList().size();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode());

        int affectRows = 0;
        Cursor selectCursor = null;
        try {
            // Do select
            selectCursor = ExecutorHelper.execute(input, selectEc, true);

            // Select then modify loop
            do {
                final List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, memoryAllocator, memoryOfOneRow);

                if (values.isEmpty()) {
                    break;
                }

                for (Integer autoIncColumnIndex : autoIncColumns) {
                    for (List<Object> value : values) {
                        final Object autoIncValue = value.get(offset + autoIncColumnIndex);
                        if (null == autoIncValue || (autoValueOnZero && 0L == RexUtils.valueOfObject1(autoIncValue))) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO,
                                "Do not support update AUTO_INC columns to null or zero");
                        }
                    }
                }

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                final RowSet rowSet = new RowSet(values, returnColumns);

                if (!values.isEmpty()) {
                    relocateEc.setPhySqlId(relocateEc.getPhySqlId() + 1);

                    // Handle primary
                    affectRows += relocate.getPrimaryRelocateWriters()
                        .stream()
                        .map(w -> execute(w, rowSet, relocateEc).size())
                        .reduce(0, Integer::sum);
                    affectRows += relocate.getPrimaryUpdateWriters()
                        .stream()
                        .map(w -> execute(w, rowSet, relocateEc))
                        .reduce(0, Integer::sum);

                    // Handle index
                    relocate.getGsiRelocateWriters().forEach(w -> execute(w, rowSet, relocateEc));
                    relocate.getGsiUpdateWriters().forEach(w -> execute(w, rowSet, relocateEc));
                }

                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            } while (true);

            return new AffectRowCursor(affectRows);
        } catch (Throwable e) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
                || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
                // Can't commit
                executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL);
            }
            throw GeneralUtil.nestedException(e);
        } finally {
            if (selectCursor != null) {
                selectCursor.close(new ArrayList<>());
            }

            selectValuesPool.destroy();

            executionContext.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, history);
        }
    }

}
