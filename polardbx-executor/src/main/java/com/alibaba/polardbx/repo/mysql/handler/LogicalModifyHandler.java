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
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ALLOW_EXTRA_READ_CONN;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

/**
 * Created by minggong.zm on 19/3/14. Execute UPDATE/DELETE that cannot be
 * pushed down.
 */
public class LogicalModifyHandler extends HandlerCommon {

    public LogicalModifyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalModify modify = (LogicalModify) logicalPlan;
        final RelNode input = modify.getInput();
        checkModifyLimitation(modify, executionContext);

        // Batch size, default 1000 * groupCount
        final int groupCount =
            ExecutorContext.getContext(modify.getSchemaName()).getTopologyHandler().getMatrix().getGroups().size();
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.UPDATE_DELETE_SELECT_BATCH_SIZE) * groupCount;
        final Object history = executionContext.getExtraCmds().get(ALLOW_EXTRA_READ_CONN);

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext modifyEc = executionContext.copy();
        modifyEc.setModifySelect(true);
        final ExecutionContext selectEc = modifyEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, true);

        // Parameters for spill out
        final long maxMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = maxMemoryLimit / batchSize;
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, Math.max(BLOCK_SIZE, maxMemoryLimit), MemoryType.OPERATOR);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

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

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                final RowSet rowSet = new RowSet(values, returnColumns);

                if (!values.isEmpty()) {
                    modifyEc.setPhySqlId(modifyEc.getPhySqlId() + 1);

                    // Handle primary
                    affectRows += modify.getPrimaryModifyWriters()
                        .stream()
                        .map(w -> execute(w, rowSet, modifyEc))
                        .reduce(0, Integer::sum);

                    // Handle index
                    modify.getGsiModifyWriters().forEach(w -> execute(w, rowSet, modifyEc));
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

    private void checkModifyLimitation(LogicalModify logicalModify, ExecutionContext executionContext) {
        List<RelOptTable> targetTables = logicalModify.getTargetTables();
        // Currently, we don't support same table name from different schema name
        boolean existsShardTable = false;
        Map<String, Set<String>> tableSchemaMap = new TreeMap<>(String::compareToIgnoreCase);
        for (RelOptTable targetTable : targetTables) {
            final List<String> qualifiedName = targetTable.getQualifiedName();
            final String tableName = Util.last(qualifiedName);
            final String schema = qualifiedName.get(qualifiedName.size() - 2);

            /**
             final TddlRuleManager rule = OptimizerContext.getContext(schema).getRuleManager();
             if (rule != null) {
             final boolean shard = rule.isShard(tableName);
             if (shard) {
             existsShardTable = true;
             }
             }
             */

            if (tableSchemaMap.containsKey(tableName) && !tableSchemaMap.get(tableName).contains(schema)) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Duplicate table name in UPDATE/DELETE");
            }
            tableSchemaMap.compute(tableName, (k, v) -> {
                final Set<String> result =
                    Optional.ofNullable(v).orElse(new TreeSet<>(String::compareToIgnoreCase));
                result.add(schema);
                return result;
            });
        }
        // update / delete the whole table
        if (executionContext.getParamManager().getBoolean(ConnectionParams.FORBID_EXECUTE_DML_ALL)) {
            logicalModify.getInput().accept(new DmlAllChecker());
        }
    }

    private static class DmlAllChecker extends RelShuttleImpl {
        private static void check(Join join) {
            if (join instanceof SemiJoin) {
                // nothing to check
                return;
            } else {
                // Cartesian product
                RexNode condition = join.getCondition();
                if (null == condition || condition.isAlwaysTrue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FORBID_EXECUTE_DML_ALL);
                }
            }
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            check(join);
            return join;
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Join) {
                check((Join) other);
                return other;
            }

            return super.visit(other);
        }
    }
}
