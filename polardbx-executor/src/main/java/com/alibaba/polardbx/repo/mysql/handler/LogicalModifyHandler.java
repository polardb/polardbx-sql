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
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.repo.mysql.handler.execute.ExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.LogicalModifyExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

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

        //广播表不支持多线程Modify
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(modify.getSchemaName()))
            .getRuleManager();
        List<String> tables = modify.getTargetTableNames();
        boolean haveBroadcast = tables.stream().anyMatch(or::isBroadCast);
        boolean canModifyByMulti =
            !haveBroadcast && executionContext.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext modifyEc = executionContext.copy();
        modifyEc.setModifySelect(true);
        PhyTableOperationUtil.enableIntraGroupParallelism(modify.getSchemaName(), modifyEc);
        final ExecutionContext selectEc = modifyEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, true);

        // Parameters for spill out
        final long batchMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = batchMemoryLimit / batchSize;
        long maxMemoryLimit = executionContext.getParamManager().getLong(ConnectionParams.MODIFY_SELECT_BUFFER_SIZE);
        final long realMemoryPoolSize = Math.max(maxMemoryLimit, Math.max(batchMemoryLimit, BLOCK_SIZE));
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, realMemoryPoolSize, MemoryType.OPERATOR);
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
                if (canModifyByMulti && values.size() >= batchSize) {
                    //values过多，转多线程执行操作
                    affectRows += doModifyExecuteMulti(modify, modifyEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow);
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

    private int doModifyExecuteMulti(LogicalModify modify, ExecutionContext executionContext,
                                     List<List<Object>> someValues, Cursor selectCursor,
                                     long batchSize, MemoryPool selectValuesPool, MemoryAllocatorCtx memoryAllocator,
                                     long memoryOfOneRow) {
        final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();
        //通过MemoryControlByBlocked控制内存大小，防止内存占用
        BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
        MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
        ParallelExecutor parallelExecutor = createModifyParallelExecutor(executionContext, modify, returnColumns,
            selectValues, memoryControl);
        long valuesSize = memoryAllocator.getAllAllocated();
        parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());
        int affectRows =
            doModifyParallelExecute(parallelExecutor, someValues, valuesSize, selectCursor, batchSize, memoryOfOneRow);
        executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
        return affectRows;

    }

    private ParallelExecutor createModifyParallelExecutor(ExecutionContext ec, LogicalModify modify,
                                                          List<ColumnMeta> returnColumns,
                                                          BlockingQueue<List<List<Object>>> selectValues,
                                                          MemoryControlByBlocked memoryControl) {
        String schemaName = modify.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }

        boolean useTrans = ec.getTransaction() instanceof IDistributedTransaction;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        //modify可能会更新多个逻辑表，所以有一个表不是单库单表就不是单库单表；groupNames是所有表的去重并集
        boolean isSingleTable = true;
        List<String> tables = modify.getTargetTableNames();
        Set<String> allGroupNames = new HashSet<>();
        for (String table : tables) {
            if (!or.isTableInSingleDb(table)) {
                isSingleTable = false;
            }
            List<String> groups = ExecUtils.getTableGroupNames(schemaName, table);
            allGroupNames.addAll(groups);
            //包含gsi情况下，auto库可能主表和GSI的group不同
            List<String> gsiTables = GlobalIndexMeta.getIndex(table, schemaName, ec)
                .stream().map(TableMeta::getTableName).collect(Collectors.toList());
            for (String gsi : gsiTables) {
                groups = ExecUtils.getTableGroupNames(schemaName, gsi);
                allGroupNames.addAll(groups);
            }
        }

        List<String> groupNames = Lists.newArrayList(allGroupNames);
        List<String> phyParallelSet = PhyTableOperationUtil.buildGroConnSetFromGroups(ec, groupNames);
        Pair<Integer, Integer> threads =
            ExecUtils.calculateLogicalAndPhysicalThread(ec, phyParallelSet.size(), isSingleTable, useTrans);
        int logicalThreads = threads.getKey();
        int physicalThreads = threads.getValue();

        LoggerFactory.getLogger(LogicalModifyHandler.class).info(
            "Modify select by ParallelExecutor, useTrans: " + useTrans + "; logicalThreads: " + logicalThreads
                + "; physicalThreads: " + physicalThreads);

        ParallelExecutor parallelExecutor = new ParallelExecutor(memoryControl);
        List<ExecuteJob> executeJobs = new ArrayList<>();

        for (int i = 0; i < logicalThreads; i++) {
            ExecuteJob executeJob = new LogicalModifyExecuteJob(ec, parallelExecutor, modify, returnColumns);
            executeJobs.add(executeJob);
        }

        parallelExecutor.createGroupRelQueue(ec, physicalThreads, phyParallelSet, !useTrans);
        parallelExecutor.setParam(selectValues, executeJobs);
        return parallelExecutor;
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

    private int doModifyParallelExecute(ParallelExecutor parallelExecutor, List<List<Object>> someValues,
                                        long someValuesSize,
                                        Cursor selectCursor, long batchSize, long memoryOfOneRow) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
            parallelExecutor.start();

            //试探values时，获取的someValues
            if (someValues != null && !someValues.isEmpty()) {
                someValues.add(Collections.singletonList(someValuesSize));
                parallelExecutor.getSelectValues().put(someValues);
            }

            // Select and DML loop
            do {
                List<List<Object>> values = new ArrayList<>();

                long batchRowsSize = selectModifyValuesFromCursor(selectCursor, batchSize, values, memoryOfOneRow);

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowsSize);
                //有可能等待空间时出错了
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowsSize));
                parallelExecutor.getSelectValues().put(values);
            } while (true);

            //正常情况等待执行完
            affectRows = parallelExecutor.waitDone();

        } catch (Throwable t) {
            parallelExecutor.failed(t);

            throw GeneralUtil.nestedException(t);
        } finally {
            parallelExecutor.doClose();
        }
        return affectRows;
    }
}
