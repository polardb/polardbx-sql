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

import com.alibaba.polardbx.common.SQLMode;
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
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.handler.execute.ExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.LogicalModifyExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalModify modify = (LogicalModify) logicalPlan;
        final RelNode input = modify.getInput();
        checkModifyLimitation(modify, executionContext);

        final boolean checkForeignKey =
            executionContext.foreignKeyChecks();
        final boolean foreignKeyChecksForUpdateDelete =
            executionContext.getParamManager().getBoolean(ConnectionParams.FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

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

        boolean haveGeneratedColumn = tables.stream().anyMatch(
            tableName -> executionContext.getSchemaManager(modify.getSchemaName()).getTable(tableName)
                .hasLogicalGeneratedColumn());

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

        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);

        int affectRows = 0;
        Cursor selectCursor = null;
        try {
            // Do select
            selectCursor = ExecutorHelper.execute(input, selectEc, true);

            // Select then modify loop
            do {
                final List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, memoryAllocator::allocateReservedMemory, memoryOfOneRow);

                if (values.isEmpty()) {
                    break;
                }

                if (haveGeneratedColumn && modify.isUpdate()) {
                    evalGeneratedColumns(modify, values, modify.getEvalRowColumnMetas(),
                        modify.getInputToEvalFieldMappings(), modify.getGenColRexNodes(), executionContext);
                }

                if (canModifyByMulti && values.size() >= batchSize) {
                    //values过多，转多线程执行操作
                    affectRows += doModifyExecuteMulti(modify, modifyEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow, haveGeneratedColumn);
                    break;
                }

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                final RowSet rowSet = new RowSet(values, returnColumns);

                if (checkForeignKey && foreignKeyChecksForUpdateDelete) {
                    beforeModifyCheck(modify, modifyEc, values);
                }

                if (!values.isEmpty()) {
                    modifyEc.setPhySqlId(modifyEc.getPhySqlId() + 1);

                    // Handle primary
                    affectRows += modify.getPrimaryModifyWriters()
                        .stream()
                        .map(writer -> {
                            Function<DistinctWriter, List<List<Object>>> rowGenerator =
                                rowSet::distinctRowSetWithoutNull;
                            if (skipUnchangedRow && modify.getNeedCompareWriters().containsKey(writer)) {
                                // 跳过没有变化的行从而：
                                // 1. 避免没有变化的情况下更新 ON UPDATE TIMESTAMP 列
                                // 2. 减少下发的物理 SQL 数
                                // affectRows 根据参数判断是否需要加上没有修改的行
                                Integer tableIndex = modify.getNeedCompareWriters().get(writer);
                                rowGenerator = new Function<DistinctWriter, List<List<Object>>>() {
                                    @Override
                                    public List<List<Object>> apply(DistinctWriter distinctWriter) {
                                        return rowSet.distinctRowSetWithoutNullThenRemoveSameRow(distinctWriter,
                                            modify.getSetColumnTargetMappings().get(tableIndex),
                                            modify.getSetColumnSourceMappings().get(tableIndex),
                                            modify.getSetColumnMetas().get(tableIndex));
                                    }
                                };

                            }
                            int executeAffectRows = execute(writer, rowGenerator, modifyEc);
                            //判断是否需要加入找到的行
                            if (modifyEc.isClientFoundRows()) {
                                executeAffectRows += rowSet.getSameRowCount();
                            }
                            return executeAffectRows;
                        }).reduce(0, Integer::sum);

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
                executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL,
                    e.getMessage());
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
                                     long memoryOfOneRow, boolean haveGeneratedColumn) {
        final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();
        //通过MemoryControlByBlocked控制内存大小，防止内存占用
        BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
        MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
        ParallelExecutor parallelExecutor = createModifyParallelExecutor(executionContext, modify, returnColumns,
            selectValues, memoryControl);
        long valuesSize = memoryAllocator.getAllAllocated();
        parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());
        int affectRows =
            doModifyParallelExecute(parallelExecutor, someValues, valuesSize, selectCursor, batchSize, memoryOfOneRow,
                haveGeneratedColumn, modify, executionContext);
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
            List<String> groups = ExecUtils.getTableGroupNames(schemaName, table, ec);
            allGroupNames.addAll(groups);
            //包含gsi情况下，auto库可能主表和GSI的group不同
            List<String> gsiTables = GlobalIndexMeta.getIndex(table, schemaName, ec)
                .stream().map(TableMeta::getTableName).collect(Collectors.toList());
            for (String gsi : gsiTables) {
                groups = ExecUtils.getTableGroupNames(schemaName, gsi, ec);
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
        SqlNode originNode = logicalModify.getOriginalSqlNode();
        checkUpdateDeleteLimitLimitation(originNode, executionContext);

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
                                        Cursor selectCursor, long batchSize, long memoryOfOneRow,
                                        boolean haveGeneratedColumn, LogicalModify modify, ExecutionContext ec) {
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
                AtomicLong batchRowSize = new AtomicLong(0L);
                List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, batchRowSize::getAndAdd, memoryOfOneRow);

                if (haveGeneratedColumn && modify.isUpdate()) {
                    evalGeneratedColumns(modify, values, modify.getEvalRowColumnMetas(),
                        modify.getInputToEvalFieldMappings(), modify.getGenColRexNodes(), ec);
                }

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
                //有可能等待空间时出错了
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowSize.get()));
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

    public static void evalGeneratedColumns(TableModify modify, List<List<Object>> values,
                                            Map<Integer, List<ColumnMeta>> evalRowColumnMetas,
                                            Map<Integer, List<Integer>> inputToEvalFieldMappings,
                                            Map<Integer, List<RexNode>> genColRexNodes, ExecutionContext ec) {
        Set<Integer> targetTableIndexSet = new HashSet<>(modify.getTargetTableIndexes());
        boolean strict = SQLMode.isStrictMode(ec.getSqlModeFlags());

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = modify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);

            if (!tableMeta.hasLogicalGeneratedColumn()) {
                continue;
            }

            List<Integer> inputToEvalFieldMapping = inputToEvalFieldMappings.get(tableIndex);
            List<ColumnMeta> columnMeta = evalRowColumnMetas.get(tableIndex);
            List<RexNode> rexNodes = genColRexNodes.get(tableIndex);

            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < inputToEvalFieldMapping.size(); i++) {
                    row.add(value.get(inputToEvalFieldMapping.get(i)));
                }
                rows.add(row);
            }

            // Convert row type
            int refColCnt = inputToEvalFieldMapping.size() - rexNodes.size();
            for (int i = 0; i < rows.size(); i++) {
                List<Object> row = rows.get(i);
                for (int j = 0; j < refColCnt; j++) {
                    row.set(j, RexUtils.convertValue(row.get(j), DataTypeUtil.getTypeOfObject(row.get(j)), strict,
                        columnMeta.get(j), ec));
                }
            }

            CursorMeta cursorMeta = CursorMeta.build(columnMeta);
            for (List<Object> row : rows) {
                Row r = new ArrayRow(cursorMeta, row.toArray());
                for (int i = 0; i < rexNodes.size(); i++) {
                    Object value = RexUtils.getValueFromRexNode(rexNodes.get(i), r, ec);
                    value = RexUtils.convertValue(value, rexNodes.get(i), strict, columnMeta.get(refColCnt + i), ec);
                    r.setObject(i + refColCnt, value);
                    row.set(i + refColCnt, value);
                }
            }

            for (int i = 0; i < values.size(); i++) {
                for (int j = refColCnt; j < inputToEvalFieldMapping.size(); j++) {
                    values.get(i).set(inputToEvalFieldMapping.get(j), rows.get(i).get(j));
                }
            }
        }
    }

    protected void beforeModifyCheck(LogicalModify logicalModify, ExecutionContext executionContext,
                                     List<List<Object>> values) {
        int depth = 1;
        int index = 0;
        int j = 0;

        Set<Integer> targetTableIndexSet = new TreeSet<>(logicalModify.getTargetTableIndexes());

        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = logicalModify.getFkPlans();

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = logicalModify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = executionContext.getSchemaManager(qn.left).getTable(qn.right);

            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < tableMeta.getAllColumns().size(); i++) {
                    row.add(value.get(i + index));
                }
                rows.add(row);
            }
            index += tableMeta.getAllColumns().size();

            if (logicalModify.getOperation() == TableModify.Operation.DELETE) {
                LogicalModify modify = null;

                DistinctWriter writer = logicalModify.getPrimaryModifyWriters().get(j);
                if (writer instanceof SingleModifyWriter) {
                    modify = ((SingleModifyWriter) writer).getModify();
                } else if (writer instanceof BroadcastModifyWriter) {
                    modify = ((BroadcastModifyWriter) writer).getModify();
                } else {
                    modify = ((ShardingModifyWriter) writer).getModify();
                }
                beforeDeleteFkCascade(modify, qn.left, qn.right, executionContext, rows, fkPlans, depth);
            } else {
                LogicalModify modify = null;

                DistinctWriter writer = logicalModify.getPrimaryModifyWriters().get(j);
                if (writer instanceof SingleModifyWriter) {
                    modify = ((SingleModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i), ((SingleModifyWriter) writer).getUpdateSetMapping()));
                    }
                } else if (writer instanceof BroadcastModifyWriter) {
                    modify = ((BroadcastModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i),
                                ((BroadcastModifyWriter) writer).getUpdateSetMapping()));
                    }
                } else {
                    modify = ((ShardingModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i), ((ShardingModifyWriter) writer).getUpdateSetMapping()));
                    }
                }

                beforeUpdateFkCheck(modify, qn.left, qn.right, executionContext, rows);
                beforeUpdateFkCascade(modify, qn.left, qn.right, executionContext, rows,
                    null, null, fkPlans, depth);
            }
            j++;
        }
    }
}
