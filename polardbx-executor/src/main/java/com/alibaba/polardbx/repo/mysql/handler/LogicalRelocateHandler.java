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
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
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
import com.alibaba.polardbx.repo.mysql.handler.execute.LogicalRelocateExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
        // Need auto-savepoint only in auto-commit mode.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalRelocate relocate = (LogicalRelocate) logicalPlan;
        checkUpdateDeleteLimitLimitation(relocate.getOriginalSqlNode(), executionContext);

        RelNode input = relocate.getInput();
        TableMeta tableMeta =
            executionContext.getSchemaManager(relocate.getSchemaName()).getTable(relocate.getLogicalTableName());
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());
        final boolean foreignKeyChecksForUpdateDelete =
            executionContext.getParamManager().getBoolean(ConnectionParams.FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        // The limit of records
        final int groupCount =
            ExecutorContext.getContext(relocate.getSchemaName()).getTopologyHandler().getMatrix().getGroups().size();
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.UPDATE_DELETE_SELECT_BATCH_SIZE) * groupCount;
        final Object history = executionContext.getExtraCmds().get(ALLOW_EXTRA_READ_CONN);

        //广播表不支持多线程Modify
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(relocate.getSchemaName()))
            .getRuleManager();
        List<String> tables = relocate.getTargetTableNames();
        boolean haveBroadcast = tables.stream().anyMatch(or::isBroadCast);
        boolean canModifyByMulti =
            !haveBroadcast && executionContext.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);

        boolean haveGeneratedColumn = tables.stream().anyMatch(
            tableName -> executionContext.getSchemaManager(relocate.getSchemaName()).getTable(tableName)
                .hasLogicalGeneratedColumn());

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext relocateEc = executionContext.copy();
        relocateEc.setModifySelect(true);
        final ExecutionContext selectEc = relocateEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, false);
        PhyTableOperationUtil.enableIntraGroupParallelism(relocate.getSchemaName(), relocateEc);

        // Parameters for spill out
        final long batchMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = batchMemoryLimit / batchSize;
        long maxMemoryLimit = executionContext.getParamManager().getLong(ConnectionParams.MODIFY_SELECT_BUFFER_SIZE);
        final long realMemoryPoolSize = Math.max(maxMemoryLimit, Math.max(batchMemoryLimit, BLOCK_SIZE));
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, realMemoryPoolSize, MemoryType.OPERATOR);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Integer> autoIncColumns = relocate.getAutoIncColumns();
        int offset = input.getRowType().getFieldList().size() - relocate.getUpdateColumnList().size();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode());

        final Map<Integer, DistinctWriter> primaryDistinctWriter = relocate.getPrimaryDistinctWriter();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = relocate.getPrimaryRelocateWriter();

        int affectRows = 0;
        Cursor selectCursor = null;

        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

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

                if (haveGeneratedColumn) {
                    LogicalModifyHandler.evalGeneratedColumns(relocate, values, relocate.getEvalRowColumnMetas(),
                        relocate.getInputToEvalFieldMappings(), relocate.getGenColRexNodes(), executionContext);
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

                if (canModifyByMulti && values.size() >= batchSize) {
                    //values过多，转多线程执行操作
                    affectRows += doRelocateExecuteMulti(relocate, relocateEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow, haveGeneratedColumn);
                    break;
                }

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                if (checkForeignKey && foreignKeyChecksForUpdateDelete) {
                    beforeModifyCheck(relocate, relocate.getSchemaName(), relocate.getLogicalTableName(),
                        relocateEc, values);
                }

                if (!values.isEmpty()) {
                    relocateEc.setPhySqlId(relocateEc.getPhySqlId() + 1);

                    for (Integer tableIndex : relocate.getSetColumnMetas().keySet()) {
                        RowSet rowSet = new RowSet(values, returnColumns);
                        DistinctWriter dw = primaryRelocateWriter.containsKey(tableIndex) ?
                            primaryRelocateWriter.get(tableIndex).getDeleteWriter() :
                            primaryDistinctWriter.get(tableIndex);
                        List<List<Object>> distinctValues = rowSet.distinctRowSetWithoutNull(dw);

                        if (relocateEc.isClientFoundRows()) {
                            affectRows += distinctValues.size();
                        }

                        // 跳过没有变化的行从而：
                        // 1. 避免没有变化的情况下更新 ON UPDATE TIMESTAMP 列
                        // 2. 减少下发的物理 SQL 数
                        // 目前只有在 UPDATE 只修改了主表和 GSI 的拆分键的情况下进行这个判断，因为 CN 无法做到完全兼容的全类型判断，
                        // 这种实现是兼容了以前在 WRITER 中判断的行为

                        final boolean useRowSet;
                        if (skipUnchangedRow && relocate.getModifySkOnlyMap().get(tableIndex)) {
                            rowSet = buildChangedRowSet(distinctValues, returnColumns,
                                relocate.getSetColumnTargetMappings().get(tableIndex),
                                relocate.getSetColumnSourceMappings().get(tableIndex),
                                relocate.getSetColumnMetas().get(tableIndex), checkJsonByStringCompare);
                            if (rowSet == null) {
                                continue;
                            }
                            useRowSet = true;
                        } else {
                            useRowSet = false;
                        }

                        // calculate affected rows by comparing all changed columns
                        if (!relocateEc.isClientFoundRows()) {
                            // targetMap和sourceMap中只包含了更新的列，不包含 ON UPDATE TIMESTAMP 列，所以不会受自动更新列影响
                            final Mapping targetMap = relocate.getSetColumnTargetMappings().get(tableIndex);
                            final Mapping sourceMap = relocate.getSetColumnSourceMappings().get(tableIndex);
                            final List<ColumnMeta> metas = relocate.getSetColumnMetas().get(tableIndex);
                            for (List<Object> row : (useRowSet ? rowSet.getRows() : distinctValues)) {
                                affectRows +=
                                    identicalRow(row, targetMap, sourceMap, metas, checkJsonByStringCompare) ? 0 : 1;
                            }
                        }

                        for (RelocateWriter w : relocate.getRelocateWriterMap().get(tableIndex)) {
                            execute(w, rowSet, relocateEc);
                        }

                        for (DistinctWriter w : relocate.getModifyWriterMap().get(tableIndex)) {
                            execute(w, rowSet, relocateEc);
                        }
                    }
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

    protected static boolean identicalRow(List<Object> row, Mapping setColumnTargetMapping,
                                          Mapping setColumnSourceMapping, List<ColumnMeta> setColumnMetas,
                                          boolean checkJsonByStringCompare) {
        final List<Object> targets = Mappings.permute(row, setColumnTargetMapping);
        final List<Object> sources = Mappings.permute(row, setColumnSourceMapping);
        final GroupKey targetKey = new GroupKey(targets.toArray(), setColumnMetas);
        final GroupKey sourceKey = new GroupKey(sources.toArray(), setColumnMetas);
        return sourceKey.equalsForUpdate(targetKey, checkJsonByStringCompare);
    }

    public static RowSet buildChangedRowSet(List<List<Object>> values, List<ColumnMeta> returnColumns,
                                            Mapping setColumnTargetMapping, Mapping setColumnSourceMapping,
                                            List<ColumnMeta> setColumnMetas, boolean checkJsonByStringCompare) {
        final List<List<Object>> changedValues = new ArrayList<>();
        for (List<Object> row : values) {
            final List<Object> targets = Mappings.permute(row, setColumnTargetMapping);
            final List<Object> sources = Mappings.permute(row, setColumnSourceMapping);
            final GroupKey targetKey = new GroupKey(targets.toArray(), setColumnMetas);
            final GroupKey sourceKey = new GroupKey(sources.toArray(), setColumnMetas);
            if (!targetKey.equalsForUpdate(sourceKey, checkJsonByStringCompare)) {
                changedValues.add(row);
            }
        }
        if (changedValues.size() == 0) {
            return null;
        }
        return new RowSet(changedValues, returnColumns);
    }

    private int doRelocateExecuteMulti(LogicalRelocate relocate, ExecutionContext executionContext,
                                       List<List<Object>> someValues, Cursor selectCursor,
                                       long batchSize, MemoryPool selectValuesPool, MemoryAllocatorCtx memoryAllocator,
                                       long memoryOfOneRow, boolean haveGeneratedColumn) {
        final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();
        //通过MemoryControlByBlocked控制内存大小，防止内存占用
        BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
        MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
        ParallelExecutor parallelExecutor = createRelocateParallelExecutor(executionContext, relocate, returnColumns,
            selectValues, memoryControl);
        long valuesSize = memoryAllocator.getAllAllocated();
        parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());
        int affectRows = doRelocateParallelExecute(relocate, executionContext, parallelExecutor, someValues, valuesSize,
            selectCursor, batchSize, memoryOfOneRow, haveGeneratedColumn);
        executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
        return affectRows;

    }

    private ParallelExecutor createRelocateParallelExecutor(ExecutionContext ec, LogicalRelocate relocate,
                                                            List<ColumnMeta> returnColumns,
                                                            BlockingQueue<List<List<Object>>> selectValues,
                                                            MemoryControlByBlocked memoryControl) {
        String schemaName = relocate.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }

        boolean useTrans = ec.getTransaction() instanceof IDistributedTransaction;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        //modify可能会更新多个逻辑表，所以有一个表不是单库单表就不是单库单表；groupNames是所有表的去重并集
        boolean isSingleTable = true;
        List<String> tables = relocate.getTargetTableNames();
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

        LoggerFactory.getLogger(LogicalRelocateHandler.class).info(
            "Relocate select by ParallelExecutor, useTrans: " + useTrans + "; logicalThreads: " + logicalThreads
                + "; physicalThreads: " + physicalThreads);

        ParallelExecutor parallelExecutor = new ParallelExecutor(memoryControl);
        List<ExecuteJob> executeJobs = new ArrayList<>();

        for (int i = 0; i < logicalThreads; i++) {
            ExecuteJob executeJob = new LogicalRelocateExecuteJob(ec, parallelExecutor, relocate, returnColumns);
            executeJobs.add(executeJob);
        }

        parallelExecutor.createGroupRelQueue(ec, physicalThreads, phyParallelSet, !useTrans);
        parallelExecutor.setParam(selectValues, executeJobs);
        return parallelExecutor;
    }

    public int doRelocateParallelExecute(LogicalRelocate relocate, ExecutionContext executionContext,
                                         ParallelExecutor parallelExecutor, List<List<Object>> someValues,
                                         long someValuesSize, Cursor selectCursor,
                                         long batchSize, long memoryOfOneRow, boolean haveGeneratedColumn) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
            RelNode input = relocate.getInput();
            final List<Integer> autoIncColumns = relocate.getAutoIncColumns();
            int offset = input.getRowType().getFieldList().size() - relocate.getUpdateColumnList().size();
            final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode());
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

                if (haveGeneratedColumn) {
                    LogicalModifyHandler.evalGeneratedColumns(relocate, values, relocate.getEvalRowColumnMetas(),
                        relocate.getInputToEvalFieldMappings(), relocate.getGenColRexNodes(), executionContext);
                }

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
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
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
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

    protected void beforeModifyCheck(LogicalRelocate logicalRelocate, String schemaName, String targetTable,
                                     ExecutionContext executionContext, List<List<Object>> values) {
        int depth = 1;
        int index = 0;

        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = logicalRelocate.getFkPlans();

        final Map<Integer, DistinctWriter> primaryDistinctWriter = logicalRelocate.getPrimaryDistinctWriter();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = logicalRelocate.getPrimaryRelocateWriter();

        for (Integer tableIndex : logicalRelocate.getSetColumnMetas().keySet()) {
            final RelOptTable table = logicalRelocate.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = executionContext.getSchemaManager(qn.left).getTable(qn.right);

            DistinctWriter writer = primaryRelocateWriter.containsKey(tableIndex) ?
                primaryRelocateWriter.get(tableIndex).getModifyWriter() :
                primaryDistinctWriter.get(tableIndex);
            int columnCnt = tableMeta.getAllColumns().size();

            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < columnCnt; i++) {
                    row.add(value.get(i + index));
                }
                rows.add(row);
            }
            index += columnCnt;

            if (logicalRelocate.getOperation() == TableModify.Operation.DELETE) {
                LogicalModify modify = null;

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
        }

    }

}
