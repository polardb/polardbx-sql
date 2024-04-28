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

package com.alibaba.polardbx.repo.mysql.handler.execute;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.NewGroupKey;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.repo.mysql.handler.LogicalRelocateHandler;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class LogicalRelocateExecuteJob extends ExecuteJob {
    private final LogicalRelocate relocate;
    private final List<ColumnMeta> returnColumns;

    public LogicalRelocateExecuteJob(ExecutionContext executionContext, ParallelExecutor parallelExecutor,
                                     LogicalRelocate relocate, List<ColumnMeta> returnColumns) {
        super(executionContext, parallelExecutor);
        this.relocate = relocate;
        this.returnColumns = returnColumns;

        this.schemaName = relocate.getSchemaName();
        if (StringUtils.isEmpty(this.schemaName)) {
            this.schemaName = executionContext.getSchemaName();
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(this.schemaName, this.executionContext);
    }

    @Override
    public void execute(List<List<Object>> values, long memorySize) throws Exception {
        int affectRows = 0;

        final Map<Integer, DistinctWriter> primaryDistinctWriter = relocate.getPrimaryDistinctWriter();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = relocate.getPrimaryRelocateWriter();

        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        for (Integer tableIndex : relocate.getSetColumnMetas().keySet()) {
            RowSet rowSet = new RowSet(values, returnColumns);
            DistinctWriter dw = primaryRelocateWriter.containsKey(tableIndex) ?
                primaryRelocateWriter.get(tableIndex).getDeleteWriter() :
                primaryDistinctWriter.get(tableIndex);
            List<List<Object>> distinctValues = rowSet.distinctRowSetWithoutNull(dw);

            if (executionContext.isClientFoundRows()) {
                affectRows += distinctValues.size();
            }

            final boolean useRowSet;
            if (skipUnchangedRow && relocate.getModifySkOnlyMap().get(tableIndex)) {
                rowSet = LogicalRelocateHandler.buildChangedRowSet(distinctValues, returnColumns,
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
            if (!executionContext.isClientFoundRows()) {
                // targetMap和sourceMap中只包含了更新的列，不包含 ON UPDATE TIMESTAMP 列，所以不会受自动更新列影响
                final Mapping targetMap = relocate.getSetColumnTargetMappings().get(tableIndex);
                final Mapping sourceMap = relocate.getSetColumnSourceMappings().get(tableIndex);
                final List<ColumnMeta> metas = relocate.getSetColumnMetas().get(tableIndex);
                for (List<Object> row : (useRowSet ? rowSet.getRows() : distinctValues)) {
                    affectRows += identicalRow(row, targetMap, sourceMap, metas, checkJsonByStringCompare) ? 0 : 1;
                }
            }

            for (RelocateWriter w : relocate.getRelocateWriterMap().get(tableIndex)) {
                int result = executeRelocateWriter(w, rowSet);
                if (result == -1) {
                    return;
                }
            }

            for (DistinctWriter w : relocate.getModifyWriterMap().get(tableIndex)) {
                int result = executeDistinctWriter(w, rowSet);
                if (result == -1) {
                    return;
                }
            }
        }

        parallelExecutor.getAffectRows().addAndGet(affectRows);
        parallelExecutor.getMemoryControl().release(memorySize);

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

    private int executeRelocateWriter(RelocateWriter relocateWriter, RowSet rowSet) throws Exception {
        final ExecutionContext insertEc = executionContext.copy();

        final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> skComparator =
            (w, p) -> {
                // Use PartitionField to compare in new partition table
                final List<Object> row = p.getKey();
                final RelocateWriter rw = w.unwrap(RelocateWriter.class);
                final boolean usePartFieldChecker = rw.isUsePartFieldChecker() &&
                    executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_NEW_SK_CHECKER);
                final boolean checkJsonByStringCompare =
                    executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

                final List<Object> skSources = Mappings.permute(row, rw.getIdentifierKeySourceMapping());
                final List<Object> skTargets = Mappings.permute(row, rw.getIdentifierKeyTargetMapping());

                if (usePartFieldChecker) {
                    final List<ColumnMeta> sourceColMetas =
                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeySourceMapping());
//                    final List<ColumnMeta> targetColMetas =
//                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeyTargetMapping());

                    try {
                        final NewGroupKey skSourceKey = new NewGroupKey(skSources,
                            sourceColMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);
                        final NewGroupKey skTargetKey = new NewGroupKey(skTargets,
                            skTargets.stream().map(DataTypeUtil::getTypeOfObject).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);

                        return skSourceKey.equals(skTargetKey);
                    } catch (Throwable e) {
                        if (!relocateWriter.printed &&
                            executionContext.getParamManager().getBoolean(ConnectionParams.DML_PRINT_CHECKER_ERROR)) {
                            // Maybe value can not be cast, just use DELETE + INSERT to be safe
                            EventLogger.log(EventType.DML_ERROR,
                                executionContext.getTraceId() + " new sk checker failed, cause by " + e);
                            LoggerFactory.getLogger(LogicalRelocateExecuteJob.class).warn(e);
                            relocateWriter.printed = true;
                        }
                    }
                    return false;
                } else {
                    final GroupKey skTargetKey = new GroupKey(skTargets.toArray(), rw.getIdentifierKeyMetas());
                    final GroupKey skSourceKey = new GroupKey(skSources.toArray(), rw.getIdentifierKeyMetas());

                    return skTargetKey.equalsForUpdate(skSourceKey, checkJsonByStringCompare);
                }
            };

        final List<RelNode> deletePlans = new ArrayList<>();
        final List<RelNode> insertPlans = new ArrayList<>();
        final List<RelNode> updatePlans = new ArrayList<>();
        final List<RelNode> replicateDeletePlans = new ArrayList<>();
        final List<RelNode> replicateInsertPlans = new ArrayList<>();
        final List<RelNode> replicateUpdatePlans = new ArrayList<>();
        final SourceRows distinctRows = relocateWriter.getInput(executionContext, insertEc,
            (w) -> SourceRows.createFromSelect(rowSet.distinctRowSetWithoutNull(w)),
            (writer, selectedRows, result) -> writer.classify(skComparator, selectedRows, executionContext, result),
            deletePlans, insertPlans, updatePlans, replicateDeletePlans, replicateInsertPlans, replicateUpdatePlans);

        // Execute update
        updatePlans.addAll(replicateUpdatePlans);
        if (!updatePlans.isEmpty()) {
            ExecutionContext updateEc = executionContext.copy();
            LogicalJob job = new LogicalJob(parallelExecutor, updateEc);
            job.setAllPhyPlan(updatePlans);
            job.setAffectRowsFunction(ExecuteJob::noAffectRows);
            SettableFuture<Boolean> future = job.addListener();
            scheduleJob(job);
            //等待该任务执行完
            boolean result = future.get();
            if (!result) {
                //任务出错，直接返回
                return -1;
            }
        }

        // Execute delete
        deletePlans.addAll(replicateDeletePlans);
        if (!deletePlans.isEmpty()) {
            ExecutionContext deleteEc = executionContext.copy();
            LogicalJob job = new LogicalJob(parallelExecutor, deleteEc);
            job.setAllPhyPlan(deletePlans);
            job.setAffectRowsFunction(ExecuteJob::noAffectRows);
            SettableFuture<Boolean> future = job.addListener();
            scheduleJob(job);
            //等待该任务执行完
            boolean result = future.get();
            if (!result) {
                //任务出错，直接返回
                return -1;
            }
        }

        // Execute insert
        insertPlans.addAll(replicateInsertPlans);
        if (!insertPlans.isEmpty()) {
            LogicalJob job = new LogicalJob(parallelExecutor, insertEc);
            job.setAllPhyPlan(insertPlans);
            job.setAffectRowsFunction(ExecuteJob::noAffectRows);
            SettableFuture<Boolean> future = job.addListener();
            scheduleJob(job);
            //等待该任务执行完
            boolean result = future.get();
            if (!result) {
                //任务出错，直接返回
                return -1;
            }
        }

        return distinctRows.selectedRows.size();
    }

    private int executeDistinctWriter(DistinctWriter writer, RowSet rowSet) throws Exception {
        List<RelNode> inputs = writer.getInput(executionContext, rowSet::distinctRowSetWithoutNull);
        final List<RelNode> primaryPhyPlan =
            inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());
        final List<RelNode> allPhyPlan = new ArrayList<>(primaryPhyPlan);
        final List<RelNode> replicatePlans =
            inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());
        allPhyPlan.addAll(replicatePlans);

        boolean isBroadcast = writer instanceof BroadcastWriter;

        if (allPhyPlan.isEmpty()) {
            return 0;
        }
        ExecutionContext ec = executionContext.copy();
        LogicalJob job = new LogicalJob(parallelExecutor, ec);
        job.setAllPhyPlan(allPhyPlan);
        job.setAffectRowsFunction(ExecuteJob::noAffectRows);
        SettableFuture<Boolean> future = job.addListener();
        scheduleJob(job);
        //等待该任务执行完
        boolean result = future.get();
        if (!result) {
            //任务出错，直接返回
            return -1;
        }

        boolean multiWriteWithoutBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && !isBroadcast;
        boolean multiWriteWithBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && isBroadcast;

        if (multiWriteWithoutBroadcast) {
            return primaryPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows())
                .sum();
        } else if (multiWriteWithBroadcast) {
            return ((BaseQueryOperation) primaryPhyPlan.get(0)).getAffectedRows();
        } else {
            return allPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows()).sum();
        }
    }
}
