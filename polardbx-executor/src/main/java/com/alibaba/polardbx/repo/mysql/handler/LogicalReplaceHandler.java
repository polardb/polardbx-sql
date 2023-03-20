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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.NewGroupKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildGroupKeys;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildNewGroupKeys;
import static com.alibaba.polardbx.optimizer.utils.RexUtils.buildRowValue;

/**
 * @author chenmo.cm
 */
public class LogicalReplaceHandler extends LogicalInsertIgnoreHandler {
    public LogicalReplaceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected int doExecute(LogicalInsert insert, ExecutionContext executionContext,
                            LogicalInsert.HandlerParams handlerParams) {
        final LogicalReplace replace = (LogicalReplace) insert;
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final RelNode input = replace.getInput();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows = 0;
        if (input instanceof LogicalDynamicValues) {
            // For batch replace, change params index.
            if (replace.getBatchSize() > 0) {
                replace.buildParamsForBatch(executionContext);
            }
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);

        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        // Replace with NODE/SCAN hint specified
        if (replace.hasHint()) {
            final List<RelNode> inputs = replaceSeqAndBuildPhyPlan(replace, executionContext, handlerParams);
            return executePhysicalPlan(inputs, executionContext, schemaName, isBroadcast);
        }

        // Append parameter for RexCallParam and RexSequenceParam
        RexUtils.updateParam(replace, executionContext, false, handlerParams);

        // Build ExecutionContext for replace
        final ExecutionContext replaceEc = executionContext.copy();

        // Try to exec by pushDown policy for scale-out
        Integer execAffectRow = tryPushDownExecute(replace, schemaName, tableName, replaceEc);
        if (execAffectRow != null) {
            return execAffectRow;
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        final RelDataType selectRowType = getRowTypeForDuplicateCheck(replace);

        try {
            Map<String, List<List<String>>> ukGroupByTable = replace.getUkGroupByTable();
            Map<String, List<String>> localIndexPhyName = replace.getLocalIndexPhyName();
            List<List<Object>> convertedValues = new ArrayList<>();
            boolean usePartFieldChecker = replace.isUsePartFieldChecker() && executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_NEW_DUP_CHECKER);
            List<List<Object>> selectedRows =
                getDuplicatedValues(replace, LockMode.EXCLUSIVE_LOCK, executionContext, ukGroupByTable,
                    localIndexPhyName, (rowCount) -> memoryAllocator.allocateReservedMemory(
                        MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType)), selectRowType, false,
                    handlerParams, convertedValues, usePartFieldChecker);
            // Bind insert rows to operation
            // Duplicate might exists between replace values
            final List<Map<Integer, ParameterContext>> batchParams = replaceEc.getParams().getBatchParameters();
            final List<DuplicateCheckResult> classifiedRows = new ArrayList<>(
                bindInsertRows(replace, selectedRows, batchParams, convertedValues, executionContext,
                    usePartFieldChecker));

            try {
                if (gsiConcurrentWrite) {
                    affectRows = concurrentExecute(replace, classifiedRows, replaceEc);
                } else {
                    affectRows = sequentialExecute(replace, classifiedRows, replaceEc);
                }
            } catch (Throwable e) {
                handleException(executionContext, e, GeneralUtil.isNotEmpty(replace.getGsiInsertWriters()));
            }
        } finally {
            selectValuesPool.destroy();
        }
        // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
        executionContext.setPhySqlId(replaceEc.getPhySqlId() + 1);
        return affectRows;
    }

    private int concurrentExecute(LogicalReplace replace, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext) {
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows;

        final ExecutionContext deduplicatedEc = executionContext.copy();

        final RowClassifier rowClassifier = buildRowClassifier(replace, executionContext, schemaName);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryReplacePlans = new ArrayList<>();
        final List<RelNode> gsiDeletePlans = new ArrayList<>();
        final List<RelNode> gsiInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplacePlans = new ArrayList<>();
        final List<RelNode> replicatedDeletePlans = new ArrayList<>();
        final List<RelNode> replicatedInsertPlans = new ArrayList<>();
        final List<RelNode> replicatedReplacePlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedDeletePlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedReplacePlans = new ArrayList<>();

        final List<DuplicateCheckResult> inputValues = new ArrayList<>();
        final Function<DistinctWriter, SourceRows> rowBuilder = (wr) -> SourceRows.createFromValues(classifiedRows);

        // 1. Build physical plans
        final ReplaceRelocateWriter primaryRelocateWriter = replace.getPrimaryRelocateWriter();
        // Primary physical plans
        if (primaryRelocateWriter != null) {
            final SourceRows sourceRows = primaryRelocateWriter
                .getInput(executionContext, deduplicatedEc, rowBuilder, rowClassifier, primaryDeletePlans,
                    primaryInsertPlans, primaryReplacePlans, replicatedDeletePlans, replicatedInsertPlans,
                    replicatedReplacePlans);
            inputValues.addAll(sourceRows.valueRows);
        }

        // Gsi physical plans
        replace.getGsiRelocateWriters().forEach(writer -> {
            writer.getInput(executionContext, deduplicatedEc, rowBuilder, rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiReplacePlans, gsiReplicatedDeletePlans, gsiReplicatedInsertPlans,
                gsiReplicatedReplacePlans);
        });

        // 2. Execute
        // Default concurrent policy is group concurrent
        final List<RelNode> allDelete = new ArrayList<>(primaryDeletePlans);
        allDelete.addAll(primaryReplacePlans);
        allDelete.addAll(replicatedDeletePlans);
        allDelete.addAll(replicatedReplacePlans);
        allDelete.addAll(gsiDeletePlans);
        allDelete.addAll(gsiReplacePlans);
        allDelete.addAll(gsiReplicatedDeletePlans);
        allDelete.addAll(gsiReplicatedReplacePlans);
        if (!allDelete.isEmpty()) {
            executePhysicalPlan(allDelete, executionContext, schemaName, isBroadcast);
        }

        final List<RelNode> allInsert = new ArrayList<>(primaryInsertPlans);
        allInsert.addAll(gsiInsertPlans);
        allInsert.addAll(replicatedInsertPlans);
        allInsert.addAll(gsiReplicatedInsertPlans);
        if (!allInsert.isEmpty()) {
            executePhysicalPlan(allInsert, executionContext, schemaName, isBroadcast);
        }

        affectRows = inputValues.stream().mapToInt(row -> row.affectedRows).sum();

        return affectRows;
    }

    private int sequentialExecute(LogicalReplace replace, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext) {
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows;
        int deleteAffectedRows = 0;
        int insertAffectedRows = 0;

        final ExecutionContext deduplicatedEc = executionContext.copy();

        final RowClassifier rowClassifier = buildRowClassifier(replace, executionContext, schemaName);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryReplacePlans = new ArrayList<>();
        final List<RelNode> replicatedDeletePlans = new ArrayList<>();
        final List<RelNode> replicatedInsertPlans = new ArrayList<>();
        final List<RelNode> replicatedReplacePlans = new ArrayList<>();

        // Primary physical plans
        replace.getPrimaryRelocateWriter().getInput(executionContext, deduplicatedEc,
            (wr) -> SourceRows.createFromValues(classifiedRows), rowClassifier, primaryDeletePlans,
            primaryInsertPlans, primaryReplacePlans, replicatedDeletePlans, replicatedInsertPlans,
            replicatedReplacePlans);

        if (!primaryReplacePlans.isEmpty()) {
            deleteAffectedRows +=
                executePhysicalPlan(primaryReplacePlans, executionContext, schemaName, isBroadcast);
        }

        if (!primaryDeletePlans.isEmpty()) {
            deleteAffectedRows +=
                executePhysicalPlan(primaryDeletePlans, executionContext, schemaName, isBroadcast);
        }

        if (!primaryInsertPlans.isEmpty()) {
            insertAffectedRows =
                executePhysicalPlan(primaryInsertPlans, executionContext, schemaName, isBroadcast);
        }

        if (!replicatedReplacePlans.isEmpty()) {
            executePhysicalPlan(replicatedReplacePlans, executionContext, schemaName, isBroadcast);
        }
        if (!replicatedDeletePlans.isEmpty()) {
            executePhysicalPlan(replicatedDeletePlans, executionContext, schemaName, isBroadcast);
        }

        if (!replicatedInsertPlans.isEmpty()) {
            executePhysicalPlan(replicatedInsertPlans, executionContext, schemaName, isBroadcast);
        }

        // Gsi physical plans
        replace.getGsiRelocateWriters().forEach(writer -> {
            final List<RelNode> gsiDeletePlans = new ArrayList<>();
            final List<RelNode> gsiInsertPlans = new ArrayList<>();
            final List<RelNode> gsiReplacePlans = new ArrayList<>();
            final List<RelNode> gsiReplicateDeletePlans = new ArrayList<>();
            final List<RelNode> gsiReplicateInsertPlans = new ArrayList<>();
            final List<RelNode> gsiReplicateReplacePlans = new ArrayList<>();

            writer.getInput(executionContext, deduplicatedEc,
                (wr) -> SourceRows.createFromValues(classifiedRows), rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiReplacePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                gsiReplicateReplacePlans);

            if (!gsiReplacePlans.isEmpty()) {
                executePhysicalPlan(gsiReplacePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiDeletePlans.isEmpty()) {
                executePhysicalPlan(gsiDeletePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiInsertPlans.isEmpty()) {
                executePhysicalPlan(gsiInsertPlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateReplacePlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateReplacePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateDeletePlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateDeletePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateInsertPlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateInsertPlans, executionContext, schemaName, isBroadcast);
            }
        });

        affectRows = deleteAffectedRows + insertAffectedRows;
        return affectRows;
    }

    protected RowClassifier buildRowClassifier(final LogicalInsert insertOrReplace,
                                               final ExecutionContext ec, final String schemaName) {
        return (writer, sourceRows, result) -> {
            if (null == result) {
                return result;
            }

            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);

            final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalPartitionKeyChecker =
                getIdenticalPartitionKeyChecker(input, ec, schemaName);

            // Classify insert/replace rows for UPDATE/REPLACE/DELETE/INSERT
            writer.classify(identicalPartitionKeyChecker, sourceRows, ec, result);

            return result;
        };
    }

    /**
     * Return a lambda expression for checking partition keys of two input row are identical
     */
    protected BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> getIdenticalPartitionKeyChecker(
        LogicalDynamicValues input,
        ExecutionContext executionContext,
        String schemaName) {
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);

        // Checker for identical partition key
        return (w, pair) -> {
            // Use PartitionField to compare in new partition table
            final RelocateWriter rw = w.unwrap(RelocateWriter.class);
            final boolean usePartFieldChecker = rw.isUsePartFieldChecker() &&
                executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_NEW_SK_CHECKER);

            final List<Object> oldValue = pair.left;
            final Map<Integer, ParameterContext> newValue = pair.right;

            final Mapping skSourceMapping = rw.getIdentifierKeySourceMapping();
            final Object[] sourceSkValue = Mappings.permute(oldValue, skSourceMapping).toArray();

            final List<RexNode> targetSkRex = Mappings.permute(rexRow, skSourceMapping);
            final Object[] targetSkValue =
                targetSkRex.stream().map(rex -> RexUtils.getValueFromRexNode(rex, executionContext, newValue))
                    .toArray();
            final List<ColumnMeta> skMetas = rw.getIdentifierKeyMetas();

            if (usePartFieldChecker) {
                try {
                    final NewGroupKey sourceSkGk = new NewGroupKey(Arrays.asList(sourceSkValue),
                        skMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()), skMetas,
                        true, executionContext);
                    final NewGroupKey targetSkGk = new NewGroupKey(Arrays.asList(targetSkValue),
                        targetSkRex.stream().map(rx -> DataTypeUtil.calciteToDrdsType(rx.getType()))
                            .collect(Collectors.toList()), skMetas, true, executionContext);

                    return sourceSkGk.equals(targetSkGk);
                } catch (Throwable e) {
                    // Maybe value can not be cast, just use DELETE + INSERT to be safe
                    LoggerFactory.getLogger(LogicalReplaceHandler.class).warn("new sk checker failed, cause by " + e);
                }
                return false;
            } else {
                final GroupKey sourceSkGk = new GroupKey(sourceSkValue, skMetas);
                final GroupKey targetSkGk = new GroupKey(targetSkValue, skMetas);

                // GroupKey(NULL).equals(GroupKey(NULL)) returns true
                return sourceSkGk.equals(targetSkGk);
            }
        };
    }

    protected List<DuplicateCheckResult> bindInsertRows(LogicalReplace replace, List<List<Object>> selectedRows,
                                                        List<Map<Integer, ParameterContext>> currentBatchParameters,
                                                        List<List<Object>> convertedValues,
                                                        ExecutionContext executionContext,
                                                        boolean usePartFieldChecker) {
        final List<List<Integer>> beforeUkMapping = replace.getBeforeUkMapping();
        final List<List<Integer>> afterUkMapping = replace.getAfterUkMapping();
        final List<List<ColumnMeta>> ukColumnMetas = replace.getUkColumnMetas();
        final List<ColumnMeta> rowColumnMetas = replace.getRowColumnMetaList();
        final LogicalDynamicValues input = RelUtils.getRelInput(replace);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final boolean multiUk = beforeUkMapping.size() > 1;

        // 1. Init before rows
        final Map<Integer, DuplicateCheckRow> checkerRows = new HashMap<>();
        final Map<Integer, List<DuplicateCheckRow>> insertDeleteMap = new HashMap<>();
        final List<Map<GroupKey, List<DuplicateCheckRow>>> checkers = usePartFieldChecker ?
            buildDuplicateCheckersWithNewGroupKey(selectedRows, beforeUkMapping, ukColumnMetas, checkerRows,
                executionContext) : buildDuplicateCheckers(selectedRows, beforeUkMapping, ukColumnMetas, checkerRows);

        final boolean skipIdenticalRowCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_ROW_CHECK) || (
                replace.isHasJsonColumn() && executionContext.getParamManager()
                    .getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_JSON_ROW_CHECK));

        // 2. Check each insert row
        Ord.zip(currentBatchParameters).forEach(o -> {
            final Integer rowIndex = o.getKey();
            final Map<Integer, ParameterContext> newRow = o.getValue();

            // Build group key
            final List<GroupKey> newGroupKeys = usePartFieldChecker ?
                buildNewGroupKeys(afterUkMapping, ukColumnMetas, convertedValues.get(rowIndex)::get, executionContext) :
                buildGroupKeys(afterUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, newRow));

            // Collect all duplicated rows
            final Set<Integer> duplicatedIndexSet = new HashSet<>();
            boolean duplicated = false;
            for (Ord<Map<GroupKey, List<DuplicateCheckRow>>> ord : Ord.zip(checkers)) {
                final Integer ukIndex = ord.getKey();
                final Map<GroupKey, List<DuplicateCheckRow>> checker = ord.getValue();

                // Might duplicated with existing row in table or previously inserted row
                duplicated |= ExecUtils.duplicated(checker, newGroupKeys.get(ukIndex),
                    (k, v) -> v.forEach(chk -> duplicatedIndexSet.add(chk.rowIndex)));
            }

            // Build row value for future duplicate check
            final DuplicateCheckRow newCheckRow = new DuplicateCheckRow();
            newCheckRow.after = buildRowValue(rexRow, null, newRow);
            newCheckRow.duplicated = duplicated;
            newCheckRow.doInsert = !duplicated;
            newCheckRow.insertParam = newRow;
            newCheckRow.keyList = newGroupKeys;
            newCheckRow.rowIndex = rowIndex;
            newCheckRow.affectedRows = 1;

            final List<DuplicateCheckRow> deleteRows = new ArrayList<>();
            if (duplicated) {
                // Duplicated with existing row in table or previously inserted row

                // Remove duplicated checker rows
                for (Map<GroupKey, List<DuplicateCheckRow>> checker : checkers) {
                    final Map<GroupKey, List<DuplicateCheckRow>> newChecker =
                        usePartFieldChecker ? new HashMap<>() : new TreeMap<>();

                    checker.forEach((k, v) -> {
                        final List<DuplicateCheckRow> newValue =
                            v.stream().filter(row -> !duplicatedIndexSet.contains(row.rowIndex))
                                .collect(Collectors.toList());
                        if (!newValue.isEmpty()) {
                            newChecker.put(k, newValue);
                        }
                    });

                    checker.clear();
                    checker.putAll(newChecker);
                }

                // Update and remove duplicate check row
                duplicatedIndexSet.stream().sorted().forEach(i -> {
                    final List<DuplicateCheckRow> removed = insertDeleteMap.remove(i);
                    if (null != removed) {
                        // Previously inserted row already replaced some row
                        deleteRows.addAll(removed);
                    }

                    final DuplicateCheckRow duplicatedRow = checkerRows.get(i);

                    // Compare entire row
                    if (skipIdenticalRowCheck || multiUk || !identicalRow(duplicatedRow.after, newCheckRow.after,
                        rowColumnMetas)) {
                        duplicatedRow.affectedRows++;
                    }

                    // Add duplicated row
                    deleteRows.add(duplicatedRow);

                    // Simulate replace
                    deleteRows.forEach(row -> row.doReplace(newCheckRow));
                });
            }

            // Add new row
            checkerRows.put(newCheckRow.rowIndex, newCheckRow);
            insertDeleteMap.put(newCheckRow.rowIndex, deleteRows);
            for (Ord<GroupKey> ord : Ord.zip(newGroupKeys)) {
                final Integer ukIndex = ord.getKey();
                final GroupKey newKey = ord.getValue();
                checkers.get(ukIndex).computeIfAbsent(newKey, (k) -> new ArrayList<>()).add(newCheckRow);
            }

        });

        // 3. Build result
        final List<DuplicateCheckResult> result = new ArrayList<>();
        insertDeleteMap.forEach((k, v) -> {
            final DuplicateCheckRow insertRow = checkerRows.get(k);

            if (insertRow.duplicated) {
                // Duplicated row
                final boolean couldBeReplace = v.stream().mapToInt(row -> row.fromTable() ? 1 : 0).sum() == 1;

                if (couldBeReplace) {
                    // REPLACE
                    DuplicateCheckRow duplicatedRow = null;

                    int amendedAffectedRows = 0;
                    for (DuplicateCheckRow row : v) {
                        if (row.fromTable()) {
                            duplicatedRow = row;
                        } else {
                            amendedAffectedRows += row.affectedRows;
                        }
                    }

                    assert null != duplicatedRow && !duplicatedRow.doInsert;

                    final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                    rowResult.insertParam = insertRow.insertParam;
                    rowResult.duplicated = true;
                    rowResult.doInsert = true;
                    rowResult.doReplace = true;

                    rowResult.before = duplicatedRow.before;
                    rowResult.after = insertRow.after;

                    // Compare entire row
                    rowResult.affectedRows = duplicatedRow.affectedRows + insertRow.affectedRows + amendedAffectedRows;

                    result.add(rowResult);
                } else {
                    // DELETE + INSERT
                    int amendedAffectedRows = 0;
                    for (DuplicateCheckRow duplicatedRow : v) {
                        final boolean needDelete = duplicatedRow.fromTable();

                        if (needDelete) {
                            // Build DELETE for every row in table that has been replaced
                            final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                            rowResult.duplicated = true;
                            rowResult.affectedRows = 1;

                            rowResult.before = duplicatedRow.before;
                            result.add(rowResult);
                        } else {
                            amendedAffectedRows += duplicatedRow.affectedRows;
                        }
                    }

                    assert insertRow.doInsert;

                    final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                    rowResult.insertParam = insertRow.insertParam;
                    // Just do insert
                    rowResult.duplicated = false;
                    rowResult.doInsert = true;
                    rowResult.affectedRows = insertRow.affectedRows + amendedAffectedRows;

                    result.add(rowResult);
                }
            } else {
                // New row
                assert v.isEmpty();

                final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                rowResult.insertParam = insertRow.insertParam;
                rowResult.duplicated = false;
                rowResult.doInsert = true;
                rowResult.affectedRows = 1;

                result.add(rowResult);
            }

        });

        return result;
    }

    /**
     * Builder duplicate checker for REPLACE
     *
     * @param selectedRows Selected rows
     * @param ukMapping Column index of each uk
     * @param ukColumnMetas Meta of uk columns
     * @return UkList[ GroupKeyMap[ group key, list of selected rows with same value on this uk ]]
     */
    private static List<Map<GroupKey, List<DuplicateCheckRow>>> buildDuplicateCheckers(List<List<Object>> selectedRows,
                                                                                       List<List<Integer>> ukMapping,
                                                                                       List<List<ColumnMeta>> ukColumnMetas,
                                                                                       Map<Integer, DuplicateCheckRow> outDuplicateCheckRow) {
        final List<Map<GroupKey, List<DuplicateCheckRow>>> result = new ArrayList<>();
        IntStream.range(0, ukMapping.size()).forEach(i -> result.add(new TreeMap<>()));
        final int selectedRowCount = selectedRows.size();

        Ord.zip(selectedRows).forEach(ord -> {
            final Integer rowIndex = ord.getKey();
            final List<Object> row = ord.getValue();
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildGroupKeys(ukMapping, ukColumnMetas, row::get);
            duplicateCheckRow.duplicated = true;
            duplicateCheckRow.rowIndex = rowIndex - selectedRowCount;
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            if (null != outDuplicateCheckRow) {
                outDuplicateCheckRow.put(duplicateCheckRow.rowIndex, duplicateCheckRow);
            }

            for (Ord<GroupKey> o : Ord.zip(duplicateCheckRow.keyList)) {
                final Integer ukIndex = o.getKey();
                final GroupKey groupKey = o.getValue();

                final Map<GroupKey, List<DuplicateCheckRow>> checkers = result.get(ukIndex);
                checkers.computeIfAbsent(groupKey, (k) -> new ArrayList<>()).add(duplicateCheckRow);
            }
        });

        return result;
    }

    private static List<Map<GroupKey, List<DuplicateCheckRow>>> buildDuplicateCheckersWithNewGroupKey(
        List<List<Object>> selectedRows,
        List<List<Integer>> ukMapping,
        List<List<ColumnMeta>> ukColumnMetas,
        Map<Integer, DuplicateCheckRow> outDuplicateCheckRow,
        ExecutionContext ec) {
        final List<Map<GroupKey, List<DuplicateCheckRow>>> result = new ArrayList<>();
        IntStream.range(0, ukMapping.size()).forEach(i -> result.add(new HashMap<>()));
        final int selectedRowCount = selectedRows.size();

        Ord.zip(selectedRows).forEach(ord -> {
            final Integer rowIndex = ord.getKey();
            final List<Object> row = ord.getValue();
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildNewGroupKeys(ukMapping, ukColumnMetas, row::get, ec);
            duplicateCheckRow.duplicated = true;
            duplicateCheckRow.rowIndex = rowIndex - selectedRowCount;
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            if (null != outDuplicateCheckRow) {
                outDuplicateCheckRow.put(duplicateCheckRow.rowIndex, duplicateCheckRow);
            }

            for (Ord<GroupKey> o : Ord.zip(duplicateCheckRow.keyList)) {
                final Integer ukIndex = o.getKey();
                final GroupKey groupKey = o.getValue();

                final Map<GroupKey, List<DuplicateCheckRow>> checkers = result.get(ukIndex);
                checkers.computeIfAbsent(groupKey, (k) -> new ArrayList<>()).add(duplicateCheckRow);
            }
        });

        return result;
    }

    private static class DuplicateCheckRow {
        /**
         * Origin row value from table, null if to-be-inserted row not duplicate with existing row in table
         */
        public List<Object> before;
        /**
         * Current row value, might have been updated several times
         */
        public List<Object> after;
        /**
         * Parameters for insert
         */
        public Map<Integer, ParameterContext> insertParam;

        public boolean doInsert = false;
        public boolean duplicated = false;
        /**
         * Index in selected or insert rows
         */
        public int rowIndex;
        public int replacedBy;
        /**
         * Group keys of all unique key
         */
        public List<GroupKey> keyList;

        public int affectedRows = 0;

        public DuplicateCheckRow() {
        }

        /**
         * Update duplicate check row, rows might duplicate with previously inserted row
         *
         * @param newCheckRow Parameter row of insert
         */
        public void doReplace(DuplicateCheckRow newCheckRow) {
            this.duplicated = true;
            this.doInsert = false;
            this.replacedBy = newCheckRow.rowIndex;
            this.after = newCheckRow.after;
        }

        public boolean fromTable() {
            return rowIndex < 0;
        }
    }
}
