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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils.ReplaceValuesCall;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildGroupKeys;

/**
 * @author chenmo.cm
 */
public class LogicalUpsertHandler extends LogicalInsertIgnoreHandler {
    public LogicalUpsertHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected int doExecute(LogicalInsert insert, ExecutionContext executionContext,
                            LogicalInsert.HandlerParams handlerParams) {
        final LogicalUpsert upsert = (LogicalUpsert) insert;
        final String schemaName = upsert.getSchemaName();
        final String tableName = upsert.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        // For batch upsert, change params index.
        if (upsert.getBatchSize() > 0) {
            upsert.buildParamsForBatch(executionContext.getParams());
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);

        // Upsert with NODE/SCAN hint specified
        if (upsert.hasHint()) {
            final List<RelNode> inputs = replaceSeqAndBuildPhyPlan(upsert, executionContext, handlerParams);
            return executePhysicalPlan(inputs, executionContext, schemaName, isBroadcast);
        }

        // Append parameter for RexCallParam and RexSequenceParam
        RexUtils.updateParam(upsert, executionContext, false, handlerParams);

        // Build ExecutionContext for upsert
        final ExecutionContext upsertEc = executionContext.copy();

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        final RelDataType insertRowType = upsert.getInsertRowType();

        int affectRows = 0;
        try {
            Map<String, List<List<String>>> ukGroupByTable = upsert.getUkGroupByTable();
            List<List<Object>> selectedRows =
                getDuplicatedValues(upsert, SqlSelect.LockMode.EXCLUSIVE_LOCK, executionContext, ukGroupByTable,
                    (rowCount) -> memoryAllocator.allocateReservedMemory(
                        MemoryEstimator.calcSelectValuesMemCost(rowCount, insertRowType)), insertRowType, false,
                    handlerParams);

            // Bind insert rows to operation
            // Duplicate might exists between upsert values
            final List<DuplicateCheckResult> classifiedRows =
                new ArrayList<>(bindInsertRows(upsert, selectedRows, upsertEc));

            upsertEc.setPhySqlId(upsertEc.getPhySqlId() + 1);

            try {
                if (gsiConcurrentWrite) {
                    affectRows = concurrentExecute(upsert, classifiedRows, upsertEc);
                } else {
//                        affectRows = deletedRows + sequentialExecute(upsert, duplicateValues, upsertEc);
                }
            } catch (Throwable e) {
                handleException(executionContext, e, GeneralUtil.isNotEmpty(upsert.getGsiInsertWriters()));
            }
        } finally {
            selectValuesPool.destroy();
        }
        // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
        executionContext.setPhySqlId(upsertEc.getPhySqlId() + 1);
        return affectRows;
    }

    private int concurrentExecute(LogicalUpsert upsert, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext) {
        final String schemaName = upsert.getSchemaName();
        final String tableName = upsert.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows;

        final ExecutionContext deduplicatedEc = executionContext.copy();
        final RowClassifier rowClassifier = buildRowClassifier(upsert, executionContext);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryUpdatePlans = new ArrayList<>();
        final List<RelNode> gsiDeletePlans = new ArrayList<>();
        final List<RelNode> gsiInsertPlans = new ArrayList<>();
        final List<RelNode> gsiUpdatePlans = new ArrayList<>();
        final List<RelNode> replilcateDeletePlans = new ArrayList<>();
        final List<RelNode> replilcateInsertPlans = new ArrayList<>();
        final List<RelNode> replilcateUpdatePlans = new ArrayList<>();
        final List<RelNode> gsiReplicateDeletePlans = new ArrayList<>();
        final List<RelNode> gsiReplicateInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplicateUpdatePlans = new ArrayList<>();

        final List<DuplicateCheckResult> inputValues = new ArrayList<>();
        final SourceRows rowBuilder = SourceRows.createFromValues(classifiedRows);
        if (!upsert.isModifyPartitionKey()) {
            // Do not modify partition key

            // Primary physical plans
            final SourceRows sourceRows = upsert.getPrimaryUpsertWriter()
                .getInput(deduplicatedEc, rowBuilder, rowClassifier, primaryDeletePlans, primaryInsertPlans,
                    primaryUpdatePlans, replilcateDeletePlans, replilcateInsertPlans, replilcateUpdatePlans);

            inputValues.addAll(sourceRows.valueRows);

            // Gsi physical plans
            upsert.getGsiUpsertWriters()
                .forEach(writer -> writer.getInput(deduplicatedEc, rowBuilder, rowClassifier,
                    gsiDeletePlans, gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                    gsiReplicateUpdatePlans));

        } else {
            // Modify partition key

            // Primary physical plans
            SourceRows sourceRows;
            if (null != upsert.getPrimaryRelocateWriter()) {
                sourceRows =
                    upsert.getPrimaryRelocateWriter().getInput(executionContext, deduplicatedEc,
                        (wr) -> rowBuilder, rowClassifier, primaryDeletePlans,
                        primaryInsertPlans, primaryUpdatePlans, replilcateDeletePlans, replilcateInsertPlans,
                        replilcateUpdatePlans);
            } else {
                sourceRows = upsert.getPrimaryUpsertWriter()
                    .getInput(deduplicatedEc, rowBuilder,
                        rowClassifier, primaryDeletePlans, primaryInsertPlans, primaryUpdatePlans,
                        replilcateDeletePlans, replilcateInsertPlans,
                        replilcateUpdatePlans);
            }

            inputValues.addAll(sourceRows.valueRows);

            // Gsi physical plans
            upsert.getGsiUpsertWriters()
                .forEach(writer -> writer.getInput(deduplicatedEc, rowBuilder, rowClassifier,
                    gsiDeletePlans, gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                    gsiReplicateUpdatePlans));
            upsert.getGsiRelocateWriters().forEach(writer -> writer.getInput(executionContext, deduplicatedEc,
                (wr) -> rowBuilder, rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                gsiReplicateUpdatePlans));
        }

        // For gsi contains no column in ON DUPLICATE KEY UPDATE statement
        upsert.getGsiInsertWriters()
            .forEach(writer -> {
                List<RelNode> inputs = writer.getInput(deduplicatedEc.copy());
                gsiInsertPlans
                    .addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                        Collectors.toList()));
                gsiReplicateInsertPlans
                    .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                        Collectors.toList()));
            });
        // Execute INSERT --> UPDATE/DELETE

        final List<RelNode> allDelete = new ArrayList<>(primaryDeletePlans);
        allDelete.addAll(gsiDeletePlans);
        allDelete.addAll(replilcateDeletePlans);
        allDelete.addAll(gsiReplicateDeletePlans);

        final List<RelNode> allUpdate = new ArrayList<>(primaryUpdatePlans);
        allUpdate.addAll(gsiUpdatePlans);
        allUpdate.addAll(replilcateUpdatePlans);
        allUpdate.addAll(gsiReplicateUpdatePlans);

        // Default concurrent policy is group concurrent
        final List<RelNode> allInsert = new ArrayList<>(primaryInsertPlans);
        allInsert.addAll(gsiInsertPlans);
        allInsert.addAll(replilcateInsertPlans);
        allInsert.addAll(gsiReplicateInsertPlans);

        if (!allDelete.isEmpty()) {
            executePhysicalPlan(allDelete, executionContext, schemaName, isBroadcast);
        }

        if (!allUpdate.isEmpty()) {
            executePhysicalPlan(allUpdate, executionContext, schemaName, isBroadcast);
        }

        if (!allInsert.isEmpty()) {
            executePhysicalPlan(allInsert, executionContext, schemaName, isBroadcast);
        }

        affectRows = inputValues.stream().mapToInt(row -> row.affectedRows).sum();

        return affectRows;
    }

    @Override
    protected BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> getIdenticalPartitionKeyChecker(
        LogicalDynamicValues input,
        ExecutionContext executionContext) {
        return (w, p) -> {
            final List<Object> row = p.getKey();
            final RelocateWriter rw = w.unwrap(RelocateWriter.class);

            // Compare partition key in two value list
            final List<Object> skTargets = Mappings.permute(row, rw.getIdentifierKeyTargetMapping());
            final List<Object> skSources = Mappings.permute(row, rw.getIdentifierKeySourceMapping());
            final GroupKey skTargetKey = new GroupKey(skTargets.toArray(), rw.getIdentifierKeyMetas());
            final GroupKey skSourceKey = new GroupKey(skSources.toArray(), rw.getIdentifierKeyMetas());

            return skTargetKey.equals(skSourceKey);
        };
    }

    /**
     * Bind insert rows to operations(INSERT / UPDATE / INSERT-then-UPDATE)
     * Convert rows duplicated with previously inserted row to INSERT-then-UPDATE (first row do insert, last row do update, skip middle)
     * <p>
     * 1. Build DuplicateCheckRow foreach selected row
     * 2. Foreach insert row, check duplicate and update DuplicateCheckRow
     * 3. Generate result
     *
     * @param upsert Upsert
     * @param duplicateValues Selected rows
     * @param upsertEc ExecutionContext contains insert rows
     * @return Bind result foreach insert row
     */
    protected List<DuplicateCheckResult> bindInsertRows(LogicalUpsert upsert,
                                                        List<List<Object>> duplicateValues,
                                                        ExecutionContext upsertEc) {
        final List<Map<Integer, ParameterContext>> currentBatchParameters = upsertEc.getParams().getBatchParameters();

        final List<List<Integer>> beforeUkMapping = upsert.getBeforeUkMapping();
        final List<List<Integer>> afterUkMapping = upsert.getAfterUkMapping();
        final List<List<ColumnMeta>> ukColumnMetas = upsert.getUkColumnMetas();
        final LogicalDynamicValues input = RelUtils.getRelInput(upsert);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final List<String> insertColumns = input.getRowType().getFieldNames();
        final List<String> tableColumns = upsert.getTable().getRowType().getFieldNames();
        final List<Integer> beforeUpdateMapping = upsert.getBeforeUpdateMapping();
        final List<ColumnMeta> rowColumnMeta = upsert.getRowColumnMetaList();
        final int seqColumnIndex = upsert.getSeqColumnIndex();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(upsertEc.getSqlMode());
        final boolean modifyPartitionKey = upsert.isModifyPartitionKey();

        final boolean skipTrivialUpdate =
            upsertEc.getParamManager().getBoolean(ConnectionParams.DML_SKIP_TRIVIAL_UPDATE);

        final Map<String, RexNode> columnValueMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(insertColumns).forEach(o -> columnValueMap.put(o.getValue(), rexRow.get(o.getKey())));
        final List<RexNode> columnValueMapping =
            tableColumns.stream().map(columnValueMap::get).collect(Collectors.toList());

        // 1. Init before rows
        final AtomicInteger logicalRowIndex = new AtomicInteger(0);
        final List<DuplicateCheckRow> checkerRows = new ArrayList<>();
        final List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> checkerMap =
            buildDuplicateCheckers(duplicateValues, beforeUkMapping, ukColumnMetas, checkerRows, logicalRowIndex);

        // 2. Check each insert row
        currentBatchParameters.forEach(newRow -> {
            // Build group key
            final List<GroupKey> keys = buildGroupKeys(afterUkMapping, ukColumnMetas,
                (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), upsertEc, newRow));

            // Check Duplicate with rows in table and rows already inserted
            // For batch insert, new row could still duplicate with previously inserted row
            boolean duplicated = false;
            final AtomicInteger duplicatedRowIndex = new AtomicInteger(-1);
            for (Ord<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> ord : Ord.zip(checkerMap)) {
                final Integer ukIndex = ord.getKey();
                final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = ord.getValue();

                duplicated |= ExecUtils.duplicated(checker, keys.get(ukIndex), (k, v) -> {
                    // If there is more than one unique key, one upsert row might be duplicate with more than one existing row.
                    //
                    // Quote from MySQL doc :
                    // "If upsert matches several rows, only one row is updated. In general, you should try to avoid
                    // using an ON DUPLICATE KEY UPDATE clause on tables with multiple unique indexes."
                    //
                    // https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
                    //
                    // We should limit duplicated rows selected from table for each insert row to one

                    // Get check row with minimum row index
                    duplicatedRowIndex.set(v.firstKey());
                });

                // Check duplicate in existence order of index in create table statement
                if (duplicated) {
                    break;
                }
            }

            if (duplicated) {
                // Duplicated with existing row in table or previously inserted row

                final int rowIndex = duplicatedRowIndex.get();
                final DuplicateCheckRow beforeRow = checkerRows.get(rowIndex);
                final DuplicateCheckRow afterRow = new DuplicateCheckRow(beforeRow, rowIndex);

                // Update old row
                afterRow.doUpdate(upsert, newRow, columnValueMapping, upsertEc);

                // Update checkerMap
                for (int i = 0; i < beforeUkMapping.size(); i++) {
                    final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = checkerMap.get(i);
                    final GroupKey beforeKey = beforeRow.keyList.get(i);
                    final GroupKey afterKey = afterRow.keyList.get(i);

                    checker.compute(beforeKey, (k, v) -> {
                        v.remove(rowIndex);
                        return v.isEmpty() ? null : v;
                    });

                    checker.compute(afterKey, (k, v) -> {
                        final SortedMap<Integer, DuplicateCheckRow> value =
                            Optional.ofNullable(v).orElseGet(TreeMap::new);
                        value.put(afterRow.rowIndex, afterRow);
                        return value;
                    });
                }

                checkerRows.set(rowIndex, afterRow);
            } else {
                // Add new row

                // Build row value for future duplicate check
                final List<Object> after = RexUtils.buildRowValue(rexRow, null, newRow);

                final DuplicateCheckRow newDuplicateCheckRow = new DuplicateCheckRow();
                newDuplicateCheckRow.after = after;
                newDuplicateCheckRow.insertRow = true;
                newDuplicateCheckRow.insertParam = newRow;
                newDuplicateCheckRow.keyList = keys;
                newDuplicateCheckRow.affectedRows = 1;
                newDuplicateCheckRow.rowIndex = logicalRowIndex.getAndIncrement();

                // Update checkerMap
                for (int i = 0; i < beforeUkMapping.size(); i++) {
                    final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = checkerMap.get(i);
                    final GroupKey key = newDuplicateCheckRow.keyList.get(i);

                    checker.compute(key, (k, v) -> {
                        final SortedMap<Integer, DuplicateCheckRow> value =
                            Optional.ofNullable(v).orElseGet(TreeMap::new);
                        value.put(newDuplicateCheckRow.rowIndex, newDuplicateCheckRow);
                        return value;
                    });
                }

                checkerRows.add(newDuplicateCheckRow);
            }
        });

        // 3. Build result
        final List<DuplicateCheckResult> result = new ArrayList<>();
        checkerRows.stream().filter(Objects::nonNull).filter(DuplicateCheckRow::doInsertOrUpdate)
            .forEach(row -> {
                final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                rowResult.insertParam = row.insertParam;
                rowResult.duplicated = row.duplicated;
                rowResult.doInsert = row.insertRow;
                rowResult.affectedRows += row.affectedRows;
                if (rowResult.duplicated) {
                    // Update or Insert-then-Update
                    rowResult.before = row.before;
                    rowResult.after = row.after;
                    rowResult.updateSource = new ArrayList<>(row.before);
                    rowResult.updateSource
                        .addAll(Mappings.permute(row.after, Mappings.source(beforeUpdateMapping, tableColumns.size())));
                    rowResult.onDuplicateKeyUpdateParam = row.duplicateUpdateParam;

                    if (seqColumnIndex >= 0 && (rowResult.doInsert || modifyPartitionKey)) {
                        final Object autoIncValue = rowResult.after.get(seqColumnIndex);

                        if (null == autoIncValue || (autoValueOnZero && 0L == RexUtils.valueOfObject1(autoIncValue))) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO,
                                "Do not support update primary key to null or zero by INSERT ON DUPLICATE KEY UPDATE");
                        }
                    }

                    if (skipTrivialUpdate && !rowResult.doInsert) {
                        rowResult.trivial = identicalRow(row.before, row.after, rowColumnMeta);
                    }

                    if (rowResult.skipUpdate() && !upsertEc.isClientFoundRows()) {
                        rowResult.affectedRows = 0;
                    }
                }
                result.add(rowResult);
            });

        return result;
    }

    /**
     * Eval source part of DUPLICATE KEY UPDATE list
     * <p>
     * 1. Replace function VALUES() with RexNode in VALUES part of INSERT
     * 2. Eval
     * 3. Build parameter context map
     *
     * @param rexRow Source part of DUPLICATE KEY UPDATE list
     * @param row Selected row
     * @param param Insert parameter row
     * @param columnValueMapping RexNode foreach column in target table
     * @param updateValues Value result foreach target column in DUPLICATE KEY UPDATE list
     * @return Parameter context map for DUPLICATE KEY UPDATE list
     */
    private static Map<Integer, ParameterContext> buildParamForDuplicateKeyUpdate(List<RexNode> rexRow, Row row,
                                                                                  Map<Integer, ParameterContext> param,
                                                                                  List<RexNode> columnValueMapping,
                                                                                  ExecutionContext upsertEc,
                                                                                  final List<Object> updateValues) {
        final ExecutionContext tmpEc = upsertEc.copy();
        tmpEc.setParams(new Parameters(param));

        final Map<Integer, ParameterContext> result = new HashMap<>(param.size());
        for (RexNode rexNode : rexRow) {
            int paramIndex;
            RexNode valueCall;
            // Source part of DUPLICATE KEY UPDATE list should already be wrapped with RexCallParam or RexDynamicParam
            if (rexNode instanceof RexCallParam) {
                final RexCallParam callParam = (RexCallParam) rexNode;
                paramIndex = callParam.getIndex();
                // Replace function VALUES() with RexNode in VALUES part of INSERT
                valueCall = (null == columnValueMapping ? callParam.getRexCall() :
                    callParam.getRexCall().accept(new ReplaceValuesCall(columnValueMapping)));
            } else {
                final RexDynamicParam dynamicParam = (RexDynamicParam) rexNode;
                paramIndex = dynamicParam.getIndex();
                valueCall = dynamicParam;
            }

            // Eval rex
            final Object value = RexUtils.getValueFromRexNode(valueCall, row, tmpEc);

            // Build parameter
            final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                paramIndex + 1, value});
            result.put(paramIndex + 1, newPc);

            if (null != updateValues) {
                updateValues.add(value);
            }
        }

        return result;
    }

    private static List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> buildDuplicateCheckers(
        List<List<Object>> selectedRows,
        List<List<Integer>> ukMapping,
        List<List<ColumnMeta>> ukColumnMetas,
        List<DuplicateCheckRow> outCheckRows,
        AtomicInteger logicalRowIndex) {
        selectedRows.forEach(row -> {
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildGroupKeys(ukMapping, ukColumnMetas, row::get);
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            outCheckRows.add(duplicateCheckRow);
        });

        // Order by first uk
        Collections.sort(outCheckRows);

        // Set logical row index and build checker map
        final List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> ukCheckerMapList =
            new ArrayList<>(ukMapping.size());
        IntStream.range(0, ukMapping.size()).forEach(i -> ukCheckerMapList.add(new TreeMap<>()));

        outCheckRows.forEach(row -> {
            row.rowIndex = logicalRowIndex.getAndIncrement();

            for (int i = 0; i < ukMapping.size(); i++) {
                ukCheckerMapList.get(i).compute(row.keyList.get(i), (k, v) -> {
                    final SortedMap<Integer, DuplicateCheckRow> checker =
                        Optional.ofNullable(v).orElseGet(TreeMap::new);
                    checker.put(row.rowIndex, row);
                    return checker;
                });
            }
        });

        return ukCheckerMapList;
    }

    private static class DuplicateCheckRow implements Comparable<DuplicateCheckRow> {
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
        /**
         * Parameters for DUPLICATE KEY UPDATE list
         */
        public Map<Integer, ParameterContext> duplicateUpdateParam;

        public boolean insertRow = false;
        public boolean duplicated = false;
        public int affectedRows = 0;
        /**
         * Index in insert rows
         */
        public int rowIndex;
        /**
         * Group keys of all unique key
         */
        public List<GroupKey> keyList;

        /**
         * Use first uk as sort key, by default
         */
        public int sortKeyIndex = 0;

        public DuplicateCheckRow() {
        }

        public DuplicateCheckRow(DuplicateCheckRow beforeRow, int thisRowIndex) {
            this.before = beforeRow.before;
            this.after = beforeRow.after;
            this.insertParam = beforeRow.insertParam;

            this.insertRow = beforeRow.insertRow;
            this.duplicated = beforeRow.duplicated;
            this.affectedRows = beforeRow.affectedRows;
            this.rowIndex = thisRowIndex;

            this.keyList = beforeRow.keyList;
            this.sortKeyIndex = beforeRow.sortKeyIndex;
        }

        /**
         * Update duplicate check row
         * For rows duplicated with previously inserted row, convert to INSERT-then-UPDATE (first row do insert, last row do update, skip middle)
         *
         * @param upsert Upsert statement
         * @param newRow Parameter row of insert
         * @param columnValueMapping For replacing VALUES() call with rexNode from VALUES part of INSERT
         */
        public void doUpdate(LogicalUpsert upsert, Map<Integer, ParameterContext> newRow,
                             List<RexNode> columnValueMapping, ExecutionContext upsertEc) {
            final List<List<Integer>> beforeUkMapping = upsert.getBeforeUkMapping();
            final List<List<ColumnMeta>> ukColumnMetas = upsert.getUkColumnMetas();
            final List<RexNode> duplicateKeyUpdateValues = upsert.getDuplicateKeyUpdateValueList();
            final List<Integer> beforeUpdateMapping = upsert.getBeforeUpdateMapping();
            final Set<Integer> appendedColumnIndex = upsert.getAppendedColumnIndex();
            final List<ColumnMeta> rowColumnMeta = upsert.getRowColumnMetaList();
            final CursorMeta meta = CursorMeta.build(upsert.getTableColumnMetaList());

            // Get update values
            final List<Object> updateValues = new ArrayList<>();
            final ArrayRow currentRow = new ArrayRow(meta, this.after.toArray());
            this.duplicateUpdateParam =
                buildParamForDuplicateKeyUpdate(duplicateKeyUpdateValues, currentRow, newRow, columnValueMapping,
                    upsertEc, updateValues);

            // Do update
            final List<Object> withoutAppended = new ArrayList<>(this.after);
            final List<Object> withAppended = new ArrayList<>(this.after);
            Ord.zip(updateValues).forEach(o -> {
                final Integer updateValueIndex = o.getKey();
                final Integer columnIndex = beforeUpdateMapping.get(updateValueIndex);

                withAppended.set(columnIndex, o.getValue());
                if (!appendedColumnIndex.contains(columnIndex)) {
                    // Skip column with 'on update current_timestamp', for identical row check
                    withoutAppended.set(columnIndex, o.getValue());
                }
            });

            // Check identical row
            final boolean isIdenticalRow = identicalRow(withoutAppended, this.after, rowColumnMeta);
            final List<Object> updated = isIdenticalRow ? withoutAppended : withAppended;
            this.affectedRows += isIdenticalRow ? 1 : 2;

            if (upsert.isModifyUniqueKey()) {
                // Update group key
                this.keyList = buildGroupKeys(beforeUkMapping, ukColumnMetas, updated::get);
            }

            // Add before value for INSERT-then-UPDATE
            if (this.before == null && this.insertRow) {
                this.before = this.after;
            }

            this.duplicated = true;
            this.after = updated;
        }

        public boolean isUpdateRow() {
            return duplicated && null != before && null != duplicateUpdateParam;
        }

        public boolean isInsertRow() {
            return insertRow;
        }

        public boolean doInsertOrUpdate() {
            return isUpdateRow() || isInsertRow();
        }

        @Override
        public int compareTo(DuplicateCheckRow that) {
            return this.keyList.get(this.sortKeyIndex).compareTo(that.keyList.get(that.sortKeyIndex));
        }
    }
}
