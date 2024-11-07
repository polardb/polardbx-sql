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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.LastInsertId;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategyResult;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.WriterFactory;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.MappingBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertWriter;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.core.TddlOperatorTable.NEXTVAL;
import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.buildUpdateColumnList;

/**
 * @author chenmo.cm
 */
public class OptimizeLogicalInsertRule extends RelOptRule {

    public static final OptimizeLogicalInsertRule INSTANCE = new OptimizeLogicalInsertRule(
        operand(LogicalInsert.class, any()));

    public OptimizeLogicalInsertRule(RelOptRuleOperand operand) {
        super(operand, OptimizeLogicalInsertRule.class.getName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalInsert insert = call.rel(0);
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        final boolean gsiConcurrentWrite =
            plannerContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        final boolean withHint = insert.hasHint() || Optional.ofNullable(plannerContext.getExecutionContext())
            .map(ExecutionContext::isOriginSqlPushdownOrRoute).orElse(false);
        final boolean optimized = null != insert.getPrimaryInsertWriter() || insert instanceof LogicalInsertIgnore;
        return !optimized && !withHint && gsiConcurrentWrite;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ExecutionContext ec = PlannerContext.getPlannerContext(call).getExecutionContext();

        final LogicalInsert origin = call.rel(0);
        final PlannerContext context = call.getPlanner().getContext().unwrap(PlannerContext.class);

        final ExecutionStrategyResult executionStrategyRs =
            ExecutionStrategy.determineExecutionStrategy(origin, context);
        ExecutionStrategy executionStrategy = executionStrategyRs.execStrategy;

        origin.setPushablePrimaryKeyCheck(executionStrategyRs.pushablePrimaryKeyCheck);
        origin.setPushableForeignConstraintCheck(executionStrategyRs.pushableForeignConstraintCheck);

        RelNode updated = origin;
        switch (executionStrategy) {
        case PUSHDOWN:
            updated = handlePushdown(origin, false, ec);
            break;
        case DETERMINISTIC_PUSHDOWN:
            updated = handlePushdown(origin, true, ec);
            break;
        case LOGICAL:
            if (origin.isReplace()) {
                origin.setUkContainsAllSkAndGsiContainsAllUk(executionStrategyRs.ukContainsAllSkAndGsiContainsAllUk);
                updated = handleReplace(origin, context, executionStrategyRs, ec);
            } else if (origin.isUpsert()) {
                updated = handleUpsert(origin, context, ec);
            } else if (origin.isInsertIgnore()) {
                updated = handleInsertIgnore(origin, context, executionStrategyRs, ec);
            } else {
                updated = handleInsert(origin, context, executionStrategyRs, ec);
            }
            break;
        default:
            throw new IllegalStateException("Unexpected value: " + executionStrategy);
        }

        if (updated != origin) {
            call.transformTo(updated);
        }
    }

    /**
     * Build LogicalInsert with writer for INSERT/INSERT IGNORE/REPLACE/UPSERT with PUSHDOWN execute strategy
     * Replace call parameter and sequence
     *
     * @param origin Origin LogicalInsert
     * @return LogicalInsert
     */
    private LogicalInsert handlePushdown(LogicalInsert origin, boolean deterministicPushdown, ExecutionContext ec) {
        final String schema = origin.getSchemaName();
        final String logicalTableName = origin.getLogicalTableName();
        final RelOptTable primaryTable = origin.getTable();

        final RelDataType sourceRowType = origin.getInsertRowType();
        final List<Integer> valuePermute = Mappings.identityMapping(sourceRowType.getFieldCount());

        // Replace call parameter and sequence
        final LogicalInsert newInsert = replaceExpAndSeqWithParam(origin, false, deterministicPushdown, ec);

        final TableMeta primaryTableMeta = ec.getSchemaManager(schema).getTable(logicalTableName);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<String> primaryColumnNames = sourceRowType.getFieldNames();
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);

        // For table with gsi, only simple insert can be pushdown
        if (!gsiMetas.isEmpty() && !deterministicPushdown && !origin.isSimpleInsert()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support PUSHDOWN strategy for INSERT IGNORE, REPLACE and UPSERT on table with gsi");
        }

        if (!gsiMetas.isEmpty() && newInsert.withDuplicateKeyUpdate()) {
            final List<String> updateColumnList = buildUpdateColumnList(newInsert);

            final Set<String> gsiCol = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            gsiMetas.stream().flatMap(tm -> tm.getAllColumns().stream()).map(ColumnMeta::getName).forEach(gsiCol::add);
            final boolean updateGsi = updateColumnList.stream().anyMatch(gsiCol::contains);

            if (updateGsi) {
                final String strategy = deterministicPushdown ? "DETERMINISTIC_PUSHDOWN" : "PUSHDOWN";
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support " + strategy + " strategy for UPSERT which update gsi");
            }
        }

        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;
        final boolean isBroadcast = oc.getRuleManager().isBroadCast(logicalTableName);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(logicalTableName);
        final boolean isReplace = origin.isReplace();
        final boolean isValueSource = !origin.isSourceSelect();

        final List<RexNode> newDuplicatedUpdateList = newInsert.isSourceSelect() ?
            LogicalInsert.buildDuplicateKeyUpdateList(newInsert, valuePermute.size(), null) :
            newInsert.getDuplicateKeyUpdateList();

        // Build writers
        final InsertWriter writer = WriterFactory
            .createInsertOrReplaceWriter(newInsert, primaryTable, sourceRowType, valuePermute, primaryTableMeta,
                newInsert.getKeywords(), newDuplicatedUpdateList, isReplace, isBroadcast, isSingleTable, isValueSource,
                ec);

        // Push insert ignore for upsert on table with gsi and not update any column in gsi
        final List<String> gsiKeywords = new ArrayList<>(newInsert.getKeywords());
        if (gsiMetas.size() > 0 && GeneralUtil.isNotEmpty(newDuplicatedUpdateList)) {
            if (!newInsert.withIgnore()) {
                gsiKeywords.add("IGNORE");
            }
        }

        // Writer for gsi
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable = catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
            final List<Integer> gsiValuePermute = gsiColumnMappings.get(i);
            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiInsertWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, sourceRowType, gsiValuePermute, gsiMeta, gsiKeywords,
                    null, isReplace, isGsiBroadcast, isGsiSingle, isValueSource, ec));
        });

        // Do not need share lock for INSERT SELECT
        newInsert.setPrimaryInsertWriter(writer);
        newInsert.setGsiInsertWriters(gsiInsertWriters);
        newInsert.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(newInsert, ec, primaryTableMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryTableMeta, newInsert, ec);

        //insert select判断
        if (newInsert.isSourceSelect()) {
            return handleInsertSelect(newInsert, ec);
        }
        return newInsert;
    }

    private LogicalInsert handleInsert(LogicalInsert insert, PlannerContext context,
                                       ExecutionStrategyResult executionStrategyRs, ExecutionContext ec) {
        return handlePushdown(insert, true, ec);
    }

    private LogicalInsert handleInsertIgnore(LogicalInsert insert, PlannerContext context,
                                             ExecutionStrategyResult strategyResult, ExecutionContext ec) {

        final String schema = insert.getSchemaName();
        final String targetTable = insert.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;

        final boolean isBroadcast = oc.getRuleManager().isBroadCast(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);
        final RelOptTable primaryTable = insert.getTable();
        final List<String> primaryColumnNames = insert.getInsertRowType().getFieldNames();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final boolean withGsi = GeneralUtil.isNotEmpty(gsiMetas);

        final RelDataType originSourceRowType = insert.getInsertRowType();
        final List<Integer> originSourceValuePermute = Mappings.identityMapping(originSourceRowType.getFieldCount());
        final boolean isOriginValueSource = !insert.isSourceSelect();

        // Build writers
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);

        boolean useStrategyByHintParams = strategyResult.useStrategyByHintParams;
        boolean canPushDuplicateIgnoreScaleOutCheck = strategyResult.canPushDuplicateIgnoreScaleOutCheck;
        boolean pushDuplicateCheckByHintParams = strategyResult.pushDuplicateCheckByHintParams;

        // Replace call parameter and sequence
        final LogicalInsert newInsert = replaceExpAndSeqWithParam(insert, true, false, ec);

        // Writer for primary table
        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryColumnNames.size()).boxed().collect(Collectors.toList());
        final InsertWriter primaryInsertWriter =

            WriterFactory
                .createInsertOrReplaceWriter(newInsert, primaryTable, newInsert.getInsertRowType(), primaryValuePermute,
                    primaryMeta,
                    null, null, false, isBroadcast, isSingleTable, !newInsert.isSourceSelect(), ec);
        // Delete writer for inserted duplicated row
        DistinctWriter primaryDeleteWriter = null;
        if (isBroadcast) {
            primaryDeleteWriter = WriterFactory.createBroadcastDeleteWriter(newInsert, primaryTable, 0, ec);
        } else if (isSingleTable) {
            primaryDeleteWriter = WriterFactory.createSingleDeleteWriter(newInsert, primaryTable, 0, ec);
        } else {
            primaryDeleteWriter = WriterFactory.createDeleteWriter(newInsert, primaryTable, 0, ec);
        }

        // Writer for gsi
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertIgnoreWriters = new ArrayList<>();
        final List<DistinctWriter> gsiDeleteWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable =
                catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));

            final List<Integer> valuePermute = gsiColumnMappings.get(i);
            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiInsertWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, newInsert.getInsertRowType(), valuePermute, gsiMeta,
                    null,
                    null, false, isGsiBroadcast, isGsiSingle, !newInsert.isSourceSelect(), ec));

            gsiInsertIgnoreWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, newInsert.getInsertRowType(), valuePermute, gsiMeta,
                    ImmutableList.of("IGNORE"),
                    null, false, isGsiBroadcast, isGsiSingle, !newInsert.isSourceSelect(), ec));

            if (isGsiBroadcast) {
                gsiDeleteWriters.add(WriterFactory.createBroadcastDeleteWriter(newInsert, gsiTable, 0, ec));
            } else if (isGsiSingle) {
                gsiDeleteWriters.add(WriterFactory.createSingleDeleteWriter(newInsert, gsiTable, 0, ec));
            } else {
                gsiDeleteWriters.add(WriterFactory.createDeleteWriter(newInsert, gsiTable, 0, ec));
            }
        });

        // Writer for Scaleout pushdown when NOT hit ScaleOut Group Build writers
        InsertWriter scaleoutPushdownWriter = null;
        if (!withGsi && !isBroadcast && !isSingleTable && scaleOutCanWrite) {
            // Only do this optimization for non-gsi table not-broadcast table
            // and its status is on scale-out writable
            if (!useStrategyByHintParams && (pushDuplicateCheckByHintParams || canPushDuplicateIgnoreScaleOutCheck)) {
                scaleoutPushdownWriter = WriterFactory
                    .createInsertOrReplaceWriter(newInsert, primaryTable, originSourceRowType, originSourceValuePermute,
                        primaryMeta,
                        newInsert.getKeywords(), null, false, isBroadcast, isSingleTable, isOriginValueSource, ec);
            }
        }

        // Do not need share lock for INSERT SELECT
        newInsert.setPrimaryInsertWriter(primaryInsertWriter);
        newInsert.setPrimaryDeleteWriter(primaryDeleteWriter);
        newInsert.setGsiInsertWriters(gsiInsertWriters);
        newInsert.setGsiInsertIgnoreWriters(gsiInsertIgnoreWriters);
        newInsert.setGsiDeleteWriters(gsiDeleteWriters);
        newInsert.setPushDownInsertWriter(scaleoutPushdownWriter);
        newInsert.initAutoIncrementColumn();

        final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukSet.addAll(GlobalIndexMeta.getUniqueKeyColumnList(targetTable, schema, true, ec));
        final List<String> selectListForDuplicateCheck =
            primaryTable.getRowType().getFieldNames().stream().filter(ukSet::contains).collect(Collectors.toList());
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final LogicalInsertIgnore insertIgnore = new LogicalInsertIgnore(newInsert, selectListForDuplicateCheck);
        insertIgnore.setTargetTableIsWritable(scaleOutCanWrite);
        insertIgnore.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        insertIgnore.setSourceTablesIsReadyToPublish(false);
        insertIgnore.setPushDownInsertWriter(scaleoutPushdownWriter);
        insertIgnore.getUkGroupByTable().putAll(groupUkByTable(insertIgnore, ec));
        insertIgnore.getLocalIndexPhyName().putAll(getLocalIndexName(insertIgnore.getUkGroupByTable(), schema, ec));
        insertIgnore.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, insertIgnore.getUkGroupByTable()));
        insertIgnore.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(insertIgnore.getColumnMetaMap()));
        insertIgnore.setUkContainGeneratedColumn(
            ukContainGeneratedColumn(primaryMeta, insertIgnore.getColumnMetaMap().keySet()));
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(insertIgnore, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, insertIgnore, ec);

        return insertIgnore;
    }

    private LogicalInsert handleReplace(LogicalInsert replace, PlannerContext context,
                                        ExecutionStrategyResult strategyResult, ExecutionContext ec) {

        final String schema = replace.getSchemaName();
        final String targetTable = replace.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;

        final boolean isBroadcast = oc.getRuleManager().isBroadCast(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);
        final RelOptTable primaryTable = replace.getTable();
        final List<String> primaryColumnNames = replace.getInsertRowType().getFieldNames();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final boolean withGsi = GeneralUtil.isNotEmpty(gsiMetas);
        final boolean hasJsonColumn = primaryMeta.getAllColumns().stream()
            .anyMatch(cm -> DataTypeUtil.equalsSemantically(cm.getDataType(), DataTypes.JsonType));

        final RelDataType originSourceRowType = replace.getInsertRowType();
        final List<Integer> originSourceValuePermute = Mappings.identityMapping(originSourceRowType.getFieldCount());
        final boolean isOriginValueSource = !replace.isSourceSelect();

        // Build writers
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);

        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryColumnNames.size()).boxed().collect(Collectors.toList());
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);

        boolean useStrategyByHintParams = strategyResult.useStrategyByHintParams;
        boolean canPushDuplicateIgnoreScaleOutCheck = strategyResult.canPushDuplicateIgnoreScaleOutCheck;
        boolean pushDuplicateCheckByHintParams = strategyResult.pushDuplicateCheckByHintParams;

        final LogicalInsert newInsert = replaceExpAndSeqWithParam(replace, true, false, ec);

        newInsert.setUkContainsAllSkAndGsiContainsAllUk(replace.isUkContainsAllSkAndGsiContainsAllUk());
        // Writer for primary table
        InsertWriter primaryInsertWriter = null;
        ReplaceRelocateWriter primaryReplaceRelocateWriter = null;

        final LogicalInsertIgnore base = new LogicalInsertIgnore(newInsert, primaryColumnNames);
        final boolean containsAllUk = base.containsAllUk(targetTable);

        // SELECT --> deduplicate --> REPLACE or DELETE + INSERT
        primaryReplaceRelocateWriter =
            WriterFactory.createReplaceRelocateWriter(newInsert, primaryTable, primaryValuePermute, primaryMeta,
                containsAllUk, false, isBroadcast, isSingleTable, ec);

        // Writer for gsi
        final List<ReplaceRelocateWriter> gsiReplaceRelocateWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable =
                catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));

            final LogicalInsertIgnore tmpBase = new LogicalInsertIgnore(newInsert, primaryColumnNames);
            final boolean gsiContainsAllUk = tmpBase.containsAllUk(gsiMeta.getTableName());

            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiReplaceRelocateWriters.add(
                WriterFactory.createReplaceRelocateWriter(newInsert, gsiTable, gsiColumnMappings.get(i), gsiMeta,
                    gsiContainsAllUk, true, isGsiBroadcast, isGsiSingle, ec));
        });

        // Writer for Scaleout pushdown when NOT hit ScaleOut Group Build writers
        InsertWriter scaleoutPushdownWriter = null;
        if (!withGsi && !isBroadcast && !isSingleTable && scaleOutCanWrite) {
            // Only do this optimization for non-gsi table not-broadcast table
            // and its status is on scale-out writable
            if (!useStrategyByHintParams && (pushDuplicateCheckByHintParams || canPushDuplicateIgnoreScaleOutCheck)) {
                // 1. Make sure that connProps has no HINT about DML_EXECUTION_STRATEGY
                // 2. Only optimise for the situation that DML_PUSH_DUPLICATE_CHECK=true or canPushDuplicateCheck=true
                scaleoutPushdownWriter = WriterFactory
                    .createInsertOrReplaceWriter(newInsert, primaryTable, originSourceRowType, originSourceValuePermute,
                        primaryMeta,
                        newInsert.getKeywords(), null, true, isBroadcast, isSingleTable, isOriginValueSource, ec);
            }
        }
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final LogicalReplace newReplace =
            new LogicalReplace(newInsert, primaryInsertWriter, primaryReplaceRelocateWriter, gsiInsertWriters,
                gsiReplaceRelocateWriters, null, primaryColumnNames, hasJsonColumn);
        newReplace.setTargetTableIsWritable(scaleOutCanWrite);
        newReplace.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        newReplace.setSourceTablesIsReadyToPublish(false);
        newReplace.setPushDownInsertWriter(scaleoutPushdownWriter);
        newReplace.getUkGroupByTable().putAll(groupUkByTable(newReplace, ec));
        newReplace.getLocalIndexPhyName().putAll(getLocalIndexName(newReplace.getUkGroupByTable(), schema, ec));
        newReplace.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, newReplace.getUkGroupByTable()));
        newReplace.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(newReplace.getColumnMetaMap()));
        newReplace.setUkContainGeneratedColumn(
            ukContainGeneratedColumn(primaryMeta, newReplace.getColumnMetaMap().keySet()));

        // TODO need exclusive lock for REPLACE SELECT ?
        newReplace.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(newReplace, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, newReplace, ec);

        return newReplace;

    }

    private RelNode handleUpsert(LogicalInsert upsert, PlannerContext context, ExecutionContext ec) {

        final String schema = upsert.getSchemaName();
        final String targetTable = upsert.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;
        final TddlRuleManager rule = oc.getRuleManager();

        final boolean isBroadcast = oc.getRuleManager().isBroadCast(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);

        final List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(targetTable, schema, true, tm -> true, ec);
        final boolean withoutPkAndUk = uniqueKeys.isEmpty() || uniqueKeys.get(0).isEmpty();

        if (withoutPkAndUk) {
            // Without pk and uk, upsert can be pushdown.
            // Assuming than table with scale out running or gsi must have primary key
            return handlePushdown(upsert, isBroadcast, ec);
        }

        if (upsert.withIgnore()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Do not support insert ignore...on duplicate key update");
        }

        final RelOptTable primaryTable = upsert.getTable();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final List<String> primaryTargetColumns = upsert.getInsertRowType().getFieldNames();
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);
        final boolean hasJsonColumn = primaryMeta.getAllColumns().stream()
            .anyMatch(cm -> DataTypeUtil.equalsSemantically(cm.getDataType(), DataTypes.JsonType));

        // Mapping from VALUES of target INSERT to VALUES of source INSERT
        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryTargetColumns.size()).boxed().collect(Collectors.toList());
        final List<List<Integer>> gsiValuePermutes = initGsiColumnMapping(primaryTargetColumns, gsiMetas);

        // Get columns to be update
        final List<String> updateColumnList = BuildPlanUtils.buildUpdateColumnList(upsert);

        if (ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK)
            && !upsert.isPushablePrimaryKeyCheck()) {
            // TODO(qihua): support upsert Pk check
            // Upsert updates primary key, and we are not able to check since primary key does not contain all sharding
            // key
            Set<String> updateColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            updateColumnSet.addAll(updateColumnList);
            if (primaryMeta.getPrimaryKey().stream().anyMatch(cm -> updateColumnSet.contains(cm.getName()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Can not check primary key for upsert");
            }
        }

        // Build column mapping for columns to be update
        final Map<Integer, List<Integer>> primaryUpdateColumnMappings = new HashMap<>();
        final Map<Integer, List<List<Integer>>> gsiUpdateColumnMappings = new HashMap<>();
        BuildPlanUtils
            .buildColumnMappings(updateColumnList, updateColumnList.stream().map(c -> 0).collect(Collectors.toList()),
                ImmutableMap.of(0, gsiMetas), primaryUpdateColumnMappings, gsiUpdateColumnMappings);

        final boolean allGsiPublished = GlobalIndexMeta.isAllGsiPublished(gsiMetas, context);
        AtomicBoolean modifyPartitionKey = new AtomicBoolean();
        final boolean modifyUniqueKey = isModifyUniqueKey(updateColumnList, targetTable, schema, ec);

        final LogicalInsert newInsert = replaceExpAndSeqWithParam(upsert, true, false, ec);

        // Build writer for primary table
        InsertWriter primaryInsertWriter = null;
        UpsertWriter primaryUpsertWriter = null;
        UpsertRelocateWriter primaryRelocateWriter = null;
        final List<String> selectListForDuplicateCheck = new ArrayList<>();
        final List<Integer> beforeUpdateMapping = new ArrayList<>();
        final AtomicBoolean withColumnRefInDuplicateKeyUpdate = new AtomicBoolean(false);

        final MappingBuilder mappingBuilder = MappingBuilder.create(primaryTargetColumns);
        // Get all columns of target table
        selectListForDuplicateCheck.addAll(mappingBuilder.getTarget());
        // Build update column mapping
        beforeUpdateMapping.addAll(mappingBuilder.source(updateColumnList).getMapping());
        // Check ON DUPLICATE KEY UPDATE part reference before value of duplicate row
        withColumnRefInDuplicateKeyUpdate.set(
            newInsert.getDuplicateKeyUpdateList().stream().map(rex -> ((RexCall) rex).getOperands().get(1))
                .anyMatch(call -> BeforeInputFinder.analyze(call).withInputRef));

        // Check modify partition key of primary
        final Set<String> partitionKeySet = rule.getSharedColumns(targetTable).stream()
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        boolean primaryDoRelocate = updateColumnList.stream().anyMatch(partitionKeySet::contains);
        boolean inputInValueColumnOrder = !checkInputInTableOrder(upsert, primaryMeta);

        if (!primaryDoRelocate && (scaleOutCanWrite || !allGsiPublished)) {
            final Set<String> pkName = GlobalIndexMeta.getPrimaryKeys(primaryMeta).stream().map(String::toLowerCase)
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            primaryDoRelocate |= updateColumnList.stream().map(String::toLowerCase).anyMatch(pkName::contains);
        }
        if (!primaryDoRelocate) {
            // SELECT --> deduplicate --> INSERT or UPDATE
            primaryUpsertWriter = WriterFactory.createUpsertWriter(newInsert, primaryTable, updateColumnList,
                primaryUpdateColumnMappings.get(0), primaryValuePermute, selectListForDuplicateCheck,
                false, primaryMeta, false, isBroadcast, isSingleTable, ec);
        } else {
            // SELECT --> deduplicate --> INSERT(values) or UPDATE or DELETE + INSERT(selected)
            primaryRelocateWriter = WriterFactory.createUpsertRelocateWriter(newInsert, primaryTable, updateColumnList,
                primaryUpdateColumnMappings.get(0), primaryValuePermute, selectListForDuplicateCheck, updateColumnList,
                primaryMeta, false, isBroadcast, isSingleTable, ec);
        }
        modifyPartitionKey.set(primaryDoRelocate);

        AtomicBoolean primaryModifySk = new AtomicBoolean();
        primaryModifySk.set(primaryDoRelocate);

        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);

        // Writer for gsi
        final List<UpsertWriter> gsiUpsertWriters = new ArrayList<>();
        final List<RelocateWriter> gsiRelocateWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        Ord.zip(gsiMetas).forEach(o -> {
            final Integer gsiIndex = o.getKey();
            final TableMeta gsiMeta = o.getValue();

            // Get update columns for gsi
            final List<Integer> gsiUpdateColumnMapping = gsiUpdateColumnMappings.get(0).get(gsiIndex);
            final boolean withoutUpdate = gsiUpdateColumnMapping.isEmpty();
            final List<String> gsiUpdateColumns = withoutUpdate ? new ArrayList<>() :
                Mappings.permute(updateColumnList, Mappings.source(gsiUpdateColumnMapping, updateColumnList.size()));

            // Check modify partition key of gsi
            final Set<String> gsiPartitionKeySet = rule.getSharedColumns(gsiMeta.getTableName()).stream()
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            boolean doRelocate = gsiUpdateColumns.stream().anyMatch(gsiPartitionKeySet::contains);

            final boolean isPublished = GlobalIndexMeta.isPublished(context.getExecutionContext(), gsiMeta);
            final boolean isGsiTableCanScaleOutWrite = ComplexTaskPlanUtils.canWrite(gsiMeta);
            if (!doRelocate && (scaleOutCanWrite || !isPublished || isGsiTableCanScaleOutWrite)) {
                final Set<String> pkName = GlobalIndexMeta.getPrimaryKeys(gsiMeta).stream()
                    .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
                doRelocate |= gsiUpdateColumns.stream().anyMatch(pkName::contains);
            }

            if (GlobalIndexMeta.canDelete(ec, gsiMeta) && !GlobalIndexMeta.canWrite(ec, gsiMeta)) {
                // DELETE_ONLY, need delete writer
                doRelocate = true;
            }

            boolean forceRelocate = primaryModifySk.get() && GlobalIndexMeta.isBackFillStatus(ec, gsiMeta);

            modifyPartitionKey.set(modifyPartitionKey.get() || doRelocate);

            final RelOptTable gsiTable = catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
            final List<Integer> gsiValuePermute = gsiValuePermutes.get(gsiIndex);

            boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);

            if (!doRelocate && !forceRelocate) {
                // SELECT --> deduplicate --> INSERT or UPDATE
                gsiUpsertWriters.add(WriterFactory
                    .createUpsertWriter(newInsert, gsiTable, gsiUpdateColumns, gsiUpdateColumnMapping, gsiValuePermute,
                        selectListForDuplicateCheck, withoutUpdate, gsiMeta, true, isGsiBroadcast, isGsiSingle, ec));
            } else {
                // SELECT --> deduplicate --> INSERT(values) or UPDATE or DELETE + INSERT(selected)
                gsiRelocateWriters.add(WriterFactory.createUpsertRelocateWriter(newInsert, gsiTable, gsiUpdateColumns,
                    gsiUpdateColumnMapping, gsiValuePermute, selectListForDuplicateCheck, updateColumnList, gsiMeta,
                    true, isGsiBroadcast, isGsiSingle, ec));
            }
        });
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final LogicalUpsert result =
            new LogicalUpsert(newInsert, primaryInsertWriter, primaryUpsertWriter, primaryRelocateWriter,
                gsiInsertWriters, gsiUpsertWriters, gsiRelocateWriters, selectListForDuplicateCheck,
                beforeUpdateMapping, selectListForDuplicateCheck.size(), modifyPartitionKey.get(), modifyUniqueKey,
                withColumnRefInDuplicateKeyUpdate.get(), hasJsonColumn, inputInValueColumnOrder);
        result.setSourceTablesIsReadyToPublish(false);
        result.setTargetTableIsWritable(scaleOutCanWrite);
        result.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        result.getUkGroupByTable().putAll(groupUkByTable(result, ec));
        result.getLocalIndexPhyName().putAll(getLocalIndexName(result.getUkGroupByTable(), schema, ec));
        result.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, result.getUkGroupByTable()));
        result.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(result.getColumnMetaMap()));
        result.setUkContainGeneratedColumn(ukContainGeneratedColumn(primaryMeta, result.getColumnMetaMap().keySet()));
        // TODO need exclusive lock for UPSERT SELECT ?
        result.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(result, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, result, ec);

        return result;
    }

    private LogicalInsert handleInsertSelect(LogicalInsert insert, ExecutionContext ec) {
        //不是LogicalInsert类（有些继承LogicalInsert，执行方式不同),或者不是insert select 直接返回
        if (!insert.getClass().isAssignableFrom(LogicalInsert.class) || !insert.isSourceSelect()) {
            return insert;
        }
        final boolean hasIndex = GlobalIndexMeta.hasGsi(
            insert.getLogicalTableName(), insert.getSchemaName(), ec);
        final boolean gsiConcurrentWrite =
            ec.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(insert.getSchemaName()))
            .getRuleManager();

        final boolean isBroadcast = or.isBroadCast(insert.getLogicalTableName());
        final boolean canGsiConcurrentWrite = !hasIndex || gsiConcurrentWrite;
        //能否多线程执行Insert,即将select的数据切割，可并行执行doExecute：
        // 1.广播表策略FIRST_THEN_CONCURRENT,不用多线程
        // 2.是简单的Insert,有些特殊的sql语句，继承LogicalInsert：LogicalInsertIgnore、LogicalReplace、LogicalUpsert等doExecute不同
        // 3.如果有gsi，gsi支持并行写
        // 4.不支持含有生成列或正在进行 OMC 的表

        String schemaName = insert.getSchemaName();
        String tableName = insert.getLogicalTableName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final boolean isColumnMultiWriting = TableColumnUtils.isModifying(schemaName, tableName, ec);
        final boolean containGeneratedColumn = tableMeta.hasLogicalGeneratedColumn();

        final boolean canMultiInsert =
            !isBroadcast && canGsiConcurrentWrite && !isColumnMultiWriting && !containGeneratedColumn;
        boolean insertSelectByMpp = ec.getParamManager().getBoolean(ConnectionParams.INSERT_SELECT_MPP);
        //用户通过hint指定MPP运行
        if (insertSelectByMpp) {
            if (!canMultiInsert) {
                throw new TddlRuntimeException(ErrorCode.ERR_INSERT_SELECT,
                    "This InsertSelect SQL isn't supported use MPP.");
            }
            insert.setInsertSelectMode(LogicalInsert.InsertSelectMode.MPP);
            return insert;
        }
        //默认开启:多线程执行insert,可hint关闭
        boolean insertSelectByMulti = ec.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);
        if (insertSelectByMulti && canMultiInsert) {
            insert.setInsertSelectMode(LogicalInsert.InsertSelectMode.MULTI);
        }
        return insert;
    }

    private static Map<String, ColumnMeta> getColumnMetaMap(TableMeta tableMeta,
                                                            Map<String, List<List<String>>> ukGroupByTable) {
        Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ukGroupByTable.values().forEach(key -> key.forEach(
            cols -> cols.forEach(col -> columnMetaMap.put(col, tableMeta.getColumnIgnoreCase(col)))));
        return columnMetaMap;
    }

    private static boolean ukContainGeneratedColumn(TableMeta tableMeta, Set<String> ukColumns) {
        return tableMeta.getGeneratedColumnNames().stream().anyMatch(ukColumns::contains);
    }

    private static boolean allColumnsSupportPartField(Map<String, ColumnMeta> columnMetaMap) {
        return columnMetaMap.values().stream().allMatch(cm -> {
            try {
                if (cm.getDataType() instanceof BinaryType) {
                    return false;
                }
                PartitionField partitionField = PartitionFieldBuilder.createField(cm.getDataType());
            } catch (Throwable ex) {
                return false;
            }
            return true;
        });
    }

    protected Map<String, List<List<String>>> groupUkByTable(LogicalInsertIgnore insertIgnore,
                                                             ExecutionContext executionContext) {
        // Map uk to table
        Map<String, List<List<String>>> tableUkMap = new HashMap<>();
        final String schemaName = insertIgnore.getSchemaName();
        final String primaryTableName = insertIgnore.getLogicalTableName();

        // Get plan for finding duplicate values
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        final SchemaManager sm = executionContext.getSchemaManager(schemaName);
        assert oc != null;
        final TableMeta baseTableMeta = sm.getTable(primaryTableName);

        // Get all uk constraints from WRITABLE tables
        // [[columnName(upper case)]]
        List<List<String>> uniqueKeys = new ArrayList<>(new HashSet<>(
            GlobalIndexMeta.getUniqueKeys(primaryTableName, schemaName, true,
                tm -> GlobalIndexMeta.canWrite(executionContext, tm), executionContext)));

        // Only lookup primary table, could be
        // 1. Set by hint
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_USING_GSI)) {
            tableUkMap.put(primaryTableName, uniqueKeys);
            return tableUkMap;
        }

        final GsiMetaManager.GsiTableMetaBean gsiTableMeta = baseTableMeta.getGsiTableMetaBean();
        List<String> writableIndexTables = new ArrayList<>();
        // Get all PUBLIC / WRITE_ONLY gsi
        if (null != gsiTableMeta && GeneralUtil.isNotEmpty(gsiTableMeta.indexMap)) {
            gsiTableMeta.indexMap.entrySet().stream().filter(e -> GlobalIndexMeta.canWrite(executionContext,
                    sm.getTable(e.getValue().indexName)))
                .forEach(e -> writableIndexTables.add(e.getKey().toUpperCase()));
        }

        // Get all tables' local uk, include PUBLIC / WRITE_ONLY gsi
        // tableName -> [indexName -> [columnName(upper case)]]
        Map<String, Map<String, Set<String>>> writableTableUkMap =
            insertIgnore.getTableUkMap()
                .entrySet()
                .stream()
                .filter(e -> writableIndexTables.contains(e.getKey().toUpperCase())
                    || primaryTableName.equalsIgnoreCase(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Map uk to tables, must be exact match for uk
        // i -> [tableName]
        Map<Integer, List<String>> ukAllTableMap = new HashMap<>();
        for (int i = 0; i < uniqueKeys.size(); i++) {
            List<String> uniqueKey = uniqueKeys.get(i);
            for (Map.Entry<String, Map<String, Set<String>>> e : writableTableUkMap.entrySet()) {
                String currentTableName = e.getKey().toUpperCase();
                Map<String, Set<String>> currentUniqueKeys = e.getValue();
                // At least match one uk in table
                if (currentUniqueKeys.values().stream().anyMatch(
                    currentUniqueKey -> currentUniqueKey.size() == uniqueKey.size() && currentUniqueKey.containsAll(
                        uniqueKey))) {
                    ukAllTableMap.computeIfAbsent(i, k -> new ArrayList<>()).add(currentTableName);
                }
            }
        }

        List<String> primaryKey = new ArrayList<>();
        if (baseTableMeta.getPrimaryIndex() != null) {
            primaryKey.addAll(
                baseTableMeta.getPrimaryIndex().getKeyColumns().stream().map(cm -> cm.getName().toUpperCase())
                    .collect(Collectors.toList()));
        }

        // Best table for all uk is the table contains all uk columns
        // and every uk contains all partition columns of this table
        final Map<String, List<List<String>>> bestTableUkMap = findBestTableForAllUk(ukAllTableMap,
            uniqueKeys,
            primaryKey,
            schemaName,
            primaryTableName,
            executionContext);

        if (bestTableUkMap.isEmpty()) {
            for (Map.Entry<Integer, List<String>> e : ukAllTableMap.entrySet()) {
                List<String> tableNames = e.getValue();
                List<String> uniqueKey = uniqueKeys.get(e.getKey());
                boolean isPrimary = uniqueKey.containsAll(primaryKey) && primaryKey.containsAll(uniqueKey);

                // PK must be searched on primary table
                String ukTargetTable = isPrimary ? primaryTableName :
                    getUkTargetTable(schemaName, primaryTableName, uniqueKey, tableNames, executionContext);
                tableUkMap.computeIfAbsent(ukTargetTable.toUpperCase(), k -> new ArrayList<>())
                    .add(uniqueKeys.get(e.getKey()));
            }
        } else {
            tableUkMap.putAll(bestTableUkMap);
        }

        return tableUkMap;
    }

    /**
     * @param ukAllTableMap map[uk index, tables contains this uk]
     * @param uniqueKeys map[uk index, columns of this uk]
     */
    private static @NotNull Map<String, List<List<String>>> findBestTableForAllUk(
        Map<Integer, List<String>> ukAllTableMap,
        List<List<String>> uniqueKeys,
        List<String> primaryKey,
        String schemaName,
        String primaryTableName,
        ExecutionContext ec) {
        final Map<String, List<List<String>>> bestTableUkMap = new HashMap<>();
        final Set<String> pkSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        pkSet.addAll(primaryKey);

        final SchemaManager sm = ec.getSchemaManager(schemaName);

        final Optional<List<String>> bestTableCandidates = ukAllTableMap
            .entrySet()
            .stream()
            // skip primary key
            .filter(e -> !(uniqueKeys.get(e.getKey()).containsAll(primaryKey)
                && pkSet.containsAll(uniqueKeys.get(e.getKey()))))
            .map(e -> e.getValue()
                .stream()
                // exclude unpublished gsi
                .filter(tableName -> tableName.equalsIgnoreCase(primaryTableName)
                    || GlobalIndexMeta.isPublished(ec, sm.getTable(tableName)))
                .collect(Collectors.toList()))
            // exclude empty table candidates list
            .filter(candidates -> !candidates.isEmpty())
            .findFirst();

        if (bestTableCandidates.isPresent()) {
            final TddlRuleManager rm = OptimizerContext.getContext(schemaName).getRuleManager();

            for (String bestTable : bestTableCandidates.get()) {
                boolean bestForAllUk = true;
                final List<List<String>> ukList = new ArrayList<>();

                for (Map.Entry<Integer, List<String>> e : ukAllTableMap.entrySet()) {
                    final Set<String> ukTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    ukTableNames.addAll(e.getValue());
                    final Set<String> ukColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    ukColumnSet.addAll(uniqueKeys.get(e.getKey()));

                    final boolean isPrimary = ukColumnSet.containsAll(pkSet) && pkSet.containsAll(ukColumnSet);

                    if (isPrimary) {
                        // PK must be searched on primary table
                        continue;
                    }

                    if (!ukTableNames.contains(bestTable)
                        || !ukColumnSet.containsAll(rm.getSharedColumns(bestTable))) {
                        bestForAllUk = false;
                        break;
                    }

                    ukList.add(uniqueKeys.get(e.getKey()));
                }

                if (bestForAllUk) {
                    bestTableUkMap.put(bestTable.toUpperCase(), ukList);

                    // PK must be searched on primary table
                    bestTableUkMap.computeIfAbsent(
                            primaryTableName.toUpperCase(),
                            k -> new ArrayList<>())
                        .add(primaryKey);
                    break;
                }
            }
        } else {
            // PK only and PK must be searched on primary table
            bestTableUkMap.computeIfAbsent(primaryTableName.toUpperCase(), k -> new ArrayList<>()).add(primaryKey);
        }
        return bestTableUkMap;
    }

    private Map<String, List<String>> getLocalIndexName(Map<String, List<List<String>>> tableUkMap, String schemaName,
                                                        ExecutionContext executionContext) {
        // Get local index name so that we can use FORCE INDEX later
        Map<String, List<String>> localIndexName = new HashMap<>();
        for (Map.Entry<String, List<List<String>>> entry : tableUkMap.entrySet()) {
            String tableName = entry.getKey();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            List<IndexMeta> indexMetas = tableMeta.getUniqueIndexes(true);
            for (List<String> uniqueKey : entry.getValue()) {
                // phyIndexName could be null since user may choose to select all uk from primary table, which may not
                // contains corresponding local uk
                String phyIndexName = null;
                for (IndexMeta indexMeta : indexMetas) {
                    Set<String> indexColumns = indexMeta.getKeyColumns().stream().map(cm -> cm.getName().toUpperCase())
                        .collect(Collectors.toCollection(HashSet::new));
                    if (indexColumns.size() == uniqueKey.size() && indexColumns.containsAll(uniqueKey)) {
                        phyIndexName = indexMeta.getPhysicalIndexName();
                        break;
                    }
                }
                localIndexName.computeIfAbsent(tableName, k -> new ArrayList<>()).add(phyIndexName);
            }
        }
        return localIndexName;
    }

    private String getUkTargetTable(String schemaName, String primaryTableName, List<String> uniqueKey,
                                    List<String> tableNames, ExecutionContext executionContext) {
        final TddlRuleManager rm = OptimizerContext.getContext(schemaName).getRuleManager();
        final SchemaManager sm = executionContext.getSchemaManager(schemaName);
        final Set<String> ukColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukColumnSet.addAll(uniqueKey);

        final Set<String> tablesPartitionedByUk = tableNames
            .stream()
            .filter(tableName -> ukColumnSet.containsAll(rm.getSharedColumns(tableName)))
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        final Set<String> primaryTableAndPublicGsiNames = tableNames
            .stream()
            .filter(tableName -> tableName.equalsIgnoreCase(primaryTableName)
                || GlobalIndexMeta.isPublished(executionContext, sm.getTable(tableName)))
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

        // If primary table is partitioned by uk
        if (tablesPartitionedByUk.contains(primaryTableName)) {
            return primaryTableName;
        }

        // Try to use table whose sharding key is included in this uk first to avoid full table scan, should
        // improve small batch performance
        for (String tableName : primaryTableAndPublicGsiNames) {
            if (tablesPartitionedByUk.contains(tableName)) {
                return tableName;
            }
        }

        // If no table partitioned by uk, check primary table contains uk first
        if (primaryTableAndPublicGsiNames.contains(primaryTableName)) {
            return primaryTableName;
        }

        for (String tableName : primaryTableAndPublicGsiNames) {
            return tableName;
        }

        // Only WRITE_ONLY GSI contains this UK
        for (String tableName : tableNames) {
            if (tablesPartitionedByUk.contains(tableName)) {
                return tableName;
            }
        }

        for (String tableName : tableNames) {
            return tableName;
        }

        // One of the UK can not find corresponding tables, which should be impossible
        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "can not find corresponding gsi for uk " + uniqueKey);
    }

    private LogicalInsert processOnDuplicateKeyUpdate(LogicalInsert upsert,
                                                      AtomicInteger maxParamIndex) {
        final RexBuilder rexBuilder = upsert.getCluster().getRexBuilder();
        final List<RexNode> duplicateKeyUpdateList = upsert.getDuplicateKeyUpdateList().stream()
            .map(rex -> {
                final RexCall rexCall = (RexCall) rex;
                final RexNode value = rexCall.getOperands().get(1)
                    .accept(new ReplaceRexCallWithParamVisitor(maxParamIndex, true, true, true));
                return rexBuilder.makeCall(rexCall.op, rexCall.getOperands().get(0), value);
            }).collect(Collectors.toList());

        final LogicalInsert result = new LogicalInsert(upsert.getCluster(),
            upsert.getTraitSet(),
            upsert.getTable(),
            upsert.getCatalogReader(),
            upsert.getInput(),
            upsert.getOperation(),
            upsert.isFlattened(),
            upsert.getInsertRowType(),
            upsert.getKeywords(),
            duplicateKeyUpdateList,
            upsert.getBatchSize(),
            upsert.getAppendedColumnIndex(),
            upsert.getHints(),
            upsert.getTableInfo());
        result.setAutoIncParamIndex(upsert.getAutoIncParamIndex());

        return result;
    }

    private LogicalInsert processOnDuplicateKeyUpdateForNondeterministic(LogicalInsert upsert,
                                                                         AtomicInteger maxParamIndex) {
        final RexBuilder rexBuilder = upsert.getCluster().getRexBuilder();
        final RexUtils.ColumnRefFinder columnRefFinder = new RexUtils.ColumnRefFinder();
        final List<RexNode> duplicateKeyUpdateList = upsert.getDuplicateKeyUpdateList().stream()
            .map(rex -> {
                final RexCall rexCall = (RexCall) rex;
                final RexNode value = rexCall.getOperands().get(1)
                    .accept(new ReplaceRexCallForBroadcastVisitor(maxParamIndex, true, false,
                        (r) -> !columnRefFinder.analyze(r)));
                return rexBuilder.makeCall(rexCall.op, rexCall.getOperands().get(0), value);
            }).collect(Collectors.toList());

        final LogicalInsert result = new LogicalInsert(upsert.getCluster(),
            upsert.getTraitSet(),
            upsert.getTable(),
            upsert.getCatalogReader(),
            upsert.getInput(),
            upsert.getOperation(),
            upsert.isFlattened(),
            upsert.getInsertRowType(),
            upsert.getKeywords(),
            duplicateKeyUpdateList,
            upsert.getBatchSize(),
            upsert.getAppendedColumnIndex(),
            upsert.getHints(),
            upsert.getTableInfo());
        result.setAutoIncParamIndex(upsert.getAutoIncParamIndex());
        return result;
    }

    private static boolean isModifyUniqueKey(List<String> updateColumnList, String logicalTableName, String schema,
                                             ExecutionContext ec) {
        final Set<String> ukColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukColumnSet.addAll(GlobalIndexMeta.getUniqueKeyColumnList(logicalTableName, schema, true, ec));
        return updateColumnList.stream().anyMatch(ukColumnSet::contains);
    }

    boolean checkInputInTableOrder(LogicalInsert logicalInsert, TableMeta tableMeta) {
        // Here all columns should be appended in tddlSqlToRelConverter
        List<String> tableColumns =
            tableMeta.getPhysicalColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        List<String> inputColumns = logicalInsert.getInsertRowType().getFieldNames();

        Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < tableColumns.size(); i++) {
            columnIndexMap.put(tableColumns.get(i), i);
        }
        int prev = -1;
        for (String column : inputColumns) {
            int current = columnIndexMap.get(column);
            if (current < prev) {
                return false;
            }
            prev = current;
        }
        return true;
    }

    /**
     * Get column that referenced by ON DUPLICATE KEY UPDATE for before value
     */
    private static class BeforeInputFinder extends RelOptUtil.InputFinder {
        public boolean withInputRef = false;

        public static BeforeInputFinder analyze(RexNode node) {
            final BeforeInputFinder inputFinder = new BeforeInputFinder();
            node.accept(inputFinder);
            return inputFinder;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            withInputRef = true;
            return super.visitInputRef(inputRef);
        }

        @Override
        public Void visitCall(RexCall call) {
            final SqlOperator op = call.getOperator();
            if ("VALUES".equalsIgnoreCase(op.getName())) {
                // Values(c1) is referencing after value of c1
                return null;
            }
            return super.visitCall(call);
        }
    }

    private static List<List<Integer>> initGsiColumnMapping(List<String> primaryColumnNames, List<TableMeta> gsiMetas) {
        final List<List<Integer>> gsiColumnMappings = new ArrayList<>();
        IntStream.range(0, primaryColumnNames.size()).forEach(i -> {
            final String column = primaryColumnNames.get(i);

            Ord.zip(gsiMetas).forEach(o -> {
                final TableMeta gsi = o.e;
                final int gsiIndex = o.i;
                if (gsiColumnMappings.size() <= gsiIndex) {
                    gsiColumnMappings.add(new ArrayList<>());
                }

                if (gsi.containsColumn(column)) {
                    gsiColumnMappings.get(gsiIndex).add(i);
                }
            });
        });
        return gsiColumnMappings;
    }

    /**
     * Replace rexCall in LogicalDynamicValues with RexCallParam
     * New parameter will be append to the end of current parameter row
     *
     * @param insert Insert plan
     * @param maxParamIndex Max parameter index of current parameter row
     * @return Insert plan with new LogicalDynamicValues
     */
    private LogicalInsert processRexCall(LogicalInsert insert, AtomicInteger maxParamIndex, ExecutionContext ec) {
        final Set<Integer> literalColumnIndex = new HashSet<>(insert.getLiteralColumnIndex());
        final Set<Integer> deterministicColumnIndex = new HashSet<>(insert.getDeterministicColumnIndex(ec));

        final LogicalDynamicValues input = RelUtils.getRelInput(insert);
        final AtomicBoolean withRexCallParam = new AtomicBoolean(false);
        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        input.tuples.forEach(tuple -> {
            final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();
            Ord.zip(tuple).forEach(o -> {
                final int columnIndex = o.getKey();
                final RexNode rex = o.getValue();

                final boolean mustBeLiteral = literalColumnIndex.contains(columnIndex);
                final ReplaceRexCallWithParamVisitor visitor =
                    new ReplaceRexCallWithParamVisitor(maxParamIndex, deterministicColumnIndex.contains(columnIndex),
                        mustBeLiteral, mustBeLiteral);
                final RexNode replaced = rex.accept(visitor);

                if (rex != replaced && !withRexCallParam.get()) {
                    withRexCallParam.set(true);
                }

                tupleBuilder.add(replaced);
            });

            tuplesBuilder.add(tupleBuilder.build());
        });

        LogicalInsert newInsert = insert;
        if (withRexCallParam.get()) {
            newInsert = new LogicalInsert(insert.getCluster(),
                insert.getTraitSet(),
                insert.getTable(),
                insert.getCatalogReader(),
                LogicalDynamicValues.createDrdsValues(input.getCluster(), input.getTraitSet(), input.getRowType(),
                    tuplesBuilder.build()),
                insert.getOperation(),
                insert.isFlattened(),
                insert.getInsertRowType(),
                insert.getKeywords(),
                insert.getDuplicateKeyUpdateList(),
                insert.getBatchSize(),
                insert.getAppendedColumnIndex(),
                insert.getHints(),
                insert.getTableInfo());
        }
        return newInsert;
    }

    /**
     * 1. Field for sequence is Literal: replace it with a dynamicParam.
     * 2. Field for sequence is DynamicParam: record its param index.
     *
     * @param logicalInsert original logicalInsert
     * @param maxParamIndex max param index of current current insert
     * @return the new logicalInsert
     */
    private LogicalInsert processAutoInc(LogicalInsert logicalInsert, AtomicInteger maxParamIndex,
                                         ExecutionContext ec) {
        final String tableName = logicalInsert.getLogicalTableName();
        final String schemaName = logicalInsert.getSchemaName();
        final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final Set<String> autoIncColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final LogicalDynamicValues values = RelUtils.getRelInput(logicalInsert);
        final List<RelDataTypeField> fields = values.getRowType().getFieldList();

        autoIncColumns.addAll(tableMeta.getAutoIncrementColumns());

        // It has been checked that autoIncNode will only be dynamicParam or literal
        if (autoIncColumns.isEmpty()) {
            return logicalInsert;
        }

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        if (or.isTableInSingleDb(tableName) || or.isBroadCast(tableName)) {
            if (!SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
                // It's using MySQL auto increment rule
                return logicalInsert;
            }
        }

        final Set<Integer> autoIncColumnIndex = new HashSet<>();
        Ord.zip(fields).stream().filter(o -> autoIncColumns.contains(o.getValue().getName()))
            .forEach(o -> autoIncColumnIndex.add(o.getKey()));

        final ImmutableList<ImmutableList<RexNode>> newTuples =
            replaceSequenceCallWithParam(logicalInsert, values.getTuples(), maxParamIndex, autoIncColumnIndex);

        final LogicalInsert result = new LogicalInsert(logicalInsert.getCluster(),
            logicalInsert.getTraitSet(),
            logicalInsert.getTable(),
            logicalInsert.getCatalogReader(),
            LogicalDynamicValues.createDrdsValues(
                values.getCluster(), values.getTraitSet(), values.getRowType(), newTuples),
            logicalInsert.getOperation(),
            logicalInsert.isFlattened(),
            logicalInsert.getInsertRowType(),
            logicalInsert.getKeywords(),
            logicalInsert.getDuplicateKeyUpdateList(),
            logicalInsert.getBatchSize(),
            logicalInsert.getAppendedColumnIndex(),
            logicalInsert.getHints(),
            logicalInsert.getTableInfo());

        return result;
    }

    public static ImmutableList<ImmutableList<RexNode>> replaceSequenceCallWithParam(LogicalInsert logicalInsert,
                                                                                     ImmutableList<ImmutableList<RexNode>> tuples,
                                                                                     AtomicInteger nextParamIndex,
                                                                                     Set<Integer> autoIncColumnIndex) {
        final String tableName = logicalInsert.getLogicalTableName();
        final List<RelDataTypeField> fields = logicalInsert.getInsertRowType().getFieldList();
        final RexBuilder rexBuilder = logicalInsert.getCluster().getRexBuilder();

        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        for (ImmutableList<RexNode> tuple : tuples) {
            final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();

            for (Ord<RexNode> o1 : Ord.zip(tuple)) {
                final Integer columnIndex = o1.getKey();
                final RelDataType fieldType = fields.get(columnIndex).getType();

                RexNode value = o1.getValue();

                final boolean isRexCallParam = value instanceof RexCallParam;
                final boolean isSeqCall = RexUtils.isSeqCall(value);
                final boolean isSeqCallInRexCall =
                    isRexCallParam && RexUtils.isSeqCall(((RexCallParam) value).getRexCall());

                if (isSeqCallInRexCall && autoIncColumnIndex.contains(columnIndex)) {
                    final RexCallParam callParam = (RexCallParam) value;
                    final RexCall seqCall = (RexCall) callParam.getRexCall();
                    final RexLiteral seqNameLiteral = (RexLiteral) seqCall.getOperands().get(0);
                    final String seqName = seqNameLiteral.getValueAs(String.class);

                    if ((ISequenceManager.AUTO_SEQ_PREFIX + tableName).equalsIgnoreCase(seqName)) {
                        final RexSequenceParam sequenceParam =
                            new RexSequenceParam(fieldType, callParam.getIndex(), seqCall);

                        value = new RexCallParam(callParam.getType(), callParam.getIndex(), rexBuilder.constantNull());
                        ((RexCallParam) value).setSequenceCall(sequenceParam);

                        tupleBuilder.add(value);
                        continue;
                    }
                }

                // Evaluate RexCall then replace dynamic parameter for implicit sequence call
                if (isRexCallParam && autoIncColumnIndex.contains(columnIndex)) {
                    final RexNode seqName = rexBuilder.makeLiteral(ISequenceManager.AUTO_SEQ_PREFIX + tableName);
                    final RexNode falseLiteral = rexBuilder.makeLiteral(false);
                    final RexNode sequenceCall = rexBuilder.makeCall(NEXTVAL, ImmutableList.of(seqName, falseLiteral));

                    final RexCallParam callParam = (RexCallParam) value;
                    callParam.setSequenceCall(new RexSequenceParam(fieldType, callParam.getIndex(), sequenceCall));

                    tupleBuilder.add(value);
                    continue;
                }

                // DO NOT replace NULL or RexLiteral with dynamic parameter for implicit sequence call,
                // because implicit sequence call has affect on result of last_insert_id

                // Replace seq.NextVal with dynamic parameter for explicit sequence call
                if (isSeqCall) {
                    value = new RexSequenceParam(fieldType, nextParamIndex.incrementAndGet(), value);
                }

                tupleBuilder.add(value);
            }

            tuplesBuilder.add(tupleBuilder.build());
        }

        return tuplesBuilder.build();
    }

    /**
     * Replace RexCall and Sequence with RexDynamicParam
     *
     * @param logicalExecute Logical execute means SELECT all rows might be affected first, then execute INSERT/UPDATE/DELETE with condition of primary key
     */
    private LogicalInsert replaceExpAndSeqWithParam(LogicalInsert origin, boolean logicalExecute,
                                                    boolean replaceNondeterministicOnly, ExecutionContext ec) {
        final boolean sourceSelect = origin.isSourceSelect();

        LogicalInsert result = (LogicalInsert) origin.copy(origin.getTraitSet(), origin.getInputs());
        final AtomicInteger maxParamIndex = getMaxParamIndex(origin);

        if (!sourceSelect) {
            // Replace expression with parameter
            result = processRexCall(origin, maxParamIndex, ec);

            // Add parameter for auto increment column which is not included in origin sql
            result = processAutoInc(result, maxParamIndex, ec);
        }

        // Add parameter for ON DUPLICATE KEY UPDATE list
        if (origin.isUpsert()) {
            if (logicalExecute) {
                result = processOnDuplicateKeyUpdate(result, maxParamIndex);
            } else if (replaceNondeterministicOnly) {
                // For broadcast table, here is a little bit tricky
                result = processOnDuplicateKeyUpdateForNondeterministic(result, maxParamIndex);
            }
        }

        boolean rebuildMultiValues = !sourceSelect && (logicalExecute || replaceNondeterministicOnly);
        if (rebuildMultiValues) {
            final LogicalDynamicValues oldInput = RelUtils.getRelInput(result);
            rebuildMultiValues &= oldInput.getTuples().size() > 1;
        }

        LogicalInsert newInsertOrReplace = result;
        if (rebuildMultiValues) {
            /**
             * <pre>
             * Build a new LogicalInsert with new LogicalDynamicValues, also save the old LogicalDynamicValues for later
             * compute the Parameters in RexUtils.calculateAndUpdateAllRexCallParams().
             * the new LogicalDynamicValues only has one tuple, we will execute the LogicalInsert in batch mode, for the batch
             * parameters(for each tuple), we will compute them in the execution time.
             * for the case:
             * insert into t1(a,b) values(1,now()),(2+3,'2010-10-12 12:12:12'),(5, null)
             * --> change to -->
             * insert into t1(a,b) values(?,?), and save the old LogicalDynamicValues[(?,?),(?,?),(?, ?)]
             * </pre>
             */
            final String schemaName = origin.getSchemaName();
            final String tableName = origin.getLogicalTableName();
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

            final List<Integer> autoIncParamIndex = new ArrayList<>();
            final LogicalDynamicValues oldInput = RelUtils.getRelInput(result);
            final LogicalDynamicValues newInput = Optional.of(oldInput.getTuples()).filter(i -> i.size() > 1).map(
                    i -> LogicalDynamicValues
                        .createDrdsValues(oldInput.getCluster(), oldInput.getTraitSet(), oldInput.getRowType(),
                            buildNewTupleForLogicalDynamicValue(oldInput, autoIncParamIndex, ec, tableMeta)))
                .orElse(oldInput);
            final int batchSize =
                (result.getBatchSize() == 0 && oldInput.getTuples().size() > 1) ? oldInput.getTuples().size() :
                    result.getBatchSize();
            newInsertOrReplace =
                new LogicalInsert(result.getCluster(), result.getTraitSet(), result.getTable(),
                    result.getCatalogReader(), newInput, result.getOperation(),
                    result.isFlattened(), result.getInsertRowType(), result.getKeywords(),
                    result.getDuplicateKeyUpdateList(), batchSize/*set the batch size*/,
                    result.getAppendedColumnIndex(), result.getHints(),
                    result.getTableInfo(), result.getPrimaryInsertWriter(),
                    result.getGsiInsertWriters(), autoIncParamIndex, null, null, result.getEvalRowColMetas(),
                    result.getGenColRexNodes(), result.getInputToEvalFieldsMapping(), result.getDefaultExprColMetas(),
                    result.getDefaultExprColRexNodes(), result.getDefaultExprEvalFieldsMapping(),
                    result.isPushablePrimaryKeyCheck(), result.isPushableForeignConstraintCheck(),
                    result.isModifyForeignKey(), result.isUkContainsAllSkAndGsiContainsAllUk());
            /**
             * 2、update the index of RexDynamicParam in onDuplicatedUpdate list recursively
             * how to update them? firstly, find out the minimum RexDynamicPara and compute the offset
             * between the maximum RexDynamicParam of the new param(the last column of the last row),
             * then update the index of RexDynamicParam in onDuplicatedUpdate list recursively by plus the offset
             */
            if (GeneralUtil.isNotEmpty(newInsertOrReplace.getDuplicateKeyUpdateList())) {
                final List<RexNode> onDuplicatedUpdate = newInsertOrReplace.getDuplicateKeyUpdateList();
                RexUtils.FindMinDynamicParam findMinDynamicParam = new RexUtils.FindMinDynamicParam();
                onDuplicatedUpdate.forEach(o -> ((RexCall) o).getOperands().get(1).accept(findMinDynamicParam));

                final AtomicInteger mapParamIndex = new AtomicInteger(0);
                final List<RexDynamicParam> params = newInput.tuples.stream().flatMap(Collection::stream)
                    .flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList());
                params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(mapParamIndex::set);

                int minPara = findMinDynamicParam.getMinDynamicParam();
                if (minPara != Integer.MAX_VALUE) {
                    int tupleSize = result.getBatchSize() == 0 ? oldInput.getTuples().size() : result.getBatchSize();
                    int offset = (mapParamIndex.get() + 1) * tupleSize - minPara;
                    RexUtils.ReplaceDynamicParam replaceDynamicParam = new RexUtils.ReplaceDynamicParam(offset);
                    //no action need for offset = 0
                    if (offset != 0) {
                        final RexBuilder rexBuilder = newInsertOrReplace.getCluster().getRexBuilder();
                        final List<RexNode> newOnDuplicatedUpdate = new ArrayList<>(onDuplicatedUpdate.size());
                        onDuplicatedUpdate
                            .forEach(o -> {
                                RexNode rexNode = ((RexCall) o).getOperands().get(1).accept(replaceDynamicParam);
                                List<RexNode> operands = new ArrayList<>(2);
                                operands.add(((RexCall) o).getOperands().get(0));
                                operands.add(rexNode);
                                RexNode rexCall = rexBuilder.makeCall(((RexCall) o).getOperator(), operands);
                                newOnDuplicatedUpdate.add(rexCall);
                            });
                        newInsertOrReplace.setDuplicateKeyUpdateList(newOnDuplicatedUpdate);
                    }
                }
            }
            newInsertOrReplace.setUnOptimizedLogicalDynamicValues(oldInput);
            newInsertOrReplace.setUnOptimizedDuplicateKeyUpdateList(result.getDuplicateKeyUpdateList());
        }
        return RelUtils.removeHepRelVertex(newInsertOrReplace);
    }

    private static AtomicInteger getMaxParamIndex(LogicalInsert insertOrReplace) {
        final boolean sourceSelect = insertOrReplace.isSourceSelect();

        final AtomicInteger maxParamIndex = new AtomicInteger(-1);
        if (sourceSelect) {
            // Get max parameter index
            if (insertOrReplace.isUpsert()) {
                final List<RexDynamicParam> params = Optional.ofNullable(insertOrReplace.getDuplicateKeyUpdateList())
                    .map(dl -> dl.stream().flatMap(p -> RexUtils.ParamFinder.getParams(p).stream())
                        .collect(Collectors.toList())).orElseGet(ArrayList::new);

                params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(maxParamIndex::set);
            } else {
                maxParamIndex.set(insertOrReplace.getInsertRowType().getFieldCount());
            }
        } else {
            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);
            final List<RexDynamicParam> params = input.tuples.stream().flatMap(Collection::stream)
                .flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList());

            params.addAll(Optional.ofNullable(insertOrReplace.getDuplicateKeyUpdateList()).map(
                    dl -> dl.stream().flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList()))
                .orElseGet(ArrayList::new));

            params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(maxParamIndex::set);
        }
        return maxParamIndex;
    }

    /**
     * <pre>
     * Check type and compute functions.
     * If the function is a sharding key, compute it.
     * If the function can not be pushed down ( like LAST_INSERT_ID() ), compute it.
     * If the function can be pushed down, check its operands, which may be functions can't be pushed down.
     * If the function is not deterministic, compute it.
     * If a parent node need to be computed, its child node must also be computed.
     * If a child node is cloned, its parent node must also be cloned.
     * </pre>
     */
    private class ReplaceRexCallWithParamVisitor extends RexShuttle {

        private final AtomicInteger currentParamIndex;
        private final Deque<Boolean> isTop = new ArrayDeque<>();

        /**
         * For logical write, if the function is not deterministic, it should be calculated.
         */
        private boolean logicalWrite;
        /**
         * If the function can't be pushed down, all its operands must be computed too.
         */
        private boolean doReplace;

        private boolean replaceLiteral;

        ReplaceRexCallWithParamVisitor(AtomicInteger currentParamIndex, boolean logicalWrite, boolean doReplace) {
            this(currentParamIndex, logicalWrite, doReplace, false);
        }

        ReplaceRexCallWithParamVisitor(AtomicInteger currentParamIndex, boolean logicalWrite,
                                       boolean forceReplace, boolean replaceLiteral) {
            this.logicalWrite = logicalWrite;
            this.currentParamIndex = currentParamIndex;
            this.doReplace = forceReplace;
            this.replaceLiteral = replaceLiteral;
            this.isTop.push(true);
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            if (this.replaceLiteral && Boolean.TRUE.equals(isTop.peek())) {
                return new RexCallParam(literal.getType(), currentParamIndex.incrementAndGet(), literal);
            } else {
                return super.visitLiteral(literal);
            }
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (Boolean.TRUE.equals(isTop.peek())) {
                return new RexCallParam(inputRef.getType(), currentParamIndex.incrementAndGet(), inputRef);
            } else {
                doReplace = true;
                return super.visitInputRef(inputRef);
            }
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            RexNode visited = null;

            this.isTop.push(false);
            try {
                visited = super.visitCall(call);
            } finally {
                this.isTop.pop();
            }

            // If the function can't be pushed down, all its operands must be computed too.
            doReplace |= !call.getOperator().canPushDown() || (logicalWrite && call.getOperator().isDynamicFunction());

            // If function is last_insert_id with no operands, compute
            doReplace |=
                Objects.equals(call.getOperator().getName(), LastInsertId.NAME) && call.getOperands().size() == 0;

            if (!doReplace) {
                return visited;
            }

            if (Boolean.TRUE.equals(isTop.peek())) {
                // Replace top RexNode with RexCallParam
                return new RexCallParam(call.getType(), currentParamIndex.incrementAndGet(), call);
            }

//            if (call.getOperator() == TddlOperatorTable.NEXTVAL) {
//                // If it's a nested seq.nextVal, we can't compute.
//                throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "'" + call + "'");
//            }

            return visited;
        }
    }

    private class ReplaceRexCallForBroadcastVisitor extends RexShuttle {
        private final AtomicInteger currentParamIndex;
        private final Deque<Boolean> isTop = new ArrayDeque<>();
        private final Deque<Boolean> isComputable = new ArrayDeque<>();

        /**
         * For logical write, if the function is not deterministic, it should be calculated.
         */
        private boolean logicalWrite;
        /**
         * If the function can't be pushed down, all its operands must be computed too.
         */
        private boolean doReplace;

        private Predicate<RexNode> computable;

        ReplaceRexCallForBroadcastVisitor(AtomicInteger currentParamIndex, boolean logicalWrite, boolean forceReplace,
                                          Predicate<RexNode> computable) {
            this.logicalWrite = logicalWrite;
            this.currentParamIndex = currentParamIndex;
            this.doReplace = forceReplace;
            this.isTop.push(true);
            this.computable = computable;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            RexNode visited = null;

            Boolean currentRexComputable = this.isComputable.peek();
            if (!Boolean.TRUE.equals(currentRexComputable)) {
                currentRexComputable = this.computable.test(call);
            }

            if (Boolean.TRUE.equals(currentRexComputable)) {
                this.isTop.push(false);
                try {
                    visited = super.visitCall(call);
                } finally {
                    this.isTop.pop();
                }

                // If the function can't be pushed down, all its operands must be computed too.
                doReplace |=
                    !call.getOperator().canPushDown() || (logicalWrite && call.getOperator().isDynamicFunction());

                if (!doReplace) {
                    return visited;
                }

                if (Boolean.TRUE.equals(isTop.peek())) {
                    // Replace top RexNode with RexCallParam
                    return new RexCallParam(call.getType(), currentParamIndex.incrementAndGet(), call);
                }

                if (call.getOperator() == TddlOperatorTable.NEXTVAL) {
                    // If it's a nested seq.nextVal, we can't compute.
                    throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "'" + call + "'");
                }

                return visited;
            } else {
                this.isComputable.push(false);
                try {
                    return super.visitCall(call);
                } finally {
                    this.isComputable.pop();
                }
            }
        }
    }

    private ImmutableList<ImmutableList<RexNode>> buildNewTupleForLogicalDynamicValue(LogicalDynamicValues input,
                                                                                      List<Integer> autoIncParamIndex,
                                                                                      ExecutionContext ec,
                                                                                      TableMeta tableMeta) {
        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        final RexBuilder rexBuilder = input.getCluster().getRexBuilder();

        final AtomicInteger sequenceParamIndex = new AtomicInteger(0);
        sequenceParamIndex.addAndGet(
            input.getTuples().get(0).stream().filter(r -> !(r instanceof RexSequenceParam || r instanceof RexLiteral))
                .mapToInt(r -> 1).sum());

        final AtomicInteger rexIndex = new AtomicInteger(0);
        ImmutableList<RexNode> tuple = input.tuples.get(0);
        final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();
        Ord.zip(tuple).forEach(o -> {
            final RexNode rex = o.getValue();
            if (rex instanceof RexSequenceParam) {
                final RexSequenceParam seqCall = (RexSequenceParam) rex;
                final int seqParamIndex = sequenceParamIndex.getAndIncrement();
                tupleBuilder.add(new RexSequenceParam(rex.getType(), seqParamIndex, seqCall.getSequenceCall()));
                if (null != autoIncParamIndex) {
                    autoIncParamIndex.add(seqParamIndex);
                }
            } else {
                tupleBuilder.add(rexBuilder.makeDynamicParam(rex.getType(), rexIndex.getAndIncrement()));
            }
        });

        tuplesBuilder.add(tupleBuilder.build());
        return tuplesBuilder.build();
    }
}
