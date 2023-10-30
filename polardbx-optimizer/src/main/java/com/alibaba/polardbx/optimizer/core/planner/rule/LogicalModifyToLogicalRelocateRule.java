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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.SelectWithLockVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.WriterFactory;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * If 1、UPDATE modify sharding key 2、UPDATE PK in ScaleOut/GSI table backfill phase
 * transform UPDATE to DELETE + INSERT
 *
 * @author chenmo.cm
 */
public class LogicalModifyToLogicalRelocateRule extends RelOptRule {

    public static final LogicalModifyToLogicalRelocateRule INSTANCE = new LogicalModifyToLogicalRelocateRule(
        operand(LogicalModify.class, any()));

    public LogicalModifyToLogicalRelocateRule(RelOptRuleOperand operand) {
        super(operand, LogicalModifyToLogicalRelocateRule.class.getName());
    }

    /**
     * tranfser LogicalModify to: [LogicalRelocate]
     *
     * @param call Rule call
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalModify modify = call.rel(0);
        final ExecutionContext ec = PlannerContext.getPlannerContext(call).getExecutionContext();

        if (!modify.isUpdate()) {
            return;
        }

        if (ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK)
            && !CheckModifyLimitation.checkPushablePrimaryKeyConstraint(modify, ec)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Can not check primary key for update");
        }

        final List<TableModify.TableInfoNode> srcInfos = modify.getTableInfo().getSrcInfos();
        final Map<Integer, List<TableMeta>> tableGsiMap = modify.getTableInfo()
            .getTargetTableIndexSet()
            .stream()
            .map(i -> Pair.of(i, GlobalIndexMeta.getIndex(srcInfos.get(i).getRefTable(), ec)))
            .collect(Collectors.toMap(p -> p.left, p -> p.right));

        final List<String> targetColumns = modify.getUpdateColumnList();
        final List<RelOptTable> targetTables = modify.getTargetTables();
        final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();

        /*
         * Collect column mapping
         */
        final Map<Integer, List<Integer>> primaryUpdateColumnMappings = new HashMap<>();
        final Map<Integer, List<List<Integer>>> gsiUpdateColumnMappings = new HashMap<>();
        BuildPlanUtils.buildColumnMappings(targetColumns, targetTableIndexes, tableGsiMap, primaryUpdateColumnMappings,
            gsiUpdateColumnMappings);

        // Check any sharding key is modified
        final boolean notPrimarySk = primaryUpdateColumnMappings.entrySet().stream().noneMatch(e -> {
            final List<Integer> mapping = e.getValue();
            final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
            final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
            final List<RelOptTable> updateTables = Mappings.permute(targetTables, updateColumnMapping);

            return CheckModifyLimitation.checkModifyShardingColumn(updateColumns, updateTables, (x, y) -> {
            });
        });
        final boolean notModifyGsi =
            gsiUpdateColumnMappings.values().stream().flatMap(List::stream).allMatch(List::isEmpty);
        final boolean primaryTableModifyPk = CheckModifyLimitation.checkModifyPk(modify, ec);
        // primaryTableCanPushDownInScaleOut true：不处于delete/write/org等状态  false: 处于delete/write/org 等状态
        final boolean primaryTableCanPushDownInScaleOut = CheckModifyLimitation.isAllTablesCouldPushDown(modify, ec);
        if (notPrimarySk && notModifyGsi && !primaryTableModifyPk && primaryTableCanPushDownInScaleOut) {
            // Do not modify sharding key or primary key
            return;
        }

        final Map<Integer, List<RelocateWriter>> relocateWriterMap = new HashMap<>();
        final Map<Integer, List<DistinctWriter>> modifyWriterMap = new HashMap<>();

        /*
         * Build writer for primary
         */
        final AtomicBoolean modifyPrimarySk = new AtomicBoolean(false);
        final AtomicBoolean modifyPrimaryWithoutPk = new AtomicBoolean(false);
        final List<String> primaryWithoutPk = new ArrayList<>();
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(call);

        // Expressions in SET are at the end of row
        final RelDataType srcRowType = modify.getInput().getRowType();
        final int fieldCount = srcRowType.getFieldCount();
        final int offset = fieldCount - modify.getUpdateColumnList().size();

        final Map<Integer, Mapping> setColumnTargetMappings = new HashMap<>();
        final Map<Integer, Mapping> setColumnSourceMappings = new HashMap<>();
        final Map<Integer, List<ColumnMeta>> setColumnMetas = new HashMap<>();

        // Primary writer
        final Map<Integer, DistinctWriter> primaryDistinctWriter = new HashMap<>();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = new HashMap<>();

        final Map<Integer, Set<String>> addedAutoUpdateColumnMap = new HashMap<>();

        primaryUpdateColumnMappings.forEach((primaryIndex, mapping) -> {
            final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
            final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
            final List<RelOptTable> updateTables = Mappings.permute(targetTables, updateColumnMapping);
            RelOptTable primary = srcInfos.get(primaryIndex).getRefTable();
            final String schemaName = RelUtils.getSchemaName(primary);

            final List<Integer> setSrc = mapping.stream().map(i -> offset + i).collect(Collectors.toList());
            final Map<String, Integer> columnIndexMap = modify.getSourceColumnIndexMap().get(primaryIndex);

            final String primaryLogicalName = RelUtils.getQualifiedTableName(primary).right;
            final TableMeta primaryTableMeta =
                plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(primaryLogicalName);

            Set<String> autoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            autoUpdateColumns.addAll(primaryTableMeta.getAutoUpdateColumns().stream().map(ColumnMeta::getName).collect(
                Collectors.toList()));
            Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            generatedColumns.addAll(primaryTableMeta.getLogicalGeneratedColumnNames());

            final AtomicInteger extraIndex = new AtomicInteger(fieldCount);
            if (GeneralUtil.isNotEmpty((modify.getExtraTargetColumns()))) {
                extraIndex.addAndGet(-modify.getExtraTargetColumns().size());
            }
            Set<String> addedAutoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Ord.zip(updateColumns).forEach(o -> {
                if (autoUpdateColumns.contains(o.e) && setSrc.get(o.i) >= extraIndex.get()) {
                    addedAutoUpdateColumns.add(o.e);
                }
            });
            addedAutoUpdateColumnMap.put(primaryIndex, addedAutoUpdateColumns);

            // For primary writer, we build all set column mapping to check if this column has updated or not in handler
            final Map<String, Integer> setColumnTargetMap = new LinkedHashMap<>();
            final Map<String, Integer> setColumnSourceMap = new LinkedHashMap<>();

            Ord.zip(updateColumns).forEach(o -> {
                // If it's an auto update column and added by us, or generated columns, we should ignore it when
                // comparing two rows
                if (!(autoUpdateColumns.contains(o.e) && setSrc.get(o.i) >= extraIndex.get())
                    && !generatedColumns.contains(o.e)) {
                    setColumnTargetMap.put(o.e, columnIndexMap.get(o.e));
                    setColumnSourceMap.put(o.e, setSrc.get(o.i));
                }
            });

            setColumnTargetMappings.put(primaryIndex,
                Mappings.source(ImmutableList.copyOf(setColumnTargetMap.values()), fieldCount));
            setColumnSourceMappings.put(primaryIndex,
                Mappings.source(ImmutableList.copyOf(setColumnSourceMap.values()), fieldCount));
            setColumnMetas.put(primaryIndex,
                setColumnSourceMap.keySet().stream().map(primaryTableMeta::getColumn).collect(Collectors.toList()));

            relocateWriterMap.put(primaryIndex, new ArrayList<>());
            modifyWriterMap.put(primaryIndex, new ArrayList<>());

            if (CheckModifyLimitation.checkModifyShardingColumn(
                updateColumns,
                updateTables,
                (x, y) -> modifyPrimarySk.getAndSet(true)) || primaryTableModifyPk) {
                RelocateWriter w = WriterFactory
                    .createRelocateWriter(modify, primary, primaryIndex, updateColumns, mapping, primaryTableMeta,
                        false, primaryLogicalName, addedAutoUpdateColumns, plannerContext, ec, false);
                relocateWriterMap.get(primaryIndex).add(w);
                primaryRelocateWriter.put(primaryIndex, w);
            } else {
                final Pair<String, String> qn = RelUtils.getQualifiedTableName(primary);
                final OptimizerContext oc = OptimizerContext.getContext(qn.left);
                assert oc != null;
                if (!ec.getSchemaManager(schemaName).getTable(qn.right).isHasPrimaryKey()) {
                    // Create PkUpdateWriter on table without primary key will cause exception
                    modifyPrimaryWithoutPk.getAndSet(true);
                    primaryWithoutPk.add(qn.right);
                } else if (oc.getRuleManager().isBroadCast(qn.right) || oc.getRuleManager()
                    .isTableInSingleDb(qn.right)) {
                    DistinctWriter w = WriterFactory
                        .createBroadcastOrSingleUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping,
                            oc.getRuleManager().isBroadCast(qn.right),
                            oc.getRuleManager().isTableInSingleDb(qn.right), ec
                        );
                    modifyWriterMap.get(primaryIndex).add(w);
                    primaryDistinctWriter.put(primaryIndex, w);
                } else {
                    DistinctWriter w =
                        WriterFactory.createUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping, ec);
                    modifyWriterMap.get(primaryIndex).add(w);
                    primaryDistinctWriter.put(primaryIndex, w);
                }
            }
        });

        /*
         * Build writer for GSI
         */
        final AtomicBoolean modifyGsiSk = new AtomicBoolean(false);
        final AtomicBoolean modifyGsi = new AtomicBoolean(false);
        final AtomicBoolean withGsi = new AtomicBoolean(false);
        final AtomicBoolean allGsiPublished = new AtomicBoolean(true);
        final AtomicBoolean allCanPushDownInScaleOut = new AtomicBoolean(primaryTableCanPushDownInScaleOut);

        gsiUpdateColumnMappings.forEach((primaryIndex, mappings) -> {
            if (GeneralUtil.isEmpty(mappings)) {
                return;
            }
            withGsi.getAndSet(true);

            final List<TableMeta> gsiMetas = tableGsiMap.get(primaryIndex);
            final RelOptTable primary = srcInfos.get(primaryIndex).getRefTable();
            final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(primary);
            final String schemaName = Optional.ofNullable(qualifiedTableName.left)
                .orElse(plannerContext.getSchemaName());

            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName, ec);

            Ord.zip(mappings).forEach(o -> {
                final List<Integer> mapping = o.e;
                if (GeneralUtil.isEmpty(mapping)) {
                    // Do not modify this GSI
                    return;
                }
                modifyGsi.getAndSet(true);

                final TableMeta gsiMeta = gsiMetas.get(o.i);
                RelOptTable gsiTable = catalog
                    .getTableForMember(ImmutableList.of(schemaName, gsiMeta.getTableName()));
                if (gsiMeta.hasUnpublishedLogicalGeneratedColumn()) {
                    gsiTable = RelOptTableImpl.create(gsiTable.getRelOptSchema(), gsiMeta.getPhysicalRowType(
                        catalog.getTypeFactory()), gsiMeta, ImmutableList.of(schemaName, gsiMeta.getTableName()));
                }

                final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
                final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
                final List<RelOptTable> updateTables = new ArrayList<>();
                for (int i = 0; i < updateColumns.size(); i++) {
                    updateTables.add(gsiTable);
                }

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(gsiTable);
                final OptimizerContext oc = OptimizerContext.getContext(qn.left);

                final boolean tableAllGsiPublished = GlobalIndexMeta
                    .isAllGsiPublished(ImmutableList.of(gsiMeta), plannerContext);

                final boolean needRelocate = CheckModifyLimitation
                    .checkModifyShardingColumn(updateColumns, updateTables, (x, y) -> modifyGsiSk.getAndSet(true))
                    || (primaryTableModifyPk && (!tableAllGsiPublished || ComplexTaskPlanUtils.canWrite(gsiMeta)));
                allGsiPublished.set(allGsiPublished.get() && tableAllGsiPublished);
                allCanPushDownInScaleOut.set(allCanPushDownInScaleOut.get() && !ComplexTaskPlanUtils.canWrite(gsiMeta));

                final boolean forceRelocate = modifyPrimarySk.get() && GlobalIndexMeta.isBackFillStatus(ec, gsiMeta);

                // Currently do not allow create gsi on table without primary key
                if (needRelocate || forceRelocate) {
                    RelocateWriter w =
                        WriterFactory.createRelocateWriter(modify, gsiTable, primaryIndex, updateColumns, mapping,
                            gsiMeta, true, qualifiedTableName.right, addedAutoUpdateColumnMap.get(primaryIndex),
                            plannerContext, ec, forceRelocate);
                    relocateWriterMap.get(primaryIndex).add(w);
                } else {
                    DistinctWriter w = WriterFactory
                        .createUpdateGsiWriter(modify, gsiTable, primaryIndex, updateColumns, mapping, gsiMeta, ec);
                    modifyWriterMap.get(primaryIndex).add(w);
                }
            });
        });

        // Check whether update modifying sharding column
        if (!modifyPrimarySk.get() && !modifyGsiSk.get() && !(primaryTableModifyPk && !allGsiPublished.get())
            && !(primaryTableModifyPk && !allCanPushDownInScaleOut.get())) {
            return;
        }

        if (modifyPrimaryWithoutPk.get()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                String.join(",", primaryWithoutPk));
        }

        modify.accept(new SelectWithLockVisitor(true));

        Map<Integer, Boolean> modifySkOnlyMap = new HashMap<>();
        // Collect sharding columns
        primaryUpdateColumnMappings.forEach((primaryIndex, mapping) -> {
            final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

            for (RelocateWriter rw : relocateWriterMap.get(primaryIndex)) {
                shardingColumns.addAll(
                    rw.getIdentifierKeyMetas().stream().map(ColumnMeta::getName).collect(Collectors.toList()));
            }

            modifySkOnlyMap.put(primaryIndex, shardingColumns.containsAll(
                setColumnMetas.get(primaryIndex).stream().map(ColumnMeta::getName).collect(Collectors.toList())));
        });

        // Collect AUTO_INCREMENT columns in update list
        final List<Integer> autoIncColumns = new ArrayList<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTables.get(i));
            final TableMeta tableMeta =
                plannerContext.getExecutionContext().getSchemaManager(qn.left).getTable(qn.right);
            final String columnName = targetColumns.get(i);
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
            if (columnMeta.isAutoIncrement()) {
                autoIncColumns.add(i);
            }
        }

        LogicalRelocate logicalRelocate;
        if (modifyPrimarySk.get() && !modifyGsi.get() && primaryUpdateColumnMappings.size() == 1) {
            // Single target table without gsi
            logicalRelocate = LogicalRelocate.singleTargetWithoutGsi(modify, autoIncColumns,
                relocateWriterMap, modifyWriterMap, setColumnTargetMappings, setColumnSourceMappings, setColumnMetas,
                modifySkOnlyMap, primaryDistinctWriter, primaryRelocateWriter, modify.getOriginalSqlNode());
        } else {
            logicalRelocate = LogicalRelocate.create(modify, autoIncColumns, relocateWriterMap, modifyWriterMap,
                setColumnTargetMappings, setColumnSourceMappings, setColumnMetas, modifySkOnlyMap,
                primaryDistinctWriter, primaryRelocateWriter, modify.getOriginalSqlNode());
        }
        GeneratedColumnUtil.buildGeneratedColumnInfoForModify(logicalRelocate, ec);
        call.transformTo(logicalRelocate);
    }

}
