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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.SelectWithLockVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.WriterFactory;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
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
 * @author chenmo.cm
 */
public class OptimizeLogicalModifyRule extends RelOptRule {

    public static final OptimizeLogicalModifyRule INSTANCE = new OptimizeLogicalModifyRule(
        operand(LogicalModify.class, any()));

    public OptimizeLogicalModifyRule(RelOptRuleOperand operand) {
        super(operand, OptimizeLogicalModifyRule.class.getName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalModify modify = call.rel(0);
        return GeneralUtil.isEmpty(modify.getPrimaryModifyWriters()) && !modify.isWithoutPk();
    }

    /**
     * transfer LogicalModify to: [LogicalModify]
     *
     * @param call Rule call
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalModify modify = call.rel(0);
        ExecutionContext ec = PlannerContext.getPlannerContext(call).getExecutionContext();
        final List<TableModify.TableInfoNode> srcInfos = modify.getTableInfo().getSrcInfos();
        final Map<Integer, List<TableMeta>> tableGsiMap = modify.getTableInfo()
            .getTargetTableIndexSet()
            .stream()
            .map(i -> Pair.of(i, GlobalIndexMeta.getIndex(srcInfos.get(i).getRefTable(), ec)))
            .collect(Collectors.toMap(p -> p.left, p -> p.right));

        if (modify.isUpdate()) {
            optimizeUpdate(call, modify, tableGsiMap, ec);
        } else if (modify.isDelete()) {
            optimizeDelete(call, modify, tableGsiMap, ec);
        }
    }

    private void optimizeUpdate(RelOptRuleCall call, LogicalModify modify, Map<Integer, List<TableMeta>> tableGsiMap,
                                ExecutionContext ec) {
        final List<String> targetColumns = modify.getUpdateColumnList();
        final List<Integer> targetTableIndexes = modify.getTargetTableIndexes();
        final List<TableModify.TableInfoNode> srcInfos = modify.getTableInfo().getSrcInfos();
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(call);

        /*
         * Collect column mapping
         */
        final Map<Integer, List<Integer>> primaryUpdateColumnMappings = new HashMap<>();
        final Map<Integer, List<List<Integer>>> gsiUpdateColumnMappings = new HashMap<>();
        BuildPlanUtils.buildColumnMappings(targetColumns, targetTableIndexes, tableGsiMap, primaryUpdateColumnMappings,
            gsiUpdateColumnMappings);

        // Expressions in SET are at the end of row
        final RelDataType srcRowType = modify.getInput().getRowType();
        final int fieldCount = srcRowType.getFieldCount();
        final int offset = fieldCount - modify.getUpdateColumnList().size();
        final Map<Integer, Mapping> setColumnTargetMappings = new HashMap<>();
        final Map<Integer, Mapping> setColumnSourceMappings = new HashMap<>();
        final Map<Integer, List<ColumnMeta>> setColumnMetas = new HashMap<>();
        final Map<DistinctWriter, Integer> needCompareWriters = new HashMap<>();

        /*
         * Build writer for primary
         */
        final List<DistinctWriter> primaryModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyPrimaryWithoutPk = new AtomicBoolean(false);
        primaryUpdateColumnMappings.forEach((primaryIndex, mapping) -> {
            final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
            final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
            final RelOptTable primary = srcInfos.get(primaryIndex).getRefTable();

            final Pair<String, String> qn = RelUtils.getQualifiedTableName(primary);
            final OptimizerContext oc = OptimizerContext.getContext(qn.left);
            assert oc != null;
            if (!ec.getSchemaManager(qn.left).getTable(qn.right).isHasPrimaryKey()) {
                // Create PkUpdateWriter on table without primary key will cause exception
                modifyPrimaryWithoutPk.getAndSet(true);
            } else if (oc.getRuleManager().isBroadCast(qn.right) && GeneralUtil.isNotEmpty(
                (modify.getExtraTargetColumns()))) {
                // 广播表，包含默认添加的填充列，例如 ON UPDATE TIMESTAMP 列
                final String schemaName = RelUtils.getSchemaName(primary);
                final List<Integer> setSrc = mapping.stream().map(i -> offset + i).collect(Collectors.toList());
                final Map<String, Integer> columnIndexMap = modify.getSourceColumnIndexMap().get(primaryIndex);

                DistinctWriter distinctWriter = WriterFactory
                    .createBroadcastOrSingleUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping,
                        oc.getRuleManager().isBroadCast(qn.right), oc.getRuleManager().isTableInSingleDb(qn.right), ec);

                primaryModifyWriters.add(distinctWriter);

                final String primaryLogicalName = RelUtils.getQualifiedTableName(primary).right;
                final TableMeta primaryTableMeta =
                    plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(primaryLogicalName);

                Set<String> autoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                autoUpdateColumns.addAll(
                    primaryTableMeta.getAutoUpdateColumns().stream().map(ColumnMeta::getName).collect(
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

                //如果包含自动修改列，如ON UPDATE TIMESTAMP列，记录列位置信息，用于后续比较，相同值不执行，以防ON UPDATE TIMESTAMP列下推修改
                if (!addedAutoUpdateColumns.isEmpty()) {
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
                        setColumnSourceMap.keySet().stream().map(primaryTableMeta::getColumn)
                            .collect(Collectors.toList()));
                    needCompareWriters.put(distinctWriter, primaryIndex);
                }

            } else if (oc.getRuleManager().isBroadCast(qn.right) || oc.getRuleManager().isTableInSingleDb(qn.right)) {
                primaryModifyWriters.add(WriterFactory
                    .createBroadcastOrSingleUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping,
                        oc.getRuleManager().isBroadCast(qn.right), oc.getRuleManager().isTableInSingleDb(qn.right),
                        ec));
            } else {
                primaryModifyWriters.add(
                    WriterFactory.createUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping, ec));
            }
        });

        /*
         * Build writer for GSI
         */
        final List<DistinctWriter> gsiModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyGsi = new AtomicBoolean(false);
        final AtomicBoolean withGsi = new AtomicBoolean(false);
        gsiUpdateColumnMappings.forEach((primaryIndex, mappings) -> {
            if (GeneralUtil.isEmpty(mappings)) {
                return;
            }
            withGsi.getAndSet(true);

            final List<TableMeta> gsiMetas = tableGsiMap.get(primaryIndex);
            final RelOptTable primary = srcInfos.get(primaryIndex).getRefTable();
            final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(primary);
            final String schemaName = Optional.ofNullable(qualifiedTableName.left)
                .orElse(PlannerContext.getPlannerContext(call).getSchemaName());

            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName, ec);

            Ord.zip(mappings).forEach(o -> {
                final List<Integer> mapping = o.e;
                if (GeneralUtil.isEmpty(mapping)) {
                    // Do not modify this GSI
                    return;
                }
                modifyGsi.getAndSet(true);

                final TableMeta gsiMeta = gsiMetas.get(o.i);
                final RelOptTable gsiTable = catalog
                    .getTableForMember(ImmutableList.of(schemaName, gsiMeta.getTableName()));

                final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
                final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(gsiTable);
                final OptimizerContext oc = OptimizerContext.getContext(qn.left);

                // Currently do not allow create gsi on table without primary key
                gsiModifyWriters.add(
                    WriterFactory.createUpdateGsiWriter(
                        modify,
                        gsiTable,
                        primaryIndex,
                        updateColumns,
                        mapping,
                        gsiMeta, ec));
            });
        });

        final LogicalModify newModify = (LogicalModify) modify.accept(new SelectWithLockVisitor(true));
        newModify.setPrimaryModifyWriters(primaryModifyWriters);
        newModify.setGsiModifyWriters(gsiModifyWriters);
        newModify.setWithoutPk(modifyPrimaryWithoutPk.get());
        GeneratedColumnUtil.buildGeneratedColumnInfoForModify(newModify, ec);
        newModify.setNeedCompareWriters(needCompareWriters);
        newModify.setSetColumnSourceMappings(setColumnSourceMappings);
        newModify.setSetColumnTargetMappings(setColumnTargetMappings);
        newModify.setSetColumnMetas(setColumnMetas);

        call.transformTo(newModify);
    }

    private void optimizeDelete(RelOptRuleCall call, LogicalModify modify, Map<Integer, List<TableMeta>> tableGsiMap,
                                ExecutionContext ec) {
        final List<Integer> targetTableIndexes = modify.getTargetTableIndexes();
        final List<TableModify.TableInfoNode> srcInfos = modify.getTableInfo().getSrcInfos();

        /*
         * Build writer for primary
         */
        final List<DistinctWriter> primaryModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyPrimaryWithoutPk = new AtomicBoolean(false);
        for (Integer targetIndex : targetTableIndexes) {
            final RelOptTable targetTable = srcInfos.get(targetIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
            final OptimizerContext oc = OptimizerContext.getContext(qn.left);
            assert oc != null;
            if (!ec.getSchemaManager(qn.left).getTable(qn.right).isHasPrimaryKey()) {
                // Create PkUpdateWriter on table without primary key will cause exception
                modifyPrimaryWithoutPk.getAndSet(true);
            } else if (oc.getRuleManager().isBroadCast(qn.right)) {
                primaryModifyWriters
                    .add(WriterFactory.createBroadcastDeleteWriter(modify, targetTable, targetIndex, ec));
            } else if (oc.getRuleManager().isTableInSingleDb(qn.right)) {
                primaryModifyWriters.add(WriterFactory.createSingleDeleteWriter(modify, targetTable, targetIndex, ec));
            } else {
                primaryModifyWriters.add(WriterFactory.createDeleteWriter(modify, targetTable, targetIndex, ec));
            }
        }

        /*
         * Build writer for GSI
         */
        final List<DistinctWriter> gsiModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyGsi = new AtomicBoolean(false);
        final AtomicBoolean withGsi = new AtomicBoolean(false);
        for (Integer targetIndex : targetTableIndexes) {
            if (GeneralUtil.isEmpty(tableGsiMap.get(targetIndex))) {
                continue;
            }
            withGsi.getAndSet(true);

            final List<TableMeta> gsiMetas = tableGsiMap.get(targetIndex);
            final RelOptTable primary = srcInfos.get(targetIndex).getRefTable();
            final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(primary);
            final String schemaName = Optional.ofNullable(qualifiedTableName.left)
                .orElse(PlannerContext.getPlannerContext(call).getSchemaName());

            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName, ec);

            gsiMetas.forEach(gsiMeta -> {
                modifyGsi.getAndSet(true);

                final RelOptTable gsiTable = catalog
                    .getTableForMember(ImmutableList.of(schemaName, gsiMeta.getTableName()));

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(gsiTable);
                final OptimizerContext oc = OptimizerContext.getContext(qn.left);

                // Currently do not allow create gsi on table without primary key
                gsiModifyWriters.add(WriterFactory.createDeleteGsiWriter(modify, gsiTable, targetIndex, gsiMeta, ec));
            });
        }

        final LogicalModify newModify = (LogicalModify) modify.accept(new SelectWithLockVisitor(true));
        newModify.setPrimaryModifyWriters(primaryModifyWriters);
        newModify.setGsiModifyWriters(gsiModifyWriters);
        newModify.setWithoutPk(modifyPrimaryWithoutPk.get());

        call.transformTo(newModify);
    }
}
