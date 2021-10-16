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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
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
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        final boolean modifyPk =
            CheckModifyLimitation.checkModifyPk(modify, ec) && !CheckModifyLimitation
                .isAllTablesCouldPushDown(modify, ec);
        if (notPrimarySk && notModifyGsi && !modifyPk) {
            // Do not modify sharding key
            return;
        }

        /*
         * Build writer for primary
         */
        final List<RelocateWriter> primaryRelocateWriters = new ArrayList<>();
        final List<DistinctWriter> primaryModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyPrimarySk = new AtomicBoolean(false);
        final AtomicBoolean modifyPrimaryWithoutPk = new AtomicBoolean(false);
        final List<String> primaryWithoutPk = new ArrayList<>();
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(call);

        primaryUpdateColumnMappings.forEach((primaryIndex, mapping) -> {
            final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
            final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
            final List<RelOptTable> updateTables = Mappings.permute(targetTables, updateColumnMapping);
            final RelOptTable primary = srcInfos.get(primaryIndex).getRefTable();
            final String schemaName = RelUtils.getSchemaName(primary);

            if (CheckModifyLimitation.checkModifyShardingColumn(
                updateColumns,
                updateTables,
                (x, y) -> modifyPrimarySk.getAndSet(true)) || modifyPk) {

                final String primaryLogicalName = RelUtils.getQualifiedTableName(primary).right;
                TableMeta primaryTableMeta =
                    plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(primaryLogicalName);
                primaryRelocateWriters.add(WriterFactory
                    .createRelocateWriter(modify, primary, primaryIndex, updateColumns, mapping, primaryTableMeta,
                        false, plannerContext, ec));
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
                    primaryModifyWriters.add(
                        WriterFactory
                            .createBroadcastOrSingleUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping,
                                oc.getRuleManager().isBroadCast(qn.right),
                                oc.getRuleManager().isTableInSingleDb(qn.right), ec
                            ));
                } else {
                    primaryModifyWriters
                        .add(WriterFactory
                            .createUpdateWriter(modify, primary, primaryIndex, updateColumns, mapping, ec));
                }
            }
        });

        /*
         * Build writer for GSI
         */
        final List<RelocateWriter> gsiRelocateWriters = new ArrayList<>();
        final List<DistinctWriter> gsiModifyWriters = new ArrayList<>();
        final AtomicBoolean modifyGsiSk = new AtomicBoolean(false);
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
                final RelOptTable gsiTable = catalog
                    .getTableForMember(ImmutableList.of(schemaName, gsiMeta.getTableName()));

                final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
                final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
                final List<RelOptTable> updateTables = IntStream.range(0, updateColumns.size())
                    .mapToObj(i -> gsiTable)
                    .collect(Collectors.toList());

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(gsiTable);
                final OptimizerContext oc = OptimizerContext.getContext(qn.left);

                final boolean allGsiPublished = GlobalIndexMeta
                    .isAllGsiPublished(ImmutableList.of(gsiMeta), plannerContext);

                final boolean needRelocate = CheckModifyLimitation
                    .checkModifyShardingColumn(updateColumns, updateTables, (x, y) -> modifyGsiSk.getAndSet(true)) || (
                    modifyPk && (!allGsiPublished || ComplexTaskPlanUtils.canWrite(gsiMeta)));

                // Currently do not allow create gsi on table without primary key
                if (needRelocate) {
                    gsiRelocateWriters.add(WriterFactory
                        .createRelocateWriter(modify, gsiTable, primaryIndex, updateColumns, mapping, gsiMeta, true,
                            plannerContext, ec));
                } else {
                    gsiModifyWriters.add(WriterFactory
                        .createUpdateGsiWriter(modify, gsiTable, primaryIndex, updateColumns, mapping, gsiMeta, ec));
                }
            });
        });

        // Check whether update modifying sharding column
        if (!modifyPrimarySk.get() && !modifyGsiSk.get() && !modifyPk) {
            return;
        }

        if (modifyPrimaryWithoutPk.get()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                String.join(",", primaryWithoutPk));
        }

        modify.accept(new SelectWithLockVisitor(true));

        if (modifyPrimarySk.get() && !modifyGsi.get() && primaryUpdateColumnMappings.size() == 1) {
            // Single target table without gsi
            call.transformTo(LogicalRelocate.singleTargetWithoutGsi(modify, primaryRelocateWriters));
            return;
        }

        call.transformTo(LogicalRelocate
            .create(modify, primaryRelocateWriters, primaryModifyWriters, gsiRelocateWriters, gsiModifyWriters));
    }

}
