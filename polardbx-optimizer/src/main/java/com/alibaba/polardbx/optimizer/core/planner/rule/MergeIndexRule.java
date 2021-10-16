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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class MergeIndexRule extends RelOptRule {

    private String schemaName;

    private List<String> gsiNameList;

    private SqlSelect.LockMode lockMode;

    private boolean work = false;

    private Map<String, RexNode> indexScanDigestToPredicate = new HashMap<>();

    public MergeIndexRule(String schemaName, List<String> gsiNameList, SqlSelect.LockMode lockMode) {
        super(operand(LogicalFilter.class, operand(LogicalTableScan.class, RelOptRule.none())),
            "MergeIndexRule");
        this.schemaName = schemaName;
        this.gsiNameList = gsiNameList;
        this.lockMode = lockMode;
    }

    public boolean work() {
        return work;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter logicalFilter = call.rel(0);
        LogicalTableScan logicalTableScan = call.rel(1);
        RexNode condition = logicalFilter.getCondition();
        if (condition == null || condition.isAlwaysTrue() || condition.isAlwaysFalse()) {
            return;
        }

        if (RexUtil.containsCorrelation(logicalFilter.getCondition())
            || (null != logicalFilter.getVariablesSet() && logicalFilter.getVariablesSet().size() > 0)) {
            return;
        }

        if (FilterTableLookupTransposeRule.checkScalarExists(logicalFilter)) {
            return;
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        List<RelNode> paths = generateOrPath(conjunctions, logicalTableScan);
        RelNode mergeIndexScan = chooseAndPathSubSet(paths);
        if (mergeIndexScan == null) {
            return;
        }

        RelMetadataQuery mq = logicalFilter.getCluster().getMetadataQuery();
        Set<RexTableInputRef.RelTableRef> tableRefs = mq.getTableReferences(mergeIndexScan);
        if (tableRefs == null || tableRefs.isEmpty()) {
            return;
        }

        RelOptTable indexTable = tableRefs.iterator().next().getTable();

        // lookup primary table
        LogicalTableLookup logicalTableLookup = RelUtils.createTableLookup(LogicalView.create(logicalTableScan,
            logicalTableScan.getTable()),
            mergeIndexScan, indexTable);

        LogicalFilter newFilter = logicalFilter.copy(logicalFilter.getTraitSet(), logicalTableLookup, condition);
        work = true;
        call.transformTo(newFilter);
    }

    private List<RelNode> generateOrPath(List<RexNode> conjunctions, LogicalTableScan logicalTableScan) {
        List<RelNode> result = new ArrayList<>();
        for (RexNode predicate : conjunctions) {
            List<RelNode> mergeIndexList = new ArrayList<>();
            if (predicate instanceof RexCall && ((RexCall) predicate).getOperator() == SqlStdOperatorTable.OR) {
                List<RexNode> disjunction = RelOptUtil.disjunctions(predicate);
                // clustering the disjunction by operand columnName
                // k2 = 10 or k1 = 1 or k1 = 2 -> {k2 = 10}, {k1 = 1 or k1 = 2}

                List<Pair<String, RexNode>> clustering = clusteringDisjunction(disjunction, logicalTableScan);

                for (Pair<String, RexNode> pair : clustering) {
                    String columnName = pair.getKey();
                    RexNode operand = pair.getValue();

                    List<RelNode> indexPaths = null;
                    if (operand instanceof RexCall && ((RexCall) operand).getOperator() == SqlStdOperatorTable.AND) {
                        final List<RexNode> conj = RelOptUtil.conjunctions(operand);
                        indexPaths = buildPathForOr(columnName, conj, logicalTableScan);
                        indexPaths.addAll(generateOrPath(conj, logicalTableScan));
                    } else if (columnName != null) {
                        List<RexNode> conj = new ArrayList<>();
                        conj.add(operand);
                        indexPaths = buildPathForOr(columnName, conj, logicalTableScan);
                    }

                    // If nothing matched one of or operand, we can't do anything with this OR
                    if (indexPaths == null || indexPaths.isEmpty()) {
                        mergeIndexList.clear();
                        break;
                    }

                    RelNode path = chooseAndPathSubSet(indexPaths);
                    mergeIndexList.add(path);
                }
            }

            if (!mergeIndexList.isEmpty()) {
                RelNode orPath = createOrPath(mergeIndexList, logicalTableScan);
                result.add(orPath);
            }
        }
        return result;
    }

    private List<Pair<String, RexNode>> clusteringDisjunction(List<RexNode> disjunction,
                                                              LogicalTableScan logicalTableScan) {
        Map<String, RexNode> map = new HashMap<>();
        List<Pair<String, RexNode>> result = new ArrayList<>();

        RelOptTable primaryTable = logicalTableScan.getTable();
        TableMeta primaryTableMeta = CBOUtil.getTableMeta(primaryTable);
        RexBuilder rexBuilder = logicalTableScan.getCluster().getRexBuilder();

        for (RexNode predicate : disjunction) {
            String columnName = null;

            if ((predicate.isA(SqlKind.EQUALS)
                || predicate.isA(SqlKind.GREATER_THAN_OR_EQUAL)
                || predicate.isA(SqlKind.LESS_THAN)
                || predicate.isA(SqlKind.GREATER_THAN)
                || predicate.isA(SqlKind.LESS_THAN_OR_EQUAL)) && predicate instanceof RexCall) {
                RexCall rexCall = (RexCall) predicate;
                RexNode operand1 = rexCall.getOperands().get(0);
                RexNode operand2 = rexCall.getOperands().get(1);
                int idx = -1;
                if (operand1 instanceof RexInputRef && !(operand2 instanceof RexInputRef)) {
                    idx = ((RexInputRef) operand1).getIndex();
                } else if (operand2 instanceof RexInputRef && !(operand1 instanceof RexInputRef)) {
                    idx = ((RexInputRef) operand2).getIndex();
                }
                if (idx != -1) {
                    ColumnMeta columnMeta = primaryTableMeta.getAllColumns().get(idx);
                    columnName = columnMeta.getName();
                }
            } else if (predicate.isA(SqlKind.IN) && predicate instanceof RexCall) {
                RexCall rexCall = (RexCall) predicate;
                RexNode operand1 = rexCall.getOperands().get(0);
                RexNode operand2 = rexCall.getOperands().get(1);
                if (operand1 instanceof RexInputRef && operand2 instanceof RexCall && operand2.isA(SqlKind.ROW)) {
                    final int idx = ((RexInputRef) operand1).getIndex();
                    ColumnMeta columnMeta = primaryTableMeta.getAllColumns().get(idx);
                    columnName = columnMeta.getName();
                }
            } else if ((predicate.isA(SqlKind.BETWEEN) || predicate.isA(SqlKind.IS_NULL))
                && predicate instanceof RexCall) {
                RexCall rexCall = (RexCall) predicate;
                RexNode operand1 = rexCall.getOperands().get(0);
                if (operand1 instanceof RexInputRef) {
                    final int idx = ((RexInputRef) operand1).getIndex();
                    ColumnMeta columnMeta = primaryTableMeta.getAllColumns().get(idx);
                    columnName = columnMeta.getName();
                }
            }

            if (columnName == null) {
                result.add(Pair.of(null, predicate));
            } else {
                RexNode rexNode = map.get(columnName);
                if (rexNode == null) {
                    map.put(columnName, predicate);
                } else {
                    RelOptUtil.disjunctions(predicate);
                    map.put(columnName, rexBuilder.makeCall(SqlStdOperatorTable.OR, predicate, rexNode));
                }
            }
        }

        for (Map.Entry<String, RexNode> entry : map.entrySet()) {
            result.add(Pair.of(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * build_paths_for_OR
     * Given a list of restriction clauses from one arm of an OR clause,
     * construct all matching IndexPaths for the relation.
     **/
    private List<RelNode> buildPathForOr(String columnName, List<RexNode> conj, LogicalTableScan logicalTableScan) {
        RelOptTable primaryTable = logicalTableScan.getTable();
        List<RelNode> result = new ArrayList<>();

        if (columnName != null) {
            assert conj.size() == 1;
        }
        final RexBuilder rexBuilder = logicalTableScan.getCluster().getRexBuilder();
        int MAX_COMBINATION_SIZE = 3;
        int combinationSize = Math.min(conj.size(), MAX_COMBINATION_SIZE);

        Set<Set<RexNode>> combinationSet = new LinkedHashSet<>();

        int PRUNING_SIZE = 8;
        List<RexNode> pruningConj = new ArrayList<>();
        if (conj.size() > PRUNING_SIZE) {
            for (int i = 0; i < PRUNING_SIZE; i++) {
                pruningConj.add(conj.get(i));
            }
        } else {
            pruningConj = conj;
        }

        // add the combination larger first
        for (int i = combinationSize; i >= 1; i--) {
            Set<RexNode> set = pruningConj.stream().collect(Collectors.toSet());
            if (set.size() >= i) {
                combinationSet.addAll(Sets.combinations(set, i));
            }
        }
        for (String gsiName : gsiNameList) {
            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName,
                PlannerContext.getPlannerContext(logicalTableScan).getExecutionContext());
            RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, gsiName));
            TableMeta indexTableMeta = CBOUtil.getTableMeta(indexTable);
            List<IndexMeta> indexMetaList = indexTableMeta.getSecondaryIndexes();

            boolean useIndex = false;
            if (columnName != null) {
                for (IndexMeta indexMeta : indexMetaList) {
                    if (columnName.equalsIgnoreCase(indexMeta.getKeyColumns().get(0).getName())) {
                        useIndex = true;
                    }
                }
            } else {
                useIndex = true;
            }

            if (useIndex) {
                for (Set<RexNode> set : combinationSet) {
                    final RexNode predicate;
                    if (set.size() == 1) {
                        predicate = set.iterator().next();
                    } else {
                        predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                            set.stream().collect(Collectors.toList()));
                    }
                    RelNode indexAccess = buildIndexAccess(indexTable, primaryTable, predicate, logicalTableScan);
                    if (indexAccess != null) {
                        result.add(indexAccess);
                    }
                }
            }
        }
        return result;
    }

    private RelNode buildIndexAccess(RelOptTable index, RelOptTable primary, RexNode predicate,
                                     LogicalTableScan logicalTableScan) {
        TableMeta primaryTableMeta = CBOUtil.getTableMeta(primary);

        final RelDataType primaryRowType = primary.getRowType();
        final RelDataType indexRowType = index.getRowType();

        final String primaryTableName = primaryTableMeta.getTableName();
        final TableMeta primaryTable =
            PlannerContext.getPlannerContext(logicalTableScan).getExecutionContext().getSchemaManager(schemaName)
                .getTable(primaryTableName);

        final List<String> pkList = Optional.ofNullable(primaryTable.getPrimaryKey())
            .map(cmList -> cmList.stream().map(ColumnMeta::getName).collect(Collectors.toList()))
            .orElse(ImmutableList.of());

        final List<String> skList = OptimizerContext.getContext(schemaName)
            .getRuleManager()
            .getSharedColumns(primaryTableName)
            .stream()
            .filter(sk -> !pkList.contains(sk))
            .collect(Collectors.toList());

        final Map<String, Integer> indexColumnRefMap = indexRowType.getFieldList()
            .stream()
            .collect(Collectors.toMap(RelDataTypeField::getName,
                RelDataTypeField::getIndex,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));

        // build index scan
        final LogicalIndexScan indexScan = new LogicalIndexScan(index,
            LogicalTableScan.create(logicalTableScan.getCluster(), index, logicalTableScan.getHints()), lockMode);

        // filter, the predicate must be cover by indexScan
        RelOptUtil.InputReferencedVisitor inputReferencedVisitor = new RelOptUtil.InputReferencedVisitor();
        predicate.accept(inputReferencedVisitor);
        boolean cover = inputReferencedVisitor.inputPosReferenced.stream()
            .map(i -> primaryRowType.getFieldList().get(i).getName())
            .allMatch(col -> indexColumnRefMap.get(col) != null);

        if (!cover) {
            return null;
        }

        Map<Integer, Integer> map = new HashMap<>();

        for (Integer i : inputReferencedVisitor.inputPosReferenced) {
            map.put(i, indexColumnRefMap.get(primaryRowType.getFieldList().get(i).getName()));
        }

        Mappings.TargetMapping mapping = Mappings.target(
            map, primaryRowType.getFieldCount(), indexRowType.getFieldCount());

        RexNode newPredicate = predicate.accept(new RexPermuteInputsShuttle(mapping, indexScan));

        LogicalFilter logicalFilter = LogicalFilter.create(indexScan, newPredicate);

        final RexBuilder rexBuilder = logicalTableScan.getCluster().getRexBuilder();
        List<String> fieldNames = new ArrayList<>();
        List<RexNode> projects = new ArrayList<>();
        for (String pk : pkList) {
            projects.add(rexBuilder.makeInputRef(logicalFilter, indexColumnRefMap.get(pk)));
            fieldNames.add(pk);
        }
        for (String sk : skList) {
            projects.add(rexBuilder.makeInputRef(logicalFilter, indexColumnRefMap.get(sk)));
            fieldNames.add(sk);
        }
        // project [pk, sk]
        LogicalProject logicalProject = LogicalProject.create(logicalFilter, projects, fieldNames);
        RelNode result = optimizeByPushDownRule(logicalProject, PlannerContext.getPlannerContext(logicalProject));
        indexScanDigestToPredicate.put(result.getDigest(), predicate);
        return result;
    }

    private RelNode optimizeByPushDownRule(RelNode input, PlannerContext plannerContext) {
        // input should contain [Project] + [Filter] + [TableLookup]
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        // push filter
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        // push project
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.stopOptimizerTrace();
        hepPlanner.setRoot(input);
        RelNode output = hepPlanner.findBestExp();
        return output;
    }

    // Given a nonempty list of paths, AND them into one path.
    private RelNode chooseAndPathSubSet(List<RelNode> paths) {
        if (paths == null || paths.isEmpty()) {
            return null;
        } else {
            // TODO: Improve me. For now we choose the min-cost, actually we can use the subSet.
            RelMetadataQuery mq = paths.get(0).getCluster().getMetadataQuery();
            RelOptCost minCost = paths.get(0).getCluster().getPlanner().getCostFactory().makeInfiniteCost();
            RelNode result = null;
            for (RelNode path : paths) {
                RelOptCost cost = mq.getCumulativeCost(path);
                if (cost.isLt(minCost)) {
                    minCost = cost;
                    result = path;
                }
            }
            return result;
        }
    }

    private RelNode createOrPath(List<RelNode> paths, LogicalTableScan logicalTableScan) {
        // To discover the same gsi merge them by or their predicates together is more efficient than union
        List<RelNode> unionInputs = new ArrayList<>();
        Map<String, RexNode> m = new LinkedHashMap<>();
        RexBuilder rexBuilder = logicalTableScan.getCluster().getRexBuilder();
        for (RelNode path : paths) {
            if (path instanceof LogicalIndexScan) {
                String gsiName = CBOUtil.getTableMeta(path.getTable()).getTableName();
                RexNode predicate = indexScanDigestToPredicate.get(path.getDigest());
                RexNode orPredicate = m.get(gsiName);
                if (orPredicate == null) {
                    orPredicate = predicate;
                } else {
                    orPredicate = rexBuilder.makeCall(SqlStdOperatorTable.OR, predicate, orPredicate);
                }
                m.put(gsiName, orPredicate);
            } else {
                unionInputs.add(path);
            }
        }
        for (Map.Entry<String, RexNode> entry : m.entrySet()) {
            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName,
                PlannerContext.getPlannerContext(logicalTableScan).getExecutionContext());
            RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, entry.getKey()));
            RelNode newPath = buildIndexAccess(indexTable, logicalTableScan.getTable(), entry.getValue(),
                logicalTableScan);
            unionInputs.add(newPath);
        }
        LogicalUnion logicalUnion = LogicalUnion.create(unionInputs, false);
        return logicalUnion;
    }
}
