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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Shi Yuxuan
 */
public class OSSMergeIndexRule extends RelOptRule {

    public static final OSSMergeIndexRule INSTANCE = new OSSMergeIndexRule(
        operand(OSSTableScan.class, null, none()), "OSSMergeIndexRule");

    public OSSMergeIndexRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_INDEX_SELECTION);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OSSTableScan ossTableScan = call.rel(0);
        if (!ossTableScan.canPushFilterProject()) {
            return;
        }
        if (ossTableScan.withAgg()) {
            return;
        }
        RelNode plan = ossTableScan.getPushedRelNode();

        plan = CBOUtil.OssTableScanFormat(plan);

        LogicalProject topProject = null;
        LogicalFilter filter = null;
        LogicalProject bottomProject = null;
        LogicalTableScan tableScan = null;

        if (plan instanceof LogicalProject) {
            topProject = (LogicalProject) plan;
            plan = topProject.getInput(0);
        }
        if (plan instanceof LogicalFilter) {
            filter = (LogicalFilter) plan;
            plan = filter.getInput(0);
        }
        if (plan instanceof LogicalProject) {
            bottomProject = (LogicalProject) plan;
            plan = bottomProject.getInput(0);
        }
        if (plan instanceof LogicalTableScan) {
            tableScan = (LogicalTableScan) plan;
        }
        if (!complexFilter(filter)) {
            return;
        }
        RelBuilder relBuilder = call.builder();

        TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        Map<ColumnMeta, Integer> allColumns = new HashMap<>();
        for (int i = 0; i < tableMeta.getPhysicalColumns().size(); i++) {
            allColumns.put(tableMeta.getPhysicalColumns().get(i), i);
        }

        // make sure baseRelNode project includes primary key
        RelNode baseRelNode = buildBaseNode(bottomProject, tableScan, tableMeta, allColumns, relBuilder);

        /**
         * get all columns with bloom filter
         */
        GenerateContext context = getIndexColumns(baseRelNode, tableScan.getTable(),
            tableMeta, allColumns, relBuilder);

        if (context == null) {
            return;
        }

        RelNode root = generateAnd(filter.getCondition(), context);
        if (root == null) {
            return;
        }
        if (topProject != null) {
            relBuilder.push(root);
            relBuilder.project(topProject.getProjects(), topProject.getRowType().getFieldNames());
            root = relBuilder.build();
        } else {
            // remove useless primary key
            if (bottomProject != null &&
                bottomProject.getRowType().getFieldCount() < root.getRowType().getFieldCount()) {
                List<RexNode> projects = new ArrayList<>();
                List<String> names = new ArrayList<>();

                relBuilder.push(root);
                for (int i = 0; i < bottomProject.getRowType().getFieldCount(); i++) {
                    projects.add(new RexInputRef(i, root.getRowType().getFieldList().get(i).getType()));
                    names.add(root.getRowType().getFieldNames().get(i));
                }
                relBuilder.project(projects, names);
                root = relBuilder.build();
            }
        }

        //debugUsage(root, ossTableScan);
        if (root.getCluster().getMetadataQuery().getCumulativeCost(root).isLt(
            ossTableScan.getCluster().getMetadataQuery().getCumulativeCost(ossTableScan))) {
            call.transformTo(root);
        }

    }

    private RelNode generateAnd(RexNode condition, GenerateContext context) {
        List<Pair<Integer, RelNode>> result = new ArrayList<>();
        List<RexNode> conditions = RelOptUtil.conjunctions(condition);
        RelBuilder relBuilder = context.getRelBuilder();
        // prepare init conditions
        int startLoc = context.getAndConditions().size();
        for (int i = 1; i < conditions.size(); i++) {
            context.addAndCondition(conditions.get(i));
        }

        for (int i = 0; i < conditions.size(); i++) {
            RexNode andCondition = conditions.get(i);
            List<RexNode> orConditions = RelOptUtil.disjunctions(andCondition);
            Map<Integer, List<RexNode>> clustering =
                clusteringDisjunction(orConditions, context);
            if (clustering != null) {
                // prepare a union tree for the 'and clause'
                List<RelNode> indexPaths = new ArrayList<>();
                for (Map.Entry<Integer, List<RexNode>> pair : clustering.entrySet()) {
                    Integer columnRef = pair.getKey();
                    List<RexNode> operands = pair.getValue();
                    // complex 'or' list
                    if (columnRef == -1) {
                        RelNode node = null;
                        switch (operands.size()) {
                        case 0:
                            // no complex condition
                            break;
                        case 1:
                            // one complex 'and' condition
                            if (operands.get(0).isA(SqlKind.AND)) {
                                node = generateAnd(operands.get(0), context);
                            } else {
                                // one complex 'or' condition
                                if (operands.get(0).isA(SqlKind.OR)) {
                                    node = generateOr(operands, context);
                                }
                            }
                            break;
                        default:
                            // many complex conditions
                            node = generateOr(operands, context);
                        }
                        if (node == null) {
                            indexPaths = null;
                            break;
                        }
                        indexPaths.add(node);
                    } else {
                        // get real filters
                        relBuilder.push(context.getCopiedBaseRelNode());
                        if (operands.size() > 1) {
                            RexBuilder rexBuilder = context.getBaseRelNode().getCluster().getRexBuilder();
                            context.addAndCondition(rexBuilder.makeCall(SqlStdOperatorTable.OR, operands));
                        } else {
                            context.addAndCondition(operands.get(0));
                        }
                        relBuilder.filter(context.getAndConditions());
                        RelNode filterNode = relBuilder.build();
                        context.removeAndCondition();
                        OSSTableScan node = new OSSTableScan(filterNode, context.getTable(), null, true,
                            new OSSTableScan.OSSIndexContext(context.isOrder(columnRef)));
                        node.rebuildPartRoutingPlanInfo();
                        indexPaths.add(node);
                    }

                }
                if (indexPaths != null) {
                    //union paths
                    if (indexPaths.size() > 1) {
                        result.add(new Pair<>(i, LogicalUnion.create(indexPaths, false)));
                    } else {
                        result.add(new Pair<>(i, indexPaths.get(0)));
                    }
                }
            }
            if (i < conditions.size() - 1) {
                context.setAndCondition(startLoc + i, conditions.get(i));
            }
        }

        //remove the and conditions
        for (int i = 1; i < conditions.size(); i++) {
            context.removeAndCondition();
        }
        if (result.isEmpty()) {
            return null;
        } else {
            int minLoc = -1;
            RelMetadataQuery mq = result.get(0).getValue().getCluster().getMetadataQuery();
            RelOptCost minCost = result.get(0).getValue().getCluster().getPlanner().
                getCostFactory().makeInfiniteCost();
            for (int i = 0; i < result.size(); i++) {
                RelNode path = result.get(i).getValue();
                RelOptCost cost = mq.getCumulativeCost(path);
                if (cost.isLt(minCost)) {
                    minCost = cost;
                    minLoc = i;
                }
            }
            return result.get(minLoc).getValue();
        }
    }

    private RelNode generateOr(List<RexNode> conditions, GenerateContext context) {
        List<RelNode> indexPaths = new ArrayList<>();
        for (RexNode orCondition : conditions) {
            RelNode node = generateAnd(orCondition, context);
            if (node == null) {
                indexPaths = null;
                break;
            }
            indexPaths.add(node);
        }
        if (indexPaths != null) {
            //union paths
            return LogicalUnion.create(indexPaths, false);
        }
        return null;
    }

    /**
     * classify or clause to two type{column index, simple rexNode compare the given index} {-1, complex rexNode}
     *
     * @return null if there is a simple rexNode that can't be indexed, otherwise the map
     */
    static public Map<Integer, List<RexNode>> clusteringDisjunction(List<RexNode> disjunction,
                                                                    GenerateContext context) {
        Map<Integer, List<RexNode>> result = new HashMap<>();
        for (RexNode predicate : disjunction) {
            int idx = -1;
            if ((predicate.isA(SqlKind.EQUALS)
                || predicate.isA(SqlKind.GREATER_THAN_OR_EQUAL)
                || predicate.isA(SqlKind.LESS_THAN)
                || predicate.isA(SqlKind.GREATER_THAN)
                || predicate.isA(SqlKind.LESS_THAN_OR_EQUAL)) && predicate instanceof RexCall) {
                RexCall rexCall = (RexCall) predicate;
                RexNode operand1 = rexCall.getOperands().get(0);
                RexNode operand2 = rexCall.getOperands().get(1);

                if (operand1 instanceof RexInputRef && !(operand2 instanceof RexInputRef)) {
                    idx = ((RexInputRef) operand1).getIndex();
                } else if (operand2 instanceof RexInputRef && !(operand1 instanceof RexInputRef)) {
                    idx = ((RexInputRef) operand2).getIndex();
                }
                if (idx != -1) {
                    if (predicate.isA(SqlKind.EQUALS)) {
                        if (!context.isFilter(idx)) {
                            return null;
                        }
                    } else {
                        if (!context.isOrder(idx)) {
                            return null;
                        }
                    }
                }
            } else if (predicate.isA(SqlKind.IN) && predicate instanceof RexCall) {
                RexCall rexCall = (RexCall) predicate;
                RexNode operand1 = rexCall.getOperands().get(0);
                RexNode operand2 = rexCall.getOperands().get(1);
                if (operand1 instanceof RexInputRef && operand2 instanceof RexCall && operand2.isA(SqlKind.ROW)) {
                    idx = ((RexInputRef) operand1).getIndex();
                    if (!context.isFilter(idx)) {
                        return null;
                    }
                }
            } else if (predicate.isA(SqlKind.BETWEEN) && predicate instanceof RexCall) {
                RexNode operand1 = ((RexCall) predicate).getOperands().get(0);
                if (operand1 instanceof RexInputRef) {
                    idx = ((RexInputRef) operand1).getIndex();
                    if (!context.isOrder(idx)) {
                        return null;
                    }
                }
            } else if (predicate.isA(SqlKind.IS_NULL) && predicate instanceof RexCall) {
                RexNode operand1 = ((RexCall) predicate).getOperands().get(0);
                if (operand1 instanceof RexInputRef) {
                    idx = ((RexInputRef) operand1).getIndex();
                    if (!context.isFilter(idx)) {
                        return null;
                    }
                }
            }
            result.computeIfAbsent(idx, k -> new ArrayList<>());
            result.get(idx).add(predicate);
        }
        return result;
    }

    /**
     * make sure the base node contains primary key
     *
     * @param bottomProject the possible project above table scan
     * @param tableScan the only table to be scanned
     * @param table the table mate of the table
     * @param allColumns all columns in the table
     * @param relBuilder relNode builder
     * @return a relNode contains primary key columns in the output
     */
    private RelNode buildBaseNode(LogicalProject bottomProject,
                                  TableScan tableScan,
                                  TableMeta table,
                                  Map<ColumnMeta, Integer> allColumns,
                                  RelBuilder relBuilder) {
        if (bottomProject != null) {
            Set<Integer> recordedColumns = bottomProject.getProjects().stream().filter(s -> s instanceof RexInputRef).
                map(ref -> ((RexInputRef) ref).getIndex()).collect(Collectors.toSet());

            boolean missed = false;
            for (ColumnMeta column : table.getPrimaryIndex().getKeyColumns()) {
                if (!recordedColumns.contains(allColumns.get(column))) {
                    missed = true;
                    break;
                }
            }
            List<RexNode> projects = new ArrayList<>(bottomProject.getProjects());
            List<String> names = new ArrayList<>(bottomProject.getRowType().getFieldNames());
            if (missed) {
                for (ColumnMeta column : table.getPrimaryIndex().getKeyColumns()) {
                    int index = allColumns.get(column);
                    if (!recordedColumns.contains(index)) {
                        projects.add(
                            new RexInputRef(index, tableScan.getRowType().getFieldList().get(index).getType()));
                        names.add(tableScan.getRowType().getFieldNames().get(index));
                    }
                }
            }
            relBuilder.push(bottomProject.getInput());
            relBuilder.project(projects, names);
            return relBuilder.build();
        }
        return tableScan;
    }

    /**
     * context in tree generation
     */
    static public class GenerateContext {
        Set<Integer> orderColumn;
        Set<Integer> filterColumn;
        RelNode baseRelNode;
        RelOptTable table;
        RelBuilder relBuilder;
        // record other and conditions
        List<RexNode> andList;
        // map the column after project to the origin table column
        Map<Integer, Integer> afterProject;

        public GenerateContext(Set<Integer> orderColumn, Set<Integer> filterColumn, RelNode baseRelNode,
                               RelOptTable table, RelBuilder relBuilder, Map<Integer, Integer> afterProject) {
            this.orderColumn = orderColumn;
            this.filterColumn = filterColumn;
            this.baseRelNode = baseRelNode;
            this.table = table;
            this.relBuilder = relBuilder;
            andList = new ArrayList<>();
            this.afterProject = afterProject;
        }

        public boolean isOrder(int id) {
            return orderColumn.contains(id);
        }

        public boolean isFilter(int id) {
            return filterColumn.contains(id) || orderColumn.contains(id);
        }

        public RelNode getBaseRelNode() {
            return baseRelNode;
        }

        /**
         * deep copy the relNode tree
         */
        public RelNode getCopiedBaseRelNode() {
            if (baseRelNode instanceof LogicalProject) {
                if (baseRelNode.getInput(0) instanceof LogicalTableScan) {
                    LogicalTableScan oldScan = (LogicalTableScan) baseRelNode.getInput(0);
                    TableScan tableScan = LogicalTableScan.create(oldScan.getCluster(), oldScan.getTable());
                    LogicalProject project = (LogicalProject) baseRelNode;
                    return LogicalProject.create(tableScan, project.getProjects(), project.getRowType());
                }
            }
            if (baseRelNode instanceof LogicalTableScan) {
                return LogicalTableScan.create(baseRelNode.getCluster(), baseRelNode.getTable());
            }
            RelDrdsWriter relWriter = new RelDrdsWriter(CalcitePlanOptimizerTrace.DEFAULT_LEVEL);
            (baseRelNode).explainForDisplay(relWriter);
            throw new TddlRuntimeException(ErrorCode.ERR_UNEXPECTED_REL_TREE, "tree is " + relWriter.asString());
        }

        public RelOptTable getTable() {
            return table;
        }

        public RelBuilder getRelBuilder() {
            return relBuilder;
        }

        public void addAndCondition(RexNode rexNode) {
            andList.add(rexNode);
        }

        public void setAndCondition(int loc, RexNode rexNode) {
            andList.set(loc, rexNode);
        }

        public void removeAndCondition() {
            andList.remove(andList.size() - 1);
        }

        public List<RexNode> getAndConditions() {
            return andList;
        }

        public int getOriginColumnId(int id) {
            return afterProject.get(id);
        }

    }

    /**
     * all columns with bloom filters
     */
    static public GenerateContext getIndexColumns(RelNode baseRelNode,
                                                  RelOptTable table,
                                                  TableMeta tableMeta,
                                                  Map<ColumnMeta, Integer> allColumns,
                                                  RelBuilder relBuilder) {
        Map<Integer, Set<Integer>> beforeProject = new HashMap<>();
        if (baseRelNode instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) baseRelNode;
            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode node = project.getProjects().get(i);
                if (node instanceof RexInputRef) {
                    int index = ((RexInputRef) node).getIndex();
                    beforeProject.computeIfAbsent(index, k -> new HashSet<>());
                    beforeProject.get(index).add(i);
                }
            }
        } else {
            for (int i = 0; i < baseRelNode.getRowType().getFieldCount(); i++) {
                Set<Integer> col = new HashSet<>();
                col.add(i);
                beforeProject.put(i, col);
            }
        }
        // map the column after project to the origin table column
        Map<Integer, Integer> afterProject = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : beforeProject.entrySet()) {
            for (Integer id : entry.getValue()) {
                afterProject.put(id, entry.getKey());
            }
        }
        Set<Integer> columns = new HashSet<>();
        Set<Integer> orderedColumns = new HashSet<>();

        if (!tableMeta.isHasPrimaryKey()) {
            return null;
        }
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            // primary index or index with one column
            if (indexMeta.isPrimaryKeyIndex()) {
                orderedColumns = beforeProject.get(allColumns.get(indexMeta.getKeyColumns().get(0)));
            } else if (indexMeta.getKeyColumns().size() == 1) {
                int index = allColumns.get(indexMeta.getKeyColumns().get(0));
                if (beforeProject.containsKey(index)) {
                    columns.addAll(beforeProject.get(index));
                }
            }
        }
        if (orderedColumns == null) {
            orderedColumns = new HashSet<>();
        }
        return new GenerateContext(orderedColumns, columns, baseRelNode, table, relBuilder, afterProject);
    }

    /**
     * check whether the filter is complex to choose index
     */
    private boolean complexFilter(LogicalFilter filter) {
        if (filter == null) {
            return false;
        }
        RexNode condition = filter.getCondition();
        if (condition.isAlwaysFalse() || condition.isAlwaysTrue()) {
            return false;
        }
        List<RexNode> andConditions = RelOptUtil.conjunctions(condition);
        if (andConditions.size() > 1) {
            return true;
        }
        if (andConditions.size() == 0) {
            return false;
        }
        List<RexNode> orConditions = RelOptUtil.disjunctions(andConditions.get(0));
        return orConditions.size() > 1;
    }

    private void debugUsage(RelNode newRoot, RelNode oldRoot) {
        RelNode[] nodes = new RelNode[] {newRoot, oldRoot};
        for (RelNode node : nodes) {
            StringBuilder sb = new StringBuilder();
            RelDrdsWriter relWriter = new RelDrdsWriter(CalcitePlanOptimizerTrace.DEFAULT_LEVEL);
            node.explainForDisplay(relWriter);
            sb.append(relWriter.asString());
            RelMetadataQuery mq = node.getCluster().getMetadataQuery();
            sb.append(mq.getCumulativeCost(node)).append("\n");
            System.out.println(sb);
        }
    }
}
