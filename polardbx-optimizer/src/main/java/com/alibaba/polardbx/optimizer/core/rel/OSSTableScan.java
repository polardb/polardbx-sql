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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.OSSMergeIndexRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OSSTableScan extends LogicalView {

    private OrcTableScan orcNodeCache;
    private boolean noMoreFilterProject = false;
    private LogicalAggregate agg = null;
    private List<RelColumnOrigin> aggColumns = null;
    private OSSIndexContext indexContext = null;

    private RexNode runtimeFilter;

    public OSSTableScan(RelInput relInput) {
        super(relInput);
        if (relInput.get("ossRuntimeFilter") != null) {
            RelNode oldLastRel = relInput.getLastRel();
            relInput.setLastRel(this);
            try {
                this.runtimeFilter = relInput.getExpression("ossRuntimeFilter");
            } finally {
                relInput.setLastRel(oldLastRel);
            }
        }
        traitSet = traitSet.replace(relInput.getPartitionWise());
    }

    public OSSTableScan(TableScan scan, SqlSelect.LockMode lockMode) {
        super(scan, lockMode);
    }

    public OSSTableScan(LogicalView newLogicalView) {
        super(newLogicalView);
    }

    public OSSTableScan(LogicalView newLogicalView, RelTraitSet traitSet) {
        super(newLogicalView, traitSet);
    }

    public OSSTableScan(RelNode rel, RelOptTable table,
                        SqlNodeList hints, boolean noMoreFilterProject, OSSIndexContext indexContext) {
        super(rel, table, hints);
        this.noMoreFilterProject = noMoreFilterProject;
        this.indexContext = indexContext;
    }

    public OSSTableScan(RelNode rel, RelOptTable table, SqlNodeList hints,
                        SqlSelect.LockMode lockMode, SqlNode indexNode) {
        super(rel, table, hints, lockMode, indexNode);
    }

    public boolean isColumnarIndex() {
        TableMeta tableMeta = CBOUtil.getTableMeta(getTable());
        return tableMeta != null && tableMeta.isColumnar();
    }

    public static RelNode fromPhysicalTableOperation(PhyTableOperation extractPlan, ExecutionContext context,
                                                     String sourceTableName, int tableParamIndex) {
        String physicalSql = extractPlan.getNativeSql();
        Map<Integer, ParameterContext> paramMap = extractPlan.getParam();
        String phySchema = extractPlan.getDbIndex();
        String phyTable = extractPlan.getTableNames().get(0).get(0);

        RelNode plan = doPlan(context, sourceTableName, physicalSql, paramMap, tableParamIndex);

        // set target tables
        Map<String, List<List<String>>> targetTables = ImmutableMap.of(
            phySchema, ImmutableList.of(ImmutableList.of(phyTable))
        );
        if (plan != null) {
            OSSTableScanVisitor visitor = new OSSTableScanVisitor(targetTables);
            plan.accept(visitor);
            return plan;
        }

        return null;
    }

    private static RelNode doPlan(ExecutionContext context, String sourceTableName, String physicalSql,
                                  Map<Integer, ParameterContext> paramMap, int tableParamIndex) {
        Object[] params = new Object[paramMap.size()];
        for (int i = 0; i < paramMap.size(); i++) {
            if (i + 1 == tableParamIndex) {
                params[i] = sourceTableName;
            } else {
                params[i] = paramMap.get(i + 1).getValue();
            }
        }
        String logicalSql = replace(physicalSql, params);

        // fix npe
        if (context.getParams() == null) {
            context.setParams(new Parameters());
        }
        return Planner.getInstance().plan(logicalSql, context).getPlan();
    }

    private static class OSSTableScanVisitor extends RelShuttleImpl {
        private Map<String, List<List<String>>> targetTables;

        OSSTableScanVisitor(Map<String, List<List<String>>> targetTables) {
            this.targetTables = targetTables;
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (scan instanceof OSSTableScan) {
                ((OSSTableScan) scan).setTargetTables(targetTables);
            }
            return scan;
        }
    }

    private static String replace(String sql, Object[] params) {
        StringBuilder builder = new StringBuilder("/*+TDDL:cmd_extra()*/");

        int paramPos = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') {
                Object param = params[paramPos++];
                if (param instanceof Number) {
                    builder.append(param);
                } else {
                    builder.append("\'" + param + "\'");
                }
            } else {
                builder.append(sql.charAt(i));
            }
        }

        if (paramPos < params.length) {
            throw new RuntimeException("paramPos = " + paramPos + ", " + params.length);
        }
        return builder.toString();
    }

    @Override
    public String explainNodeName() {
        String name = "OSSTableScan";
        return name;

    }

    @Override
    public LogicalView copy(RelTraitSet traitSet) {
        OSSTableScan ossTableScan = new OSSTableScan(this);
        ossTableScan.traitSet = traitSet;
        ossTableScan.pushDownOpt = pushDownOpt.copy(ossTableScan, this.getPushedRelNode());
        ossTableScan.setNoMoreFilterProject(noMoreFilterProject);
        ossTableScan.runtimeFilter = runtimeFilter;
        if (indexContext != null) {
            ossTableScan.indexContext = indexContext.copy();
        }
        return ossTableScan;
    }

    @Override
    public LogicalView copy(RelTraitSet traitSet, RelNode newPushRelNode) {
        OSSTableScan ossTableScan = new OSSTableScan(this);
        ossTableScan.traitSet = traitSet;
        ossTableScan.pushDownOpt = pushDownOpt.copy(ossTableScan, newPushRelNode);
        ossTableScan.setNoMoreFilterProject(noMoreFilterProject);
        ossTableScan.runtimeFilter = runtimeFilter;
        if (indexContext != null) {
            ossTableScan.indexContext = indexContext.copy();
        }
        return ossTableScan;
    }

    @Override
    public RelNode clone() {
        final OSSTableScan ossTableScan = new OSSTableScan(this, lockMode);
        ossTableScan.setScalarList(scalarList);
        ossTableScan.correlateVariableScalar.addAll(correlateVariableScalar);
        ossTableScan.runtimeFilter = runtimeFilter;
        if (indexContext != null) {
            ossTableScan.indexContext = indexContext.copy();
        }
        return ossTableScan;
    }

    /**
     * check whether the ossTable scan should push project and filter or not
     *
     * @return false if can't push
     */
    public boolean canPushFilterProject() {
        return (!noMoreFilterProject) && (!withAgg());
    }

    public void setNoMoreFilterProject(boolean noMoreFilterProject) {
        this.noMoreFilterProject = noMoreFilterProject;
    }

    private LogicalFilter getFilter() {
        RelNode node = this.getPushedRelNode();
        while (!(node instanceof LogicalFilter)) {
            if (node.getInputs().isEmpty()) {
                return null;
            }
            node = node.getInput(0);
        }
        return (LogicalFilter) node;
    }

    /**
     * the table scan can push agg if it has no agg and at most a specific filter
     *
     * @return true if the agg can be push down to the table scan
     */
    public boolean canPushAgg() {
        if (isColumnarIndex()) {
            return false;
        }
        if (withAgg()) {
            return false;
        }
        /* the filter should be one of the following
            1.x>?
            2.x<?
            3.x>=?
            4.x<=?
            5.x between (?,?)
           where x is the first column of primary key
         */
        LogicalFilter filter = getFilter();
        if (filter == null) {
            return true;
        }
        RexNode predicate = filter.getCondition();

        int idx = -1;
        if (predicate instanceof RexCall) {
            RexCall rexCall = (RexCall) predicate;
            if (predicate.isA(SqlKind.GREATER_THAN_OR_EQUAL)
                || predicate.isA(SqlKind.LESS_THAN)
                || predicate.isA(SqlKind.GREATER_THAN)
                || predicate.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                RexNode operand1 = rexCall.getOperands().get(0);
                RexNode operand2 = rexCall.getOperands().get(1);
                if (operand1 instanceof RexInputRef && operand2 instanceof RexDynamicParam) {
                    idx = ((RexInputRef) operand1).getIndex();
                } else if (operand2 instanceof RexInputRef && operand1 instanceof RexDynamicParam) {
                    idx = ((RexInputRef) operand2).getIndex();
                }
            }
            if (predicate.isA(SqlKind.BETWEEN)) {
                RexNode operand1 = ((RexCall) predicate).getOperands().get(0);
                RexNode operand2 = ((RexCall) predicate).getOperands().get(1);
                RexNode operand3 = ((RexCall) predicate).getOperands().get(2);
                if (operand1 instanceof RexInputRef && operand2 instanceof RexDynamicParam
                    && operand3 instanceof RexDynamicParam) {
                    idx = ((RexInputRef) operand1).getIndex();
                }
            }
        }
        // not a target filter
        if (idx == -1) {
            return false;
        }
        String name = filter.getCluster().getMetadataQuery().getColumnOrigin(filter, idx).getColumnName();
        TableMeta tableMeta = CBOUtil.getTableMeta(getTable());
        return tableMeta.getPrimaryIndex().getKeyColumns().get(0).getName().equals(name);
    }

    public boolean withAgg() {
        return getAgg() != null;
    }

    /**
     * get the possible agg operator inside the OSSTableScan.
     * The expected aggregator should be one of COUNT/SUM/MIN/MAX
     *
     * @return an aggregator if not null
     */
    public LogicalAggregate getAgg() {
        if (agg == null) {
            getOrcNode();
        }
        return agg;
    }

    public List<RelColumnOrigin> getAggColumns() {
        if (agg == null) {
            getOrcNode();
        }
        return aggColumns;
    }

    private void indexSelection(OrcTableScan orcNode) {
        if (!canPushFilterProject()) {
            return;
        }

        RelNode plan = CBOUtil.OssTableScanFormat(getPushedRelNode());
        LogicalFilter filter = null;
        LogicalProject bottomProject = null;
        LogicalTableScan tableScan = null;
        if (plan instanceof LogicalProject) {
            plan = plan.getInput(0);
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
        // without filter, it's a full table scan
        if (filter == null) {
            return;
        }

        TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        Map<ColumnMeta, Integer> allColumns = new HashMap<>();
        for (int i = 0; i < tableMeta.getPhysicalColumns().size(); i++) {
            allColumns.put(tableMeta.getPhysicalColumns().get(i), i);
        }

        // build index
        OSSMergeIndexRule.GenerateContext generateContext = OSSMergeIndexRule.getIndexColumns(
            bottomProject == null ? tableScan : bottomProject,
            tableScan.getTable(),
            tableMeta,
            allColumns,
            null);
        if (generateContext == null) {
            return;
        }
        indexContext = andIndexContext(filter.getCondition(), generateContext, -1);

        orcNode.setIndexAble(indexContext);
    }

    private OSSIndexContext andIndexContext(RexNode andCondition,
                                            OSSMergeIndexRule.GenerateContext generateContext,
                                            int goal) {
        OSSIndexContext index = null;
        for (RexNode condition : RelOptUtil.conjunctions(andCondition)) {
            index = OSSIndexContext.mergeIndex(index, orIndexContext(condition, generateContext, goal));
        }
        return index;
    }

    private OSSIndexContext orIndexContext(RexNode orCondition, OSSMergeIndexRule.GenerateContext generateContext,
                                           int goal) {
        OSSIndexContext index = new OSSIndexContext(true);
        List<RexNode> conditions = RelOptUtil.disjunctions(orCondition);
        Map<Integer, List<RexNode>> clustering =
            OSSMergeIndexRule.clusteringDisjunction(conditions, generateContext);
        // can't find index for all clauses, use all bloom filters
        if (clustering == null) {
            return new OSSIndexContext();
        }

        Set<Integer> keys = new HashSet<>();
        // the index needed by the condition
        int goalIdx = -1;
        for (Integer id : clustering.keySet()) {
            if (generateContext.isOrder(id) || generateContext.isFilter(id)) {
                keys.add(id);
                goalIdx = id;
            }
        }
        // too many indexes, use all bloom filters
        if (keys.size() > 1) {
            return new OSSIndexContext();
        }
        //update the target
        if (goal == -1) {
            goal = goalIdx;
        }
        // the index needed here is not the target
        if (goalIdx != -1 && goal != goalIdx) {
            return new OSSIndexContext();
        }
        // need go further
        if (clustering.containsKey(-1)) {
            if (clustering.get(-1).size() == 1) {
                if (RelOptUtil.conjunctions(clustering.get(-1).get(0)).size() == 1) {
                    return new OSSIndexContext();
                }
            }
            for (RexNode condition : clustering.get(-1)) {
                index = OSSIndexContext.lowerIndex(index, andIndexContext(condition, generateContext, goal));
            }
        }
        return index;
    }

    public OrcTableScan getOrcNode() {
        if (orcNodeCache == null) {
            synchronized (this) {
                if (orcNodeCache == null) {
                    RelNode root = CBOUtil.optimizeByOrcImpl(this.getPushedRelNode());
                    // when agg are pushed to tableScan, we should record the agg information for executor
                    if (root instanceof LogicalAggregate) {
                        agg = (LogicalAggregate) root;
                        root = agg.getInput();
                        aggColumns = new ArrayList<>();
                        for (AggregateCall call : agg.getAggCallList()) {
                            if (call.getAggregation().getKind() == SqlKind.COUNT) {
                                if (call.getArgList().size() == 1) {
                                    aggColumns.add(agg.getCluster().getMetadataQuery()
                                        .getColumnOrigin(((OrcTableScan) root).getNodeForMetaQuery(),
                                            call.getArgList().get(0)));
                                } else {
                                    aggColumns.add(null);
                                }
                                continue;
                            }
                            aggColumns.add(agg.getCluster().getMetadataQuery()
                                .getColumnOrigin(((OrcTableScan) root).getNodeForMetaQuery(),
                                    call.getArgList().get(0)));
                        }
                    }
                    orcNodeCache = (OrcTableScan) root;
                    if (noMoreFilterProject && indexContext == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_SHOULD_NOT_BE_NULL);
                    }
                    orcNodeCache.setIndexAble(indexContext);
                }
            }
        }
        return orcNodeCache;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (withAgg()) {
            return this.getCluster().getPlanner().getCostFactory()
                .makeTinyCost();
        }

        if (isColumnarIndex() || PlannerContext.getPlannerContext(this).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_MOCK_COLUMNAR)) {
            double estimateRowCount = mq.getRowCount(this);
            return planner.getCostFactory().makeCost(estimateRowCount, estimateRowCount, 0, 0, 0);
        }

        // MetaQuery will compute the pushed node cumulative cost
        OrcTableScan orcNode = getOrcNode();
        // index selection
        if (indexContext == null) {
            indexSelection(orcNode);
        }
        int shardUpperBound = calShardUpperBound();
        int totalShardCount = getTotalShardCount();

        double estimateRowCount = mq.getRowCount(this);
        double totalRowCount = TableTopologyUtil.isShard(CBOUtil.getTableMeta(getTable())) ?
            getTable().getRowCount() * shardUpperBound / totalShardCount : getTable().getRowCount();
        // get total stripe number
        double rowSize = (double) TableScanIOEstimator.estimateRowSize(getTable().getRowType());
        int avgStripeLen = (int) ((64 << 20) / rowSize);
        long totalStripes = orcNode.getGroupNumber(estimateRowCount, totalRowCount, avgStripeLen);
        // get real row count to be tested in bloom filter
        double totalStripeRows = Math.min(totalRowCount, totalStripes * avgStripeLen);
        // read cost
        RelOptCost readCost = orcNode.getShardedReadCost(planner, estimateRowCount, totalStripeRows);
        // bloom filter cost
        if (indexContext != null && !indexContext.isPrimary()) {
            int base = indexContext.isSecondaryIndex() ? 1 : 2;
            readCost = readCost.plus(planner.getCostFactory().makeCost(
                0, totalStripeRows * base * CostModelWeight.BLOOM_FILTER_READ_COST, 0, 0, 0));
        }
        return readCost;
    }

    @Override
    public RelWriter explainLogicalView(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());

        List<RelNode> relList = new ArrayList<>();

        if (runtimeFilter != null) {
            relList.add(LogicalFilter.create(getOrcNode(), runtimeFilter));
        } else {
            relList.add(getOrcNode());
        }
        pw.item(RelDrdsWriter.LV_INPUTS, relList);
        return pw;
    }

    public void pushRuntimeFilter(RexNode runtimeFilter) {
        if (this.runtimeFilter == null) {
            this.runtimeFilter = runtimeFilter;
        } else {
            this.runtimeFilter = RexUtil.composeConjunction(this.getCluster().getRexBuilder(),
                RexUtil.flattenAnd(ImmutableList.of(this.runtimeFilter, runtimeFilter)), false);
        }
    }

    @Override
    public List<Integer> getBloomFilters() {
        List<Integer> bloomFilterIds = new ArrayList<>();

        if (runtimeFilter == null) {
            return bloomFilterIds;
        }

        List<RexNode> conditions = RelOptUtil.conjunctions(runtimeFilter);
        for (RexNode rexNode : conditions) {
            if (rexNode instanceof RexCall &&
                ((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterFunction) {
                SqlRuntimeFilterFunction runtimeFilterFunction =
                    (SqlRuntimeFilterFunction) ((RexCall) rexNode).getOperator();
                bloomFilterIds.add(runtimeFilterFunction.getId());
            }
        }
        return bloomFilterIds;
    }

    public Map<Integer, RexCall> getBloomFiltersMap() {
        Map<Integer, RexCall> results = new HashMap<>();

        if (runtimeFilter == null) {
            return results;
        }

        List<RexNode> conditions = RelOptUtil.conjunctions(runtimeFilter);
        for (RexNode rexNode : conditions) {
            if (rexNode instanceof RexCall &&
                ((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterFunction) {
                SqlRuntimeFilterFunction runtimeFilterFunction =
                    (SqlRuntimeFilterFunction) ((RexCall) rexNode).getOperator();
                results.put(runtimeFilterFunction.getId(), (RexCall) rexNode);
            }
        }
        return results;
    }

    /**
     * there are 4 types:
     * 1. Without filters.
     * 2. With filters, but can't use secondary index.
     * 3. With filters and can use a secondary index. Note that it is different from type 2
     * because it can prune stripes and row groups faster, so it's cost should be smaller.
     * 4. primary key is used
     */
    public static class OSSIndexContext implements Comparable<OSSIndexContext> {
        /* whether the table scan is indexAble or not*/
        boolean indexAble;
        /* whether the index is primary or not*/
        boolean primary;

        public OSSIndexContext() {
            indexAble = false;
        }

        public OSSIndexContext(boolean primary) {
            this.indexAble = true;
            this.primary = primary;
        }

        public OSSIndexContext(boolean indexAble, boolean primary) {
            this.indexAble = indexAble;
            this.primary = primary;
        }

        public boolean isPrimary() {
            return indexAble && primary;
        }

        public boolean isSecondaryIndex() {
            return indexAble && (!primary);
        }

        public OSSIndexContext copy() {
            return new OSSIndexContext(this.indexAble, this.primary);
        }

        public static OSSIndexContext mergeIndex(OSSIndexContext index1, OSSIndexContext index2) {
            if (index1 == null) {
                return index2;
            }
            if (index2 == null) {
                return index1;
            }
            return (index1.compareTo(index2) >= 0) ? index1 : index2;
        }

        public static OSSIndexContext lowerIndex(OSSIndexContext index1, OSSIndexContext index2) {
            if (index1 == null || index2 == null) {
                return null;
            }
            return (index1.compareTo(index2) >= 0) ? index2 : index1;
        }

        @Override
        public int compareTo(@NotNull OSSTableScan.OSSIndexContext o) {
            int value = primary ? 3 : (indexAble ? 2 : 1);
            int valueO = o.primary ? 3 : (o.indexAble ? 2 : 1);
            return value - valueO;
        }

        @Override
        public String toString() {
            return "OSSIndexContext{" +
                "indexAble=" + indexAble +
                ", Primary=" + primary + '}';
        }
    }

    @Override
    public void optimize() {
        getPushDownOpt().optimizeOSS();
        tableNames = collectTableNames();
        rebuildPartRoutingPlanInfo();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("ossRuntimeFilter", this.runtimeFilter, this.runtimeFilter != null)
            .itemIf("partitionWise", this.traitSet.getPartitionWise(), !this.traitSet.getPartitionWise().isTop());
    }
}
