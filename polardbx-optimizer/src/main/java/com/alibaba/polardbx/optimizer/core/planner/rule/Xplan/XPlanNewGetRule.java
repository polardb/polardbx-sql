package com.alibaba.polardbx.optimizer.core.planner.rule.Xplan;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanEqualTuple;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XPlanNewGetRule extends RelOptRule {

    public XPlanNewGetRule(RelOptRuleOperand operand, String description) {
        super(operand, "XPlan_new_rule:" + description);
    }

    public static final XPlanNewGetRule INSTANCE = new XPlanNewGetRule(
        operand(LogicalFilter.class, operand(XPlanTableScan.class, none())), "filter_tableScan_to_get");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        XPlanTableScan tableScan = (XPlanTableScan) call.rels[1];

        if (!tableScan.getGetExprs().isEmpty() || tableScan.getGetIndex() != null) {
            return; // Already pushed.
        }

        final List<RexNode> restConditions = new ArrayList<>();
        final List<XPlanEqualTuple> andConditions = XPlanNewGetRule.getAndLookupCondition(filter, restConditions);
        if (andConditions.isEmpty()) {
            return; // No conditions to generate get.
        }

        final TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes = findGetIndex(tableMeta, andConditions);
        PlannerContext context = call.getPlanner().getContext().unwrap(PlannerContext.class);
        lookupIndexes = filterIndex(tableScan, tableMeta, lookupIndexes, context);
        if (lookupIndexes.isEmpty()) {
            return; // No indexed to lookup.
        }

        // choose the best index
        int selected = selectIndex(
            tableScan,
            tableMeta,
            lookupIndexes,
            andConditions
        );

        if (selected == -1) {
            return;
        }

        final String useIndexName;
        final List<Pair<Integer, Long>> useConditions;
        useIndexName = lookupIndexes.get(selected).getKey().getPhysicalIndexName();
        useConditions = lookupIndexes.get(selected).getValue();

        recordWhereInfo(filter, !(useConditions.size() == andConditions.size() && restConditions.isEmpty()));

        final List<XPlanEqualTuple> getExpr = useConditions.stream()
            .map(cond -> andConditions.get(cond.left))
            .collect(Collectors.toList());
        tableScan.getGetExprs().add(getExpr);
        tableScan.setGetIndex(useIndexName);
        tableScan.resetNodeForMetaQuery();

        final boolean needExtraFilter = useConditions.stream()
            .anyMatch(cond -> cond.right != 0); // Any sub part in equal condition.
        if (needExtraFilter || DynamicConfig.getInstance().getXprotoAlwaysKeepFilterOnXplanGet()) {
            return; // Just keep the filter and keep the original rel node tree.
        }

        if (useConditions.size() == andConditions.size() && restConditions.isEmpty()) {
            // Full push down.
            call.transformTo(tableScan);
        } else {
            // Part push down.
            // Generate rest filter.
            RexNode restFilter = null;
            for (RexNode rex : restConditions) {
                if (null == restFilter) {
                    restFilter = rex;
                } else {
                    restFilter = tableScan.getCluster().getRexBuilder()
                        .makeCall(SqlStdOperatorTable.AND, restFilter, rex);
                }
            }
            final Set<Integer> useConditionSet =
                useConditions.stream().map(cond -> cond.left).collect(Collectors.toSet());
            for (int idx = 0; idx < andConditions.size(); ++idx) {
                if (useConditionSet.contains(idx)) {
                    continue;
                }
                final XPlanEqualTuple tuple = andConditions.get(idx);
                final RexNode rex = tableScan.getCluster().getRexBuilder()
                    .makeCall(tuple.getOperator(), tuple.getKey(), tuple.getValue());
                if (null == restFilter) {
                    restFilter = rex;
                } else {
                    restFilter = tableScan.getCluster().getRexBuilder()
                        .makeCall(SqlStdOperatorTable.AND, restFilter, rex);
                }
            }
            if (null == restFilter) {
                throw GeneralUtil.nestedException("XPlan part filter push down fatal.");
            }

            final LogicalFilter newFilter = LogicalFilter.create(tableScan, restFilter);
            call.transformTo(newFilter);
        }
    }

    protected int selectIndex(
        XPlanTableScan tableScan,
        TableMeta tableMeta,
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes,
        List<XPlanEqualTuple> andConditions) {

        // choose primary key or unique key firstly
        for (int idx = 0; idx < lookupIndexes.size(); ++idx) {
            IndexMeta indexMeta = lookupIndexes.get(idx).getKey();
            // pk or uk
            if (indexMeta.isPrimaryKeyIndex() || indexMeta.isUniqueIndex()) {
                // covering all key columns
                if (indexMeta.getKeyColumns().size() == lookupIndexes.get(idx).getValue().size()) {
                    return idx;
                }
            }
        }

        // don't use xplan for built in db access
        if (SystemDbHelper.isDBBuildIn(tableMeta.getSchemaName())) {
            return -1;
        }

        // statistics expired
        if (StatisticManager.expired(tableMeta.getSchemaName(), tableMeta.getTableName())) {
            return -1;
        }

        // Select minimum row count
        Pair<Double, Integer> idx = getBestIndexByRows(tableScan, lookupIndexes, andConditions);
        if (idx.getKey() < PlannerContext.getPlannerContext(tableScan).getParamManager()
            .getLong(ConnectionParams.XPLAN_MAX_SCAN_ROWS)) {
            return idx.getValue();
        }
        return -1;
    }

    protected static Pair<Double, Integer> getBestIndexByRows(
        XPlanTableScan tableScan,
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes,
        List<XPlanEqualTuple> andConditions) {
        double minRowCount = Double.MAX_VALUE;
        int selected = -1;
        final RelMetadataQuery mq = tableScan.getCluster().getMetadataQuery();
        for (int idx = 0; idx < lookupIndexes.size(); ++idx) {
            RexNode cond = null;
            for (Pair<Integer, Long> conditonIdx : lookupIndexes.get(idx).getValue()) {
                final RexNode rex = tableScan.getCluster().getRexBuilder()
                    .makeCall(andConditions.get(conditonIdx.left).getOperator(),
                        andConditions.get(conditonIdx.left).getKey(),
                        andConditions.get(conditonIdx.left).getValue());
                if (null == cond) {
                    cond = rex;
                } else {
                    cond = tableScan.getCluster().getRexBuilder()
                        .makeCall(SqlStdOperatorTable.AND, cond, rex);
                }
            }
            if (null == cond) {
                cond = tableScan.getCluster().getRexBuilder().makeLiteral(true);
            }
            final double rowCount = RelMdUtil.estimateFilteredRows(tableScan, cond, mq);
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selected = idx;
            }
        }
        return Pair.of(minRowCount, selected);
    }

    protected static List<Pair<IndexMeta, List<Pair<Integer, Long>>>> findGetIndex(TableMeta tableMeta,
                                                                                   List<XPlanEqualTuple> conditions) {
        final Map<String, Integer> keyMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int idx = 0; idx < conditions.size(); ++idx) {
            keyMap.put(tableMeta.getAllColumns().get(conditions.get(idx).getKey().getIndex()).getName(), idx);
        }
        final List<Pair<IndexMeta, List<Pair<Integer, Long>>>> result = new ArrayList<>();

        // Note: Indexes include PRIMARY.
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            final List<Pair<Integer, Long>> conditionIndexes = new ArrayList<>();
            if (indexMeta.getPhysicalIndexName().equalsIgnoreCase("PRIMARY") && !tableMeta.isHasPrimaryKey()) {
                continue;
            }
            for (IndexColumnMeta columnMeta : indexMeta.getKeyColumnsExt()) {
                final Integer idx = keyMap.get(columnMeta.getColumnMeta().getName());
                if (null == idx) {
                    break;
                } else {
                    conditionIndexes.add(Pair.of(idx, columnMeta.getSubPart())); // Record the sub part length.
                }
            }
            if (conditionIndexes.size() > 0) {
                result.add(Pair.of(indexMeta, conditionIndexes));
            }
        }
        return result;
    }

    static List<XPlanEqualTuple> getAndLookupCondition(LogicalFilter filter, List<RexNode> restCondition) {
        final Queue<RexCall> scanQueue = new LinkedList<>();
        final RexNode cnf = RexUtil.toSimpleCnf(filter.getCluster().getRexBuilder(), filter.getCondition());
        if (!(cnf instanceof RexCall)) {
            restCondition.add(cnf);
            return Collections.emptyList();
        }

        scanQueue.offer((RexCall) cnf);

        final List<XPlanEqualTuple> result = new ArrayList<>();

        // BFS.
        while (!scanQueue.isEmpty()) {
            final RexCall call = scanQueue.poll();
            switch (call.getOperator().getKind()) {
            case AND:
                // Push children.
                for (RexNode node : call.getOperands()) {
                    if (node instanceof RexCall) {
                        scanQueue.offer((RexCall) node);
                    } else {
                        restCondition.add(node);
                    }
                }
                break;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                // Record equal condition.
                if (call.getOperands().size() != 2 || !(call.getOperands().get(0) instanceof RexInputRef)) {
                    restCondition.add(call);
                    continue; // Not supported.
                }
                final RexNode value = call.getOperands().get(1);
                if (value.getKind() != SqlKind.DYNAMIC_PARAM && value.getKind() != SqlKind.LITERAL) {
                    restCondition.add(call);
                    continue; // Can not cast to scalar.
                }
                // Dealing key.
                result.add(new XPlanEqualTuple(call.getOperator(), (RexInputRef) call.getOperands().get(0), value));
                break;

            default:
                restCondition.add(call); // Not supported.
            }
        }
        return result;
    }

    protected List<Pair<IndexMeta, List<Pair<Integer, Long>>>> filterIndex(
        XPlanTableScan tableScan,
        TableMeta tableMeta,
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes,
        PlannerContext plannerContext) {
        Integer filterIndex = Optional.ofNullable(tableScan.getIndexNode())
            // FORCE INDEX
            .filter(indexNode -> indexNode instanceof SqlNodeList && ((SqlNodeList) indexNode).size() > 0)
            // If more than one index specified, choose first one only
            .map(indexNode -> (SqlIndexHint) ((SqlNodeList) indexNode).get(0))
            // only support force index
            .filter(SqlIndexHint::forceIndex)
            // Dealing with force index(`xxx`), `xxx` will decoded as string.
            .map(indexNode -> GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(indexNode.getIndexList().get(0))))
            .flatMap(indexName -> {
                // Find index from hint if can use.
                final List<IndexMeta> indexNameList = Pair.left(lookupIndexes);
                for (int i = 0; i < indexNameList.size(); ++i) {
                    if (indexNameList.get(i).getPhysicalIndexName().equalsIgnoreCase(indexName)) {
                        return Optional.of(i);
                    }
                }
                return Optional.empty();
            }).orElse(null);
        if (filterIndex != null) {
            return ImmutableList.of(lookupIndexes.get(filterIndex));
        }

        // filter hot indexes
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> resultList = Lists.newArrayList();
        for (Pair<IndexMeta, List<Pair<Integer, Long>>> pair : lookupIndexes) {
            StatisticResult statisticResult;
            if (plannerContext != null && plannerContext.isNeedStatisticTrace()) {
                statisticResult = StatisticManager.getInstance().hotColumns(
                    tableMeta.getSchemaName(),
                    tableMeta.getTableName(),
                    pair.left.getKeyColumns().stream().limit(pair.right.size()).map(ColumnMeta::getName).collect(
                        Collectors.toList())
                    , true);
                plannerContext.recordStatisticTrace(statisticResult.getTrace());
            } else {
                statisticResult = StatisticManager.getInstance().hotColumns(
                    tableMeta.getSchemaName(),
                    tableMeta.getTableName(),
                    pair.left.getKeyColumns().stream().limit(pair.right.size()).map(ColumnMeta::getName).collect(
                        Collectors.toList())
                    , false);
            }

            if (!statisticResult.getBooleanValue()) {
                resultList.add(pair);
            }
        }
        return resultList;
    }

    protected void recordWhereInfo(RelNode node, boolean usingWhere) {
        // do nothing
    }
}
