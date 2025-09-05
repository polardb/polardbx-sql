package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;

public class PreFilterToSubqueryRule extends RelOptRule {

    public PreFilterToSubqueryRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final PreFilterToSubqueryRule INSTANCE = new PreFilterToSubqueryRule(
        operand(LogicalFilter.class, RelOptRule.any()), "filter_to_subquery");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_PREFILTER_TO_SUBQUERY);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
        List<RexCall> preFilters = Lists.newArrayList();
        List<RexCall> sargableFilters = Lists.newArrayList();
        List<RexNode> newConditions = Lists.newArrayList();
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall) {
                RexCall rexCall = (RexCall) condition;
                if (rexCall.getOperator() == TddlOperatorTable.PRE_FILTER) {
                    preFilters.add(rexCall);
                    continue;
                }
                newConditions.add(rexCall);
                if (rexCall.getOperator().getKind().belongsTo(SqlKind.SARGABLE)) {
                    sargableFilters.add(rexCall);
                }
            }
        }
        if (preFilters.isEmpty()) {
            return;
        }

        RelMetadataQuery mq = call.getMetadataQuery();
        Set<RexTableInputRef.RelTableRef> tableRefs = mq.getTableReferences(filter);
        if (tableRefs == null || tableRefs.size() != 1) {
            return;
        }

        for (RexCall preFilter : preFilters) {
            newConditions.add(convertPreFilter(call, filter, sargableFilters,
                preFilter.getOperands(), call.getMetadataQuery()));
        }
        if (newConditions.size() == 1) {
            call.transformTo(
                filter.copy(filter.getTraitSet(), filter.getInput(), newConditions.get(0)));
            return;
        }
        RexBuilder rb = call.builder().getRexBuilder();
        call.transformTo(
            filter.copy(filter.getTraitSet(), filter.getInput(), RexUtil.flatten(rb, rb.makeCall(AND, newConditions))));
    }

    RexNode convertPreFilter(RelOptRuleCall call, LogicalFilter filter, List<RexCall> sargableFilters,
                             List<RexNode> nodes, RelMetadataQuery mq) {
        RelBuilder rb = call.builder();
        RexBuilder rexBuilder = rb.getRexBuilder();
        PlannerContext pc = PlannerContext.getPlannerContext(filter);
        if (CollectionUtils.isEmpty(nodes) || nodes.size() != 1) {
            return convertTruth(rexBuilder);
        }
        if (!(nodes.get(0) instanceof RexInputRef)) {
            return convertTruth(rexBuilder);
        }

        // get column origin of pre-filter column
        RelColumnOrigin columnOrigin = mq.getColumnOrigin(filter, ((RexInputRef) nodes.get(0)).getIndex());
        if (columnOrigin == null) {
            return convertTruth(rexBuilder);
        }
        String preFilterColumn = columnOrigin.getColumnName();
        int preFilterOrd = columnOrigin.getOriginColumnOrdinal();

        // must be auto mode shard table
        TableMeta tm = CBOUtil.getTableMeta(columnOrigin.getOriginTable());
        if (tm == null) {
            return convertTruth(rexBuilder);
        }
        if (!DbInfoManager.getInstance().isNewPartitionDb(tm.getSchemaName())) {
            return convertTruth(rexBuilder);
        }
        if (tm.getPartitionInfo() == null ||
            tm.getPartitionInfo().getAllPartLevelCount() == 0) {
            return convertTruth(rexBuilder);
        }

        // get sk except the last partition by
        List<String> skList = Lists.newArrayList();
        for (int i = 1; i < tm.getPartitionInfo().getAllPartLevelCount(); i++) {
            List<String> preSk = tm.getPartitionInfo().getPartLevelToPartColsMapping().get(i);
            if (preSk != null) {
                skList.addAll(preSk);
            }
        }

        // <sklist, preFilterColumn> can't be duplicated
        Set<String> skSet = Sets.newTreeSet(String::compareToIgnoreCase);
        skSet.addAll(skList);
        if (skSet.size() != skList.size() || skList.contains(preFilterColumn)) {
            return convertTruth(rexBuilder);
        }

        // tm must have an index started with <sklist, preFilterColumn>
        String indexName = getForceIndex(tm, ImmutableList.<String>builder()
            .addAll(skList).add(preFilterColumn).build());
        if (StringUtils.isEmpty(indexName)) {
            return convertTruth(rexBuilder);
        }

        // lower bound and upper bound of subquery result
        List<RexNode> filterBound = Lists.newArrayList(null, null);
        // filter condition in subquery, contains equality only
        List<RexNode> subqueryCond = Lists.newArrayList();
        // record covered columns in subqueryCond
        Set<String> filteredColumns = Sets.newTreeSet(String::compareToIgnoreCase);
        // all the shard keys except the last must have equality
        for (RexCall rexCall : sargableFilters) {
            classifyRexCall(rexCall,
                index -> getRefColumnName(filter, tm, index, skSet, mq, preFilterColumn),
                pc,
                filterBound,
                subqueryCond,
                filteredColumns);
        }

        // all sk are covered
        if (filteredColumns.size() != skList.size()) {
            return convertTruth(rexBuilder);
        }

        // switch
        if (!pc.getParamManager().getBoolean(ConnectionParams.ENABLE_PREFILTER_LOWER_BOUND)) {
            filterBound.set(0, null);
        }
        if (!pc.getParamManager().getBoolean(ConnectionParams.ENABLE_PREFILTER_UPPER_BOUND)) {
            filterBound.set(1, null);
        }
        // neither lower bound nor upper bound is set
        if (filterBound.get(0) == null && filterBound.get(1) == null) {
            return convertTruth(rexBuilder);
        }

        RexNode resultCondition = null;
        for (int i = 0; i < 2; i++) {
            if (filterBound.get(i) == null) {
                continue;
            }
            // make tableScan
            LogicalTableScan newScan =
                LogicalTableScan.create(filter.getCluster(), columnOrigin.getOriginTable(), null,
                    ForceIndexUtil.genForceSqlNode(indexName), null, null, null);
            rb.push(newScan);

            // make filter
            RexShuttle newCond = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    RelColumnOrigin origin = mq.getColumnOrigin(filter, inputRef.getIndex());
                    if (origin == null) {
                        return null;
                    }
                    if (!tm.equals(CBOUtil.getTableMeta(origin.getOriginTable()))) {
                        return null;
                    }
                    return RexInputRef.of(origin.getOriginColumnOrdinal(), newScan.getRowType());
                }
            };
            subqueryCond = subqueryCond.stream().map(x -> x.accept(newCond)).collect(Collectors.toList());
            if (subqueryCond.size() == 1) {
                rb.filter(subqueryCond.get(0));
            } else if (subqueryCond.size() > 1) {
                rb.filter(rexBuilder.makeCall(AND, subqueryCond));
            }

            RelNode newFilter = rb.build();
            // make order by
            if (i == 0) {

                // col >= ? --> max(col) >= ?
                LogicalAggregate agg = LogicalAggregate.create(newFilter, ImmutableBitSet.of(), null,
                    Lists.newArrayList(AggregateCall.create(SqlStdOperatorTable.MAX,
                        false,
                        false,
                        ImmutableIntList.of(preFilterOrd),
                        -1,
                        newScan.getRowType().getFieldList().get(preFilterOrd).getType(),
                        newScan.getRowType().getFieldList().get(preFilterOrd).getName())));
                rb.push(agg);
                // make subquery
                RexSubQuery rexSubQuery = RexSubQuery.scalar(rb.build());
                // make final rexNode
                resultCondition = rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, rexSubQuery, filterBound.get(i));
            } else {
                // col <= ? --> min(col) <= ?
                LogicalAggregate agg = LogicalAggregate.create(newFilter, ImmutableBitSet.of(), null,
                    Lists.newArrayList(AggregateCall.create(SqlStdOperatorTable.MIN,
                        false,
                        false,
                        ImmutableIntList.of(preFilterOrd),
                        -1,
                        newScan.getRowType().getFieldList().get(preFilterOrd).getType(),
                        newScan.getRowType().getFieldList().get(preFilterOrd).getName())));
                rb.push(agg);
                // make subquery
                RexSubQuery rexSubQuery = RexSubQuery.scalar(rb.build());
                // make final rexNode
                resultCondition = resultCondition == null ?
                    rexBuilder.makeCall(LESS_THAN_OR_EQUAL, rexSubQuery, filterBound.get(i)) :
                    rexBuilder.makeCall(AND, resultCondition,
                        rexBuilder.makeCall(LESS_THAN_OR_EQUAL, rexSubQuery, filterBound.get(i)));
            }
        }
        return resultCondition;
    }

    private void classifyRexCall(RexCall rexCall,
                                 Function<Integer, Pair<Boolean, String>> function,
                                 PlannerContext pc,
                                 List<RexNode> filterBound,
                                 List<RexNode> subqueryCond,
                                 Set<String> filteredColumns) {
        RexNode leftRexNode;
        RexNode rightRexNode;
        Pair<Boolean, String> originInfo;
        int inputRef = -1;
        Object value = null;
        switch (rexCall.getOperator().getKind()) {
        case EQUALS:
            if (rexCall.getOperands().size() != 2) {
                break;
            }
            leftRexNode = rexCall.getOperands().get(0);
            rightRexNode = rexCall.getOperands().get(1);
            if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
                inputRef = ((RexInputRef) leftRexNode).getIndex();
                value = DrdsRexFolder.fold(rightRexNode, pc);
            } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
                inputRef = ((RexInputRef) rightRexNode).getIndex();
                value = DrdsRexFolder.fold(leftRexNode, pc);
            }
            if (inputRef != -1 && value != null &&
                (originInfo = function.apply(inputRef)) != null) {
                if (originInfo.getKey()) {
                    break;
                }
                filteredColumns.add(originInfo.getValue());
                subqueryCond.add(rexCall);
            }
            break;
        case IS_NULL:
            if (rexCall.getOperands().size() != 1) {
                break;
            }
            leftRexNode = rexCall.getOperands().get(0);
            if (leftRexNode instanceof RexInputRef) {
                inputRef = ((RexInputRef) leftRexNode).getIndex();
            }
            if (inputRef != -1 &&
                (originInfo = function.apply(inputRef)) != null) {
                if (originInfo.getKey()) {
                    break;
                }
                filteredColumns.add(originInfo.getValue());
                subqueryCond.add(rexCall);
            }
            break;
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            if (rexCall.getOperands().size() != 2) {
                break;
            }
            leftRexNode = rexCall.getOperands().get(0);
            rightRexNode = rexCall.getOperands().get(1);
            if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
                inputRef = ((RexInputRef) leftRexNode).getIndex();
                value = DrdsRexFolder.fold(rightRexNode, pc);
                if (value != null &&
                    (originInfo = function.apply(inputRef)) != null) {
                    if (originInfo.getKey()) {
                        if (rexCall.getOperator().getKind() == SqlKind.LESS_THAN ||
                            rexCall.getOperator().getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
                            // col < ?
                            filterBound.set(1, rightRexNode);
                        } else {
                            // col > ?
                            filterBound.set(0, rightRexNode);
                        }
                    }
                }
            } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
                inputRef = ((RexInputRef) rightRexNode).getIndex();
                value = DrdsRexFolder.fold(leftRexNode, pc);
                if (value != null &&
                    (originInfo = function.apply(inputRef)) != null) {
                    if (originInfo.getKey()) {
                        if (rexCall.getOperator().getKind() == SqlKind.LESS_THAN ||
                            rexCall.getOperator().getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
                            // ? < col
                            filterBound.set(0, leftRexNode);
                        } else {
                            // ? > col
                            filterBound.set(1, leftRexNode);
                        }
                    }
                }
            }
            break;
        case BETWEEN:
            if (rexCall.getOperands().size() != 3) {
                break;
            }
            leftRexNode = rexCall.getOperands().get(0);
            rightRexNode = rexCall.getOperands().get(1);
            RexNode thirdRexNode = rexCall.getOperands().get(2);
            if (leftRexNode instanceof RexInputRef &&
                !(rightRexNode instanceof RexInputRef) &&
                !(thirdRexNode instanceof RexInputRef)) {
                inputRef = ((RexInputRef) leftRexNode).getIndex();
                if (DrdsRexFolder.fold(rightRexNode, pc) != null &&
                    DrdsRexFolder.fold(thirdRexNode, pc) != null &&
                    (originInfo = function.apply(inputRef)) != null) {
                    if (originInfo.getKey()) {
                        filterBound.set(0, rightRexNode);
                        filterBound.set(1, thirdRexNode);
                    }
                }
            }
            break;
        case IN:
        case NOT_EQUALS:
        default:
            // do nothing
        }
    }

    private static RexNode convertTruth(RexBuilder rb) {
        return rb.makeLiteral(true);
    }

    private String getForceIndex(TableMeta tm, List<String> cols) {
        for (IndexMeta indexMeta : tm.getIndexes()) {
            if (indexMeta.isCoverShardKey(cols)) {
                return indexMeta.getPhysicalIndexName();
            }
        }
        return null;
    }

    /**
     * @return {whether preFilterColumn, column name of ref index} pair
     */
    private static Pair<Boolean, String> getRefColumnName(LogicalFilter filter, TableMeta tm, Integer ref,
                                                          Set<String> skSet,
                                                          RelMetadataQuery mq, String preFilterColumn) {
        RelColumnOrigin origin = mq.getColumnOrigin(filter, ref);
        if (origin == null) {
            return null;
        }
        if (!tm.equals(CBOUtil.getTableMeta(origin.getOriginTable()))) {
            return null;
        }
        if (skSet.contains(origin.getColumnName())) {
            return Pair.of(false, origin.getColumnName());
        }
        if (preFilterColumn.equalsIgnoreCase(origin.getColumnName())) {
            return Pair.of(true, origin.getColumnName());
        }
        return null;
    }
}
