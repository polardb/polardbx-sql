package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.OneStepTransformer;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PagingForceToJoinRule extends RelOptRule {
    static final Predicate<LogicalSort> LIMIT = new PredicateImpl<LogicalSort>() {
        @Override
        public boolean test(LogicalSort logicalSort) {
            return logicalSort != null && logicalSort.fetch != null;
        }
    };

    static final Predicate<LogicalFilter> FILTER_NO_SUBQUERY = new PredicateImpl<LogicalFilter>() {
        @Override
        public boolean test(LogicalFilter logicalFilter) {
            return logicalFilter != null && !RexUtil.hasSubQuery(logicalFilter.getCondition());
        }
    };

    static final Predicate<LogicalProject> PROJECT_NO_SUBQUERY = new PredicateImpl<LogicalProject>() {
        @Override
        public boolean test(LogicalProject logicalProject) {
            if (logicalProject == null) {
                return false;
            }
            for (RexNode rexNode : logicalProject.getProjects()) {
                if (RexUtil.hasSubQuery(rexNode)) {
                    return false;
                }
            }
            return true;
        }
    };

    public static final PagingForceToJoinRule INSTANCE = new PagingForceToJoinRule(
        operand(LogicalSort.class, null, LIMIT,
            operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY,
                operand(TableScan.class, null, PagingForceRemoveRule.PAGING, none()))),
        RelFactories.LOGICAL_BUILDER, "INSTANCE");

    public static final PagingForceToJoinRule PROJECT = new PagingForceToJoinRule(
        operand(LogicalSort.class, null, LIMIT,
            operand(LogicalProject.class, null, PROJECT_NO_SUBQUERY,
                operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY,
                    operand(TableScan.class, null, PagingForceRemoveRule.PAGING, none())))),
        RelFactories.LOGICAL_BUILDER, "PROJECT");

    public PagingForceToJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, "PagingForceToJoinRule:" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PAGING_FORCE_TO_JOIN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == PROJECT) {
            // We have to perform SortProjectTranspose before and ProjectSortTransitive after in this rule to
            // make sure Sort is on the top,
            // instead of calling DrdsSortProjectTransposeRule, PagingForceToJoinRule, ProjectSortTransitiveRule
            // separately in HepPlanner.
            // Note that ProjectSortTransitiveRule is not an inverse of DrdsSortProjectTransposeRule,
            // it's possible that DrdsSortProjectTransposeRule makes Project on Top, but ProjectSortTransitive
            // fails to transpose Sort to Top.
            // Executor assumes that Sort on Top of LogicalView implies mergeSort, so it's not allowed to
            // change the root if it's Sort.
            // When the assumption is no longer needed, one can simplify this rule.
            LogicalSort oldSort = call.rel(0);
            LogicalProject oldProject = call.rel(1);
            // SortProjectTranspose
            RelNode node = OneStepTransformer.transform(
                oldSort.copy(oldSort.getTraitSet(), oldProject, oldSort.getCollation(), oldSort.offset, oldSort.fetch),
                DrdsSortProjectTransposeRule.INSTANCE);
            if (!(node instanceof LogicalProject)) {
                return;
            }
            LogicalProject project = (LogicalProject) node;
            if (!(project.getInput() instanceof LogicalSort)) {
                return;
            }
            LogicalSort sort = (LogicalSort) project.getInput();
            LogicalFilter filter = call.rel(2);
            TableScan scan = call.rel(3);
            RelNode input = onMatch(call, sort, filter, scan);
            if (!(input instanceof LogicalSort)) {
                return;
            }
            node = OneStepTransformer.transform(
                project.copy(project.getTraitSet(), ImmutableList.of(input)),
                ProjectSortTransitiveRule.NO_PRUNE);
            if (node instanceof LogicalSort) {
                call.transformTo(node);
            }
        } else {
            LogicalSort sort = call.rel(0);
            LogicalFilter filter = call.rel(1);
            TableScan scan = call.rel(2);
            RelNode node = onMatch(call, sort, filter, scan);
            if (node instanceof LogicalSort) {
                call.transformTo(node);
            }
        }
    }

    public RelNode onMatch(RelOptRuleCall call,
                           LogicalSort sort,
                           LogicalFilter filter,
                           TableScan scan) {

        // get indexMeta of paging_force index
        String index = getIndexName(scan);
        if (index == null) {
            return null;
        }
        TableMeta tm = CBOUtil.getTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        IndexMeta indexMeta = tm.getIndexMeta(index);
        if (indexMeta == null) {
            return null;
        }

        // build bitSet of columns of index
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
            int loc = columnOrd.getOrDefault(columnMeta.getName().toLowerCase(), -1);
            if (loc >= 0) {
                builder.set(loc);
            }
        }

        List<Integer> pkList = getPkList(tm, columnOrd);
        List<Integer> skList = ForceIndexUtil.getSkList(tm, columnOrd);
        if (CollectionUtils.isEmpty(pkList) || skList == null) {
            return null;
        }
        // remove sk column in pkList
        skList = skList.stream().filter(x -> !pkList.contains(x)).collect(Collectors.toList());
        // add primary key to indexBits
        for (int loc : pkList) {
            builder.set(loc);
        }
        ImmutableBitSet indexBits = builder.build();

        // check filter column and sort column can be covered by paging_force
        ImmutableBitSet filterBits = RelOptUtil.InputFinder.bits(filter.getCondition());
        ImmutableBitSet sortBits = ImmutableBitSet.of(sort.getCollation().getKeys());
        ImmutableBitSet skBits = ImmutableBitSet.of(skList);
        if (!indexBits.contains(filterBits)) {
            return null;
        }
        if (!indexBits.contains(sortBits)) {
            return null;
        }
        if (!indexBits.contains(skBits)) {
            return null;
        }

        //         topSort
        //            |
        //       topProject(right)
        //            |
        //         join(pk=pk,sk=sk)
        //        /           \
        //  project(pk,sk)     tablescan
        //    |
        //   sort
        //    |
        //  filter
        //    |
        //  tablescan(force index)
        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        SqlNode sqlNode = ForceIndexUtil.genForceSqlNode(index);
        LogicalTableScan newTableScan =
            LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                sqlNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
        LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newTableScan, filter.getCondition());
        LogicalSort newSort = sort.copy(sort.getTraitSet(), newFilter, sort.getCollation(), sort.offset, sort.fetch);
        relBuilder.push(newSort);

        List<Integer> pkskList = ImmutableList.<Integer>builder().addAll(pkList).addAll(skList).build();
        // TODO: remove the boolean list if ConditionExtractor support '<=>'
        List<Boolean> pkskbNullableList = getPkSKNullableList(tm, pkList, skList, newFilter.getCondition(),
            PlannerContext.getPlannerContext(filter));
        List<RexNode> projects = Lists.newArrayList();
        for (int loc : pkskList) {
            projects.add(rexBuilder.makeInputRef(newSort, loc));
        }
        relBuilder.project(projects);

        RelNode left = relBuilder.build();
        RelNode right = LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
            null, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
        List<RexNode> joinCond = Lists.newArrayList();
        for (int i = 0; i < pkskList.size(); i++) {
            joinCond.add(rexBuilder.makeCall(
                pkskbNullableList.get(i) ? TddlOperatorTable.IS_NOT_DISTINCT_FROM : TddlOperatorTable.EQUALS,
                rexBuilder.makeInputRef(left, i),
                RexUtil.shift(rexBuilder.makeInputRef(right, pkskList.get(i)), pkskList.size())));
        }

        relBuilder.push(left);
        relBuilder.push(right);
        relBuilder.join(JoinRelType.INNER, joinCond);
        RelNode join = relBuilder.build();
        relBuilder.push(join);

        List<RexNode> topProjects = Lists.newArrayList();
        for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
            topProjects.add(rexBuilder.makeInputRef(join, i + pkskList.size()));
        }
        relBuilder.project(topProjects);
        return sort.copy(sort.getTraitSet(), relBuilder.build(), sort.getCollation(), null, sort.fetch);
    }

    public static String getIndexName(TableScan scan) {
        Optional<String> indexName;
        // get indexMeta of paging_force index
        indexName = IndexUtil.getPagingForceIndex(scan.getIndexNode()).stream().findFirst();
        if (!indexName.isPresent()) {
            return null;
        }
        return SQLUtils.normalizeNoTrim(indexName.get().toLowerCase());
    }

    private List<Integer> getPkList(TableMeta tm, Map<String, Integer> columnOrd) {
        // build pk list
        List<Integer> pkList = Lists.newArrayList();
        for (ColumnMeta columnMeta : tm.getPrimaryKey()) {
            int loc = columnOrd.getOrDefault(columnMeta.getName().toLowerCase(), -1);
            if (loc < 0) {
                return null;
            }
            pkList.add(loc);
        }

        // must have pk
        if (pkList.isEmpty()) {
            return null;
        }
        return pkList;
    }

    /**
     * Get the nullable state list of Primary Key and Shard Key.
     * A nullable column can be not-nullable when applying some conditions.
     *
     * @param tm table of PKSK
     * @param pkList Primary Key list
     * @param skList Shard Key list
     * @param condition condition applied
     * @param plannerContext context for parameter
     * @return nullable state list PkSK list, true if nullable
     */
    private List<Boolean> getPkSKNullableList(TableMeta tm, List<Integer> pkList, List<Integer> skList,
                                              RexNode condition, PlannerContext plannerContext) {
        ImmutableList.Builder<Boolean> builder = ImmutableList.builder();
        // pk are not nullable
        for (int i = 0; i < pkList.size(); i++) {
            builder.add(false);
        }

        // bit is set when the column is not nullable
        BitSet notNullableBitSet = getNotNullableBitSet(condition, plannerContext);
        for (Integer sk : skList) {
            if (tm.getAllColumns().get(sk).isNullable() && (!notNullableBitSet.get(sk))) {
                builder.add(true);
                continue;
            }
            builder.add(false);
        }
        return builder.build();
    }

    /**
     * Get the bitset of not-nullable columns under given condition.
     * A column must be not-nullable if the corresponding bit is set.
     *
     * @param rexNode given condition
     * @param plannerContext context for parameter
     * @return bitset of not-nullable columns. bit is set if not-nullable
     */
    BitSet getNotNullableBitSet(RexNode rexNode, PlannerContext plannerContext) {
        final BitSet[] bitSet = {new BitSet()};
        if (!(rexNode instanceof RexCall)) {
            return bitSet[0];
        }
        ForceIndexUtil.SargAbleHandler sargAbleHandler = new ForceIndexUtil.SargAbleHandler(plannerContext) {
            @Override
            protected void handleUnaryRef(RexInputRef ref, RexCall call) {
            }

            @Override
            protected void handleBinaryRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                int inputRef = leftRef.getIndex();
                Object value = DrdsRexFolder.fold(rightRex, plannerContext);
                if (value != null && inputRef >= 0) {
                    bitSet[0].set(inputRef);
                }
            }

            @Override
            protected void handleTernaryRef(RexInputRef firstRex, RexNode leftRex, RexNode rightRex, RexCall call) {
                int inputRef = firstRex.getIndex();
                Object value = DrdsRexFolder.fold(leftRex, plannerContext);
                if (value != null && inputRef >= 0) {
                    value = DrdsRexFolder.fold(rightRex, plannerContext);
                    if (value != null ) {
                        bitSet[0].set(inputRef);
                    }
                    bitSet[0].set(inputRef);
                }
            }

            @Override
            protected void handleDefault(RexCall call) {
                List<RexNode> ops = call.getOperands();
                switch (call.getKind()) {
                case AND:
                    // not-nullable if any branch is not-nullable
                    bitSet[0] = getNotNullableBitSet(ops.get(0), plannerContext);
                    for (int i = 1; i < ops.size(); i++) {
                        bitSet[0].or(getNotNullableBitSet(ops.get(i), plannerContext));
                    }
                    return;
                case OR:
                    // not-nullable if and only if all branches are not-nullable
                    bitSet[0] = getNotNullableBitSet(ops.get(0), plannerContext);
                    for (int i = 1; i < ops.size(); i++) {
                        bitSet[0].and(getNotNullableBitSet(ops.get(i), plannerContext));
                    }
                    return;
                case IS_NOT_NULL:
                    // ref is not null
                    if (ops.size() == 1 && ops.get(0) instanceof RexInputRef) {
                        int inputRef = ((RexInputRef) ops.get(0)).getIndex();
                        if (inputRef >= 0) {
                            bitSet[0].set(inputRef);
                        }
                    }
                    return;
                case NOT:
                    if (ops.size() != 1) {
                        return;
                    }
                    bitSet[0] = getNotNullableBitSet(ops.get(0), plannerContext);
                    return;
                default:
                }
            }

            @Override
            protected void handleInVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                Object value = DrdsRexFolder.fold(rightRex, plannerContext);
                if (value != null) {
                    List<RexNode> inRefs = leftRex.getOperands();
                    for (RexNode inRef : inRefs) {
                        if (inRef instanceof RexInputRef && ((RexInputRef) inRef).getIndex() > 0) {
                            bitSet[0].set(((RexInputRef) inRef).getIndex());
                        }
                    }
                }
            }

            @Override
            protected void handleEqualVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                Object value = DrdsRexFolder.fold(rightRex, plannerContext);
                if (value != null) {
                    List<RexNode> inRefs = leftRex.getOperands();
                    for (RexNode inRef : inRefs) {
                        if (inRef instanceof RexInputRef && ((RexInputRef) inRef).getIndex() > 0) {
                            bitSet[0].set(((RexInputRef) inRef).getIndex());
                        }
                    }
                }
            }
        };
        sargAbleHandler.handleCondition(rexNode);
        return bitSet[0];
    }
}