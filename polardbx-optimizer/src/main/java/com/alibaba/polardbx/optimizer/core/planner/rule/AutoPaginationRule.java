package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil.SargAbleHandler;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.core.planner.rule.PagingForceToJoinRule.FILTER_NO_SUBQUERY;
import static com.alibaba.polardbx.optimizer.core.planner.rule.PagingForceToJoinRule.PROJECT_NO_SUBQUERY;

public class AutoPaginationRule extends RelOptRule {

    static final Predicate<LogicalSort> ORDER_BY_LIMIT = new PredicateImpl<LogicalSort>() {
        @Override
        public boolean test(LogicalSort logicalSort) {
            return logicalSort != null && logicalSort.fetch != null && logicalSort.withOrderBy();
        }
    };

    public static final AutoPaginationRule INSTANCE = new AutoPaginationRule(
        operand(LogicalSort.class, null, ORDER_BY_LIMIT,
            operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY,
                operand(TableScan.class, null, none()))),
        RelFactories.LOGICAL_BUILDER, "INSTANCE"
    );

    public static final AutoPaginationRule PROJECT = new AutoPaginationRule(
        operand(LogicalSort.class, null, ORDER_BY_LIMIT,
            operand(LogicalProject.class, null, PROJECT_NO_SUBQUERY,
                operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY,
                    operand(TableScan.class, null, none())))),
        RelFactories.LOGICAL_BUILDER, "PROJECT"
    );

    public AutoPaginationRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                              String description) {
        super(operand, relBuilderFactory, "AutoPaginationRule" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_INDEX);
    }

    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == PROJECT) {
            LogicalSort sort = call.rel(0);
            LogicalProject project = call.rel(1);
            LogicalFilter filter = call.rel(2);
            TableScan scan = call.rel(3);
            SqlNode indexNode = genIndexNodeForRel(sort, project, filter, scan);
            if (indexNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                LogicalTableScan newTableScan =
                    LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                        indexNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
                LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newTableScan, filter.getCondition());
                LogicalProject newProject = project.copy(project.getTraitSet(), newFilter, project.getProjects(),
                    project.getRowType());
                LogicalSort newSort =
                    sort.copy(sort.getTraitSet(), newProject, sort.getCollation(), sort.offset, sort.fetch);
                call.transformTo(newSort);
            }
        } else {
            LogicalSort sort = call.rel(0);
            LogicalFilter filter = call.rel(1);
            TableScan scan = call.rel(2);
            SqlNode indexNode = genIndexNodeForRel(sort, filter, scan);
            if (indexNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                LogicalTableScan newTableScan =
                    LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                        indexNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
                LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newTableScan, filter.getCondition());
                LogicalSort newSort =
                    sort.copy(sort.getTraitSet(), newFilter, sort.getCollation(), sort.offset, sort.fetch);
                call.transformTo(newSort);
            }
        }
    }

    private SqlNode genIndexNodeForRel(LogicalSort sort, LogicalProject project,
                                       LogicalFilter filter, TableScan scan) {
        ImmutableBitSet.Builder usedColBuilder = ImmutableBitSet.builder();
        usedColBuilder.addAll(RelOptUtil.InputFinder.bits(filter.getCondition()));
        final Mappings.TargetMapping map = RelOptUtil.permutation(project.getProjects(), filter.getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return null;
            }
            usedColBuilder.set(map.getTargetOpt(fc.getFieldIndex()));
        }
        RelCollation newCollation = sort.getCluster().traitSet().canonize(RexUtil.apply(map, sort.getCollation()));

        ImmutableBitSet.Builder outputColBuilder = ImmutableBitSet.builder();
        for (RexNode rex : project.getProjects()) {
            outputColBuilder.addAll(RelOptUtil.InputFinder.bits(rex));
        }
        return genIndexNode(newCollation, filter, usedColBuilder.build(), outputColBuilder.build(), scan);
    }

    private SqlNode genIndexNodeForRel(LogicalSort sort, LogicalFilter filter, TableScan scan) {
        ImmutableBitSet.Builder usedColBuilder = ImmutableBitSet.builder();
        usedColBuilder.addAll(RelOptUtil.InputFinder.bits(filter.getCondition()));
        for (int key : sort.getCollation().getKeys()) {
            usedColBuilder.set(key);
        }
        return genIndexNode(sort.getCollation(), filter, usedColBuilder.build(),
            ImmutableBitSet.range(sort.getRowType().getFieldCount()), scan);
    }

    /**
     * Generates an index node based on the given sorting information, filter condition, used columns, and table scan.
     * This method aims to select the most appropriate index for query optimization.
     *
     * @param sortCollation Sorting information, used to determine if the index matches the sorting.
     * @param filter Filter condition, used to determine if the index can be utilized.
     * @param usedCols Columns used in the query for filter and sort, used to evaluate index usability.
     * @param outputCols Columns output in the query, used to evaluate index usability.
     * @param scan Table scan information, containing details about the table and possible indexes.
     * @return Returns the generated index node, which could be a force index, ignore index, or paging force index.
     */
    private SqlNode genIndexNode(RelCollation sortCollation, LogicalFilter filter,
                                 ImmutableBitSet usedCols, ImmutableBitSet outputCols, TableScan scan) {
        if (CollectionUtils.isEmpty(sortCollation.getFieldCollations())) {
            return null;
        }
        // Attempt to use the force index specified by the user, if any.
        SqlNode node = scan.getIndexNode();
        // if force index is used, try to convert it to paging force
        if (node != null) {
            return genPagingForceIndexNode(node, sortCollation, filter, usedCols, outputCols, scan);
        }

        // Auto select force index when user did not specify a force index.
        node = genForceIndexNode(sortCollation, filter, usedCols, scan);
        // try to auto ignore index if auto force index failed
        if (node == null) {
            return genIgnoreIndexNode(sortCollation, filter, scan);
        }
        // try to auto paging_force index if auto force index succeed
        SqlNode pagingNode = genPagingForceIndexNode(node, sortCollation, filter, usedCols, outputCols, scan);
        return pagingNode != null ? pagingNode : node;
    }

    /**
     * Generates a SqlNode object for a force index hint based on the given conditions.
     * This method decides whether to force the use of a specific index for a query based on the sort order,
     * filter conditions, and columns used in the query.
     *
     * @param sortCollation The sort information of the query.
     * @param filter The filter conditions of the query.
     * @param usedCols The columns used in the query.
     * @param scan The table scan node.
     * @return Returns the SqlNode object for the force index hint, or null if no suitable index is found.
     */
    SqlNode genForceIndexNode(RelCollation sortCollation, LogicalFilter filter,
                              ImmutableBitSet usedCols, TableScan scan) {
        // If the table scan already has an index hint, no further processing is needed.
        if (ForceIndexUtil.hasIndexHint(scan)) {
            return null;
        }
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        // Build a map of column ordinals.
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        // Get the shard key column ordinals.
        List<Integer> skList = ForceIndexUtil.getSkList(tm, columnOrd);
        ImmutableBitSet skBitSet = skList == null ? ImmutableBitSet.of() : ImmutableBitSet.of(skList);
        // Initialize the best index for skipping sort.
        ForceIndexUtil.BestIndex bestSkipSortIndex = new ForceIndexUtil.BestIndex(null, -1, -1D, false);
        // Initialize the best index for sort.
        ForceIndexUtil.BestIndex bestSortIndex = new ForceIndexUtil.BestIndex(null, -1, -1D, false);
        // Iterate through all indexes of the table to find the best index.
        for (IndexMeta indexMeta : tm.getIndexes()) {
            // Get the list of key column ordinals for the current index.
            List<Integer> indexColumns = keyColumnOrdList(columnOrd, tm, indexMeta);
            if (CollectionUtils.isEmpty(indexColumns)) {
                continue;
            }
            // If the number of index columns exceeds the maximum limit, skip the current index.
            if (indexColumns.size() >= ForceIndexUtil.INDEX_MAX_LEN) {
                continue;
            }
            ImmutableBitSet indexBitSet = ImmutableBitSet.of(indexColumns);
            // Only covering index is considered
            if (!indexBitSet.contains(usedCols)) {
                continue;
            }
            // Check if the current index covers the shard keys.
            boolean coverSk = indexBitSet.contains(skBitSet);
            // If the current index can skip the sort, update the best index for skipping sort.
            if (testIfSkipSortOrder(columnOrd, indexMeta, sortCollation, filter, scan, tm)) {
                int startLoc = sortLocInIndex(indexColumns, sortCollation.getKeys());
                bestSkipSortIndex = bestSkipSortIndex.findBetterIndex(indexMeta, startLoc,
                    calcCardinality(startLoc, indexColumns, tm), coverSk);
            } else {
                // If the current index cannot skip the sort, update the best index for sort.
                Set<Integer> equalColumnsSet = equalColumnsSetForFilter(columnOrd, filter, scan);
                int loc;
                for (loc = 0; loc < indexColumns.size(); loc++) {
                    if (!equalColumnsSet.contains(indexColumns.get(loc))) {
                        break;
                    }
                }
                bestSortIndex = bestSortIndex.findBetterIndex(indexMeta, loc,
                    calcCardinality(loc, indexColumns, tm), coverSk);
            }
        }

        // If a suitable index for skipping sort is found, generate and return the corresponding SqlNode object.
        // prefer skip sort index
        if (bestSkipSortIndex.getIndexMeta() != null
            && bestSkipSortIndex.getEqPreLen() >= PlannerContext.getPlannerContext(scan).getParamManager()
            .getInt(ConnectionParams.SKIP_SORT_EQ_PRE_COL)) {
            return ForceIndexUtil.genForceSqlNode(bestSkipSortIndex.getIndexMeta().getPhysicalIndexName());
        }

        // If a suitable index for sort is found, generate and return the corresponding SqlNode object.
        if (bestSortIndex.getIndexMeta() != null
            && bestSortIndex.getEqPreLen() >= PlannerContext.getPlannerContext(scan).getParamManager()
            .getInt(ConnectionParams.SORT_EQ_PRE_COL)) {
            return ForceIndexUtil.genForceSqlNode(bestSortIndex.getIndexMeta().getPhysicalIndexName());
        }
        // If no suitable index is found, return null.
        return null;
    }

    SqlNode genPagingForceIndexNode(SqlNode node, RelCollation sortCollation, LogicalFilter filter,
                                    ImmutableBitSet usedCols, ImmutableBitSet outputCols, TableScan scan) {
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        if (!PlannerContext.getPlannerContext(scan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_PAGING_FORCE)) {
            return null;
        }
        Optional<String> indexNames = IndexUtil.getForceIndex(node).stream().findFirst();
        String indexName = indexNames.map(s -> SQLUtils.normalizeNoTrim(s.toLowerCase())).orElse(null);
        if (StringUtils.isEmpty(indexName)) {
            return null;
        }
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        IndexMeta indexMeta = tm.getIndexMeta(indexName);
        if (indexMeta == null) {
            return null;
        }

        // Only covering index is considered
        List<Integer> indexColumns = keyColumnOrdList(columnOrd, tm, indexMeta);
        ImmutableBitSet indexBitSet = ImmutableBitSet.of(indexColumns);
        if (CollectionUtils.isEmpty(indexColumns) || !indexBitSet.contains(usedCols)
            || indexBitSet.contains(outputCols)) {
            return null;
        }
        if (!testIfSkipSortOrder(columnOrd, indexMeta, sortCollation, filter, scan, tm)) {
            return ForceIndexUtil.genPagingForceSqlNode(indexName);
        }

        // can't skip sort order, check reverse ref scan
        if (sortDirection(indexMeta, sortCollation) == Direction.ASCENDING) {
            return null;
        }
        ImmutableBitSet filterCols = RelOptUtil.InputFinder.bits(filter.getCondition());
        ImmutableIntList sortCol = sortCollation.getKeys();
        if (sortCol.isEmpty()) {
            return null;
        }
        if (!filterCols.get(sortCol.get(0))) {
            return ForceIndexUtil.genPagingForceSqlNode(indexName);
        }
        return null;
    }

    SqlNode genIgnoreIndexNode(RelCollation sortCollation, LogicalFilter filter, TableScan scan) {
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        PlannerContext plannerContext = PlannerContext.getPlannerContext(scan);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_IGNORE_INDEX)) {
            return null;
        }

        List<RexNode> sargs = Lists.newArrayList();
        SargAbleHandler sargAbleHandler = new SargAbleHandler(plannerContext) {
            @Override
            protected void handleUnaryRef(RexInputRef ref, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleBinaryRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleTernaryRef(RexInputRef firstRex, RexNode leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleInVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            protected void handleEqualVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            protected void handleNotEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                // do nothing
            }
        };

        for (RexNode cond : RelOptUtil.conjunctions(filter.getCondition())) {
            sargAbleHandler.handleCondition(cond);
        }
        ImmutableBitSet.Builder sargBitsBuilder = ImmutableBitSet.builder();
        for (RexNode sarg : sargs) {
            sargBitsBuilder.addAll(RelOptUtil.InputFinder.bits(sarg));
        }
        ImmutableBitSet sargBits = sargBitsBuilder.build();
        ImmutableBitSet filterColumnBits = RelOptUtil.InputFinder.bits(filter.getCondition());
        Set<String> ignoreIndexSet = Sets.newHashSet();
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        boolean useAbleIndex = false;
        // ignore index starting with sort column if there are any other index usable
        for (IndexMeta indexMeta : tm.getIndexes()) {
            List<Integer> indexColumns = keyColumnOrdList(columnOrd, tm, indexMeta);
            if (CollectionUtils.isEmpty(indexColumns)) {
                continue;
            }
            int startLoc = sortLocInIndex(indexColumns, sortCollation.getKeys());
            if (startLoc == 0 && !filterColumnBits.get(indexColumns.get(0))) {
                ignoreIndexSet.add(indexMeta.getPhysicalIndexName());
                continue;
            }
            if (sargBits.get(indexColumns.get(0))) {
                useAbleIndex = true;
            }
        }
        if (useAbleIndex && !CollectionUtils.isEmpty(ignoreIndexSet)) {
            return ForceIndexUtil.genIgnoreSqlNode(ignoreIndexSet);
        }
        return null;
    }

    /**
     * Determines if the sorting order can be skipped based on specific conditions.
     * The test returns true doesn't mean the index can skip sort necessarily,
     * but false means the index can't skip sort definitely.
     *
     * @param columnOrd Map of column names to their ordinal positions, used to obtain column positions.
     * @param indexMeta Index metadata, containing information about the index.
     * @param sortCollation Collation of Logical sort object.
     * @param filter Logical filter object, representing the filtering operation.
     * @param scan Table scan object, representing the table scanning operation.
     * @return true if the sorting order can be skipped; otherwise, false.
     */
    protected boolean testIfSkipSortOrder(Map<String, Integer> columnOrd,
                                          IndexMeta indexMeta, RelCollation sortCollation,
                                          LogicalFilter filter, TableScan scan, TableMeta tm) {
        if (sortCollation == null) {
            return false;
        }
        // Get the sort column list
        ImmutableIntList sortCol = sortCollation.getKeys();
        // If the sort column list is empty, return true
        if (sortCol.isEmpty()) {
            return true;
        }
        if (sortDirection(indexMeta, sortCollation) == null) {
            return false;
        }

        List<Integer> indexColumns = keyColumnOrdList(columnOrd, tm, indexMeta);
        if (indexColumns == null) {
            return false;
        }
        int startLoc = sortLocInIndex(indexColumns, sortCol);
        if (startLoc < 0) {
            return false;
        }
        // check prev columns are equality
        Set<Integer> equalColumnsSet = equalColumnsSetForFilter(columnOrd, filter, scan);
        // Check if the columns before the start location are all in the equal columns set
        for (int i = 0; i < startLoc; i++) {
            if (!equalColumnsSet.contains(indexColumns.get(i))) {
                return false;
            }
        }

        // If all checks pass, return true
        return true;
    }

    private int sortLocInIndex(List<Integer> indexColumns, ImmutableIntList sortCol) {
        // Find the starting position of the sort column in the index columns
        int startLoc = indexColumns.indexOf(sortCol.get(0));
        // If the starting position is not found, return -1
        if (startLoc < 0) {
            return startLoc;
        }
        // check sort columns are continuous in index
        for (int i = 1, indexLoc = startLoc; i < sortCol.size(); i++, indexLoc++) {
            // If the index column list is shorter than the current index location, return -1
            if (indexColumns.size() <= indexLoc) {
                return -1;
            }
            // If the current sort column does not match the index column at the current index location, return -1
            if (!Objects.equals(indexColumns.get(indexLoc), sortCol.get(i))) {
                return -1;
            }
        }
        return startLoc;
    }

    private Direction sortDirection(IndexMeta indexMeta, RelCollation sortCollation) {
        Direction direction = null;
        for (RelFieldCollation relFieldCollation : sortCollation.getFieldCollations()) {
            if (direction == null) {
                direction = relFieldCollation.direction;
                continue;
            }
            if (direction.isDescending() != relFieldCollation.direction.isDescending()) {
                return null;
            }
        }
        // todo: support index direction
        return direction;
    }

    private List<Integer> keyColumnOrdList(Map<String, Integer> columnOrd, TableMeta tm, IndexMeta indexMeta) {
        // Initialize the list of index columns
        List<Integer> indexColumns = Lists.newArrayList();
        // Populate the indexColumns list with the ordinal positions of the index columns
        for (IndexColumnMeta indexColumnMeta : indexMeta.getKeyColumnsExt()) {
            if (indexColumnMeta.getSubPart() != 0) {
                return null;
            }
            Integer ord = columnOrd.getOrDefault(indexColumnMeta.getColumnMeta().getName().toLowerCase(), -1);
            if (ord < 0) {
                return null;
            }
            indexColumns.add(ord);
        }
        if (tm.isHasPrimaryKey()) {
            for (ColumnMeta columnMeta : tm.getPrimaryIndex().getKeyColumns()) {
                Integer ord = columnOrd.getOrDefault(columnMeta.getName().toLowerCase(), -1);
                if (ord < 0) {
                    return null;
                }
                if (!indexColumns.contains(ord)) {
                    indexColumns.add(ord);
                }
            }
        }
        return indexColumns;
    }

    private Set<Integer> equalColumnsSetForFilter(Map<String, Integer> columnOrd, LogicalFilter filter,
                                                  TableScan scan) {
        ForceIndexUtil.RequiredUniqueColumnsCollector equalColumnsCollector =
            new ForceIndexUtil.RequiredUniqueColumnsCollector(
                scan, scan.getCluster().getMetadataQuery(), null);
        Set<Integer> equalColumnsSet = Sets.newHashSet();
        // Collect columns that are constant
        filter.accept(equalColumnsCollector);
        for (List<String> equalColumns : equalColumnsCollector.getEqualColumnsList()) {
            for (String equalColumn : equalColumns) {
                equalColumnsSet.add(columnOrd.getOrDefault(equalColumn.toLowerCase(), -1));
            }
        }
        return equalColumnsSet;
    }

    private double calcCardinality(int startLoc, List<Integer> indexColumns, TableMeta tm) {
        double cardinality = 1D;
        for (int i = 0; i < startLoc; i++) {
            StatisticResult statisticResult1 =
                StatisticManager.getInstance().getCardinality(tm.getSchemaName(), tm.getTableName(),
                    tm.getAllColumns().get(indexColumns.get(i)).getName(), true, true);
            cardinality *= statisticResult1.getLongValue();
        }
        return cardinality;
    }
}
