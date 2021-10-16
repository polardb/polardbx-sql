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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author shengyu
 * This is a rule trying to mock mysql join reroder.
 * It is based on mysql 5.7, the entry point is
 * MYSQL_SERVER/sql/sql_planner.cc "Optimize_table_order::choose_table_order()"
 * Many functions should be tuned to behave more like mysql
 */
public class MysqlMultiJoinToLogicalJoinRule extends LoptOptimizeJoinRule {

    protected final Logger logger = LoggerFactory.getLogger(MysqlMultiJoinToLogicalJoinRule.class);

    public static final MysqlMultiJoinToLogicalJoinRule INSTANCE =
        new MysqlMultiJoinToLogicalJoinRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a MysqlMultiJoinToLogicalJoinRule.
     */
    public MysqlMultiJoinToLogicalJoinRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);
        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
        final RelMetadataQuery mq = call.getMetadataQuery();
        //we sort the tables for further pruning
        List<Integer> sortedList = sortTables(mq, multiJoin);
        greedySearch(
            call.builder(),
            multiJoin,
            call,
            sortedList);
    }

    /**
     * sort all factors
     * - table before another table that depends on it (outer join), then
     * - table with smallest number of records first, then
     * - the table with lowest-value pointer (i.e., the one located
     * in the lowest memory address) first.
     *
     * @param multiJoin the multiJoin considered
     * @return the sorted index
     */
    private List<Integer> sortTables(RelMetadataQuery mq, LoptMultiJoin multiJoin) {
        List<Integer> ans = new ArrayList<>(multiJoin.getNumJoinFactors());
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            ans.add(i);
        }
        ans.sort((o1, o2) -> {
            //for outer join, the outer part be behind the inner part
            if (multiJoin.isNullGenerating(o1)
                && multiJoin.getOuterJoinFactors(o1).get(o2)) {
                return 1;
            }
            if (multiJoin.isNullGenerating(o2)
                && multiJoin.getOuterJoinFactors(o2).get(o1)) {
                return -1;
            }

            if (isMysqlJoinReorderFirstTable(multiJoin.getJoinFactor(o1))) {
                return -1;
            } else if (isMysqlJoinReorderFirstTable(multiJoin.getJoinFactor(o2))) {
                return 1;
            }

            //compare row count
            Double rc1 = mq.getRowCount(multiJoin.getJoinFactor(o1));
            Double rc2 = mq.getRowCount(multiJoin.getJoinFactor(o2));
            if (Math.abs(rc1 - rc2) > RelOptUtil.EPSILON) {
                return rc1.compareTo(rc2);
            }
            //compare index
            return o1.compareTo(o2);
        });
        return ans;
    }

    private boolean isMysqlJoinReorderFirstTable(RelNode rel) {
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        Set<RexTableInputRef.RelTableRef> tableRefs = mq.getTableReferences(rel);
        if (tableRefs != null && tableRefs.size() == 1) {
            if (PlannerContext.getPlannerContext(rel).getMysqlJoinReorderFirstTable()
                == tableRefs.iterator().next().getTable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * use greedy search to find a plan
     * see Mysql Optimize_table_order::greedy_search
     */
    private void greedySearch(
        RelBuilder relBuilder,
        LoptMultiJoin multiJoin,
        RelOptRuleCall call,
        List<Integer> sortedIndexes) {

        // the limited depth of a search
        int exhaustiveDepth = PlannerContext.getPlannerContext(call).getParamManager()
            .getInt(ConnectionParams.MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH);

        final List<String> fieldNames =
            multiJoin.getMultiJoinRel().getRowType().getFieldNames();

        final int nJoinFactors = multiJoin.getNumJoinFactors();
        final BitSet factorsAdded = new BitSet(nJoinFactors);
        final List<RexNode> filtersToAdd =
            new ArrayList<>(multiJoin.getJoinFilters());

        LoptJoinTree joinTree = null;
        SearchCtx searchCtx = null;
        int idx;
        //once add a factor
        RelMetadataQuery mq = call.getMetadataQuery();
        for (idx = 0; idx <= Math.max(0, sortedIndexes.size() - exhaustiveDepth); idx++) {
            //prevent memory leak
            call.rel(0).getCluster().invalidateMetadataQuery();
            mq = call.getMetadataQuery();

            // sortedIndexes[0]..sortedIndexes[idx-1] are ordered factors
            // sortedIndexes[idx]..sortedIndexes[numOfFactor] are the un-chosen factors
            // the best factor would be placed at sortedIndexes[idx]
            searchCtx = new SearchCtx(
                idx,
                sortedIndexes,
                exhaustiveDepth,
                factorsAdded);
            //exhaustive search for a best partial plan
            bestExtensionByLimitedSearch(
                mq,
                relBuilder,
                multiJoin,
                searchCtx,
                joinTree,
                filtersToAdd);

            // add the head of the best partial plan to the current tree
            joinTree = addToTop(
                mq,
                relBuilder,
                multiJoin,
                joinTree,
                searchCtx.getBestPosition(idx),
                filtersToAdd);

            //move the factor to sortedIndexes[idx]
            int endFactor = searchCtx.getBestPosition(idx);
            int prevIndex = searchCtx.getBestPosition(idx);
            for (int i = idx; i < nJoinFactors; i++) {
                int newIndex = sortedIndexes.get(i);
                sortedIndexes.set(i, prevIndex);
                prevIndex = newIndex;
                if (prevIndex == endFactor) {
                    break;
                }
            }
            factorsAdded.set(endFactor);
        }

        //add the remaining factors to the join tree
        for (; idx < nJoinFactors; idx++) {
            joinTree = addToTop(
                mq,
                relBuilder,
                multiJoin,
                joinTree,
                searchCtx.getBestPosition(idx),
                filtersToAdd
            );
        }
        Set<RelNode> baseNodes = new HashSet<>();
        for (int j = 0; j < multiJoin.getNumJoinFactors(); j++) {
            baseNodes.add(multiJoin.getJoinFactor(j));
        }
        RelNode newProject =
            createTopProject(call.builder(), multiJoin, joinTree, fieldNames, baseNodes);
        call.transformTo(newProject);
    }

    /**
     * Find a good, possibly optimal, query execution plan (QEP) by a possibly
     * exhaustive search.
     *
     * @param searchCtx context of the search
     * @param joinTree the join tree we have now
     * @param filtersToAdd filters not used
     * The final optimal plan is stored in 'searchCtx.bestPositions'. The
     * corresponding cost of the optimal plan is in 'searchCtx.minCost'.
     * see mysql Optimize_table_order::best_extension_by_limited_search
     */
    private void bestExtensionByLimitedSearch(
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        LoptMultiJoin multiJoin,
        SearchCtx searchCtx,
        LoptJoinTree joinTree,
        List<RexNode> filtersToAdd) {
        //record a better partial join tree
        if (searchCtx.searchEnd()) {

            considerPlan(searchCtx);
        }
        //have more steps to go
        if (searchCtx.getLeftDepth() >= 1) {
            BitSet eqRefExtended = new BitSet(multiJoin.getNumJoinFactors());
            for (int id = searchCtx.getIdx(); id < searchCtx.getLen(); id++) {
                int factorToAdd = searchCtx.getPosition(id);

                //make sure that an outer join's dependency has been calculated
                if (multiJoin.isNullGenerating(factorToAdd)
                    && !BitSets.contains(searchCtx.getFactorsAdded(),
                    multiJoin.getOuterJoinFactors(factorToAdd))) {
                    continue;
                }
                //prune the factor if it has been extended by the equal join extension
                if (eqRefExtended.get(factorToAdd)) {
                    continue;
                }
                boolean prunedFlag = false;
                List<RexNode> newFilters = Lists.newArrayList(filtersToAdd);
                LoptJoinTree newTree = addToTop(
                    mq,
                    relBuilder,
                    multiJoin,
                    joinTree,
                    factorToAdd,
                    newFilters);

                // find the best way to access the new factor
                AccessPathInfo accessPathInfo = bestAccessPath(mq, newTree);
                // calculate the accumulative cost of the partial plan
                Double prefixCost = setPrefixJoinCost(mq, newTree, accessPathInfo.readCost, searchCtx);

                // if we already have a bigger cost, we can prune it
                if (!searchCtx.isBt(prefixCost)) {
                    prunedFlag = true;
                }

                searchCtx.printCur(multiJoin, factorToAdd, prefixCost, accessPathInfo);
                if (!prunedFlag) {
                    searchCtx.forward(id, prefixCost, accessPathInfo);
                    //consider equal join first
                    if (searchCtx.getIndex() != null && searchCtx.checkJoinFanOut()) {
                        eqRefExtended = eqRefExtensionByLimitedSearch(
                            mq,
                            relBuilder,
                            multiJoin,
                            searchCtx,
                            newTree,
                            newFilters);
                        eqRefExtended.set(factorToAdd);
                        /*bestExtensionByLimitedSearch(
                            mq,
                            relBuilder,
                            multiJoin,
                            searchCtx,
                            newTree,
                            newFilters);*/
                    } else {
                        bestExtensionByLimitedSearch(
                            mq,
                            relBuilder,
                            multiJoin,
                            searchCtx,
                            newTree,
                            newFilters);
                    }
                    // we don't consider any other order, if we find mysqlJoinReorderFirstTable
                    if (accessPathInfo.readCost == 0.0) {
                        return;
                    }
                    searchCtx.back(id);
                }
            }
        }
    }

    /**
     * Heuristic utility used by best_extension_by_limited_search().
     * Adds EQ_REF-joined tables to the partial plan without
     * extensive 'greedy' cost calculation.
     *
     * @param searchCtx context of the search
     * @param joinTree the join tree we have now
     * @param filtersToAdd filters not used
     * @return the bit map of all factors extended by eq reference
     * <p>
     * see mysql Optimize_table_order::eq_ref_extension_by_limited_search
     */
    BitSet eqRefExtensionByLimitedSearch(
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        LoptMultiJoin multiJoin,
        SearchCtx searchCtx,
        LoptJoinTree joinTree,
        List<RexNode> filtersToAdd) {
        //record a better partial join tree
        if (searchCtx.searchEnd()) {
            considerPlan(searchCtx);
            return new BitSet(multiJoin.getNumJoinFactors());
        }
        //have more steps to go
        if (searchCtx.getLeftDepth() >= 1) {
            for (int id = searchCtx.getIdx(); id < searchCtx.getLen(); id++) {
                int factorToAdd = searchCtx.getPosition(id);

                //make sure that an outer join's dependency has been calculated
                if (multiJoin.isNullGenerating(factorToAdd)
                    && !BitSets.contains(searchCtx.getFactorsAdded(),
                    multiJoin.getOuterJoinFactors(factorToAdd))) {
                    continue;
                }

                boolean prunedFlag = false;
                List<RexNode> newFilters = Lists.newArrayList(filtersToAdd);
                LoptJoinTree newTree = addToTop(
                    mq,
                    relBuilder,
                    multiJoin,
                    joinTree,
                    factorToAdd,
                    newFilters);

                // find the best way to access the new factor
                AccessPathInfo accessPathInfo = bestAccessPath(mq, newTree);
                // calculate the accumulative cost of the partial plan
                Double prefixCost = setPrefixJoinCost(mq, newTree, accessPathInfo.readCost, searchCtx);

                // if we already have a bigger cost, we can prune it
                if (!searchCtx.isBt(prefixCost)) {
                    prunedFlag = true;
                }

                searchCtx.printCur(multiJoin, factorToAdd, prefixCost, accessPathInfo);
                final boolean addedToEqRefExtension =
                    searchCtx.getIndex() != null &&
                        almostEqual(searchCtx.getReadCost(), accessPathInfo.readCost) &&
                        almostEqual(searchCtx.getRowFetched(), accessPathInfo.rowFetched);
                if (!prunedFlag && addedToEqRefExtension) {
                    searchCtx.forward(id, prefixCost, accessPathInfo);
                    //consider equal join first
                    BitSet eqRefExt = eqRefExtensionByLimitedSearch(
                        mq,
                        relBuilder,
                        multiJoin,
                        searchCtx,
                        newTree,
                        newFilters);
                    searchCtx.back(id);
                    eqRefExt.set(factorToAdd);
                    return eqRefExt;
                }
            }
        }
        bestExtensionByLimitedSearch(
            mq,
            relBuilder,
            multiJoin,
            searchCtx,
            joinTree,
            filtersToAdd);
        return new BitSet(multiJoin.getNumJoinFactors());
    }

    static private boolean almostEqual(Double left, Double right) {
        final double boundary = 0.1;
        return (left >= right * (1.0 - boundary)) && (left <= right * (1.0 + boundary));
    }

    /**
     * Cost calculation of another (partial-)QEP has been completed.
     *
     * @param searchCtx the context of the search
     * see mysql Optimize_table_order::consider_plan
     */
    private void considerPlan(SearchCtx searchCtx) {
        if (searchCtx.isBt(searchCtx.getCurCost())) {
            searchCtx.recordBetter();
        }
    }

    /**
     * Cost of processing a number of records and evaluating the query condition
     * on the records.
     *
     * @param rows number of rows to evaluate
     * @return Cost of evaluating the records
     * see mysql row_evaluate_cost
     */
    private double rowEvaluateCost(double rows) {
        if (rows < 0) {
            return 0;
        }
        return rows * 0.1;
    }

    /**
     * Set complete estimated cost and produced rowcount for the prefix of tables
     * up to and including this table, calculated from the cost of the previous
     * stage, the fanout of the current stage and the cost to process a row at
     * the current stage.
     *
     * @param joinTree the tree considered
     * @param readCost the cost of the current tree
     * @param searchCtx context of the search
     * @return the current cost
     * see mysql set_prefix_join_cost
     */
    private double setPrefixJoinCost(
        RelMetadataQuery mq,
        LoptJoinTree joinTree,
        double readCost,
        SearchCtx searchCtx) {

        // this is the first factor considered
        if (joinTree.getFactorTree() instanceof LoptJoinTree.Leaf) {
            return searchCtx.getCurCost() + readCost;
        }

        RelNode root = joinTree.getJoinTree();
        while ((root instanceof LogicalFilter || root instanceof LogicalProject)) {
            root = ((SingleRel) root).getInput();
        }

        return searchCtx.getCurCost() + readCost +
            rowEvaluateCost(mq.getRowCount(root));
    }

    /**
     * information about joining the factor to the partial tree
     */
    class AccessPathInfo {
        /**
         * index the join can use
         */
        Index index;
        /**
         * the read cost of the factor
         */
        Double readCost;
        /**
         * rows fetched
         */
        Double rowFetched;

        /**
         * the count of left rows
         */
        Double leftCount;
        /**
         * RelOptCost of the right side of the join
         */
        RelOptCost relCost;

        AccessPathInfo(
            Index index,
            Double readCost,
            Double joinedRow,
            Double leftRow,
            RelOptCost relCost) {
            this.index = index;
            this.readCost = readCost;
            this.relCost = relCost;
            this.leftCount = leftRow;
            //deal with null
            if (joinedRow == null || leftRow == null) {
                rowFetched = Double.MAX_VALUE;
                if (index != null) {
                    logger.error("If there is no information about the join, it can't have any index!");
                }
                return;
            }
            assert joinedRow >= 0D;
            assert leftRow >= 0D;

            //the left side is 0 and result is 0
            if (joinedRow < RelOptUtil.EPSILON && leftRow < RelOptUtil.EPSILON) {
                rowFetched = 1D;
                return;
            }
            //only left side is 0
            if (leftRow < RelOptUtil.EPSILON) {
                rowFetched = Double.MAX_VALUE;
                return;
            }
            //both sides are not 0
            rowFetched = joinedRow / leftRow;
        }
    }

    /**
     * Find the best access path for an extension of a partial execution
     * plan and add this path to the plan.
     * see Mysql Optimize_table_order::best_access_path
     */
    private AccessPathInfo bestAccessPath(
        RelMetadataQuery mq,
        LoptJoinTree joinTree) {
        if (joinTree == null) {
            return new AccessPathInfo(
                null,
                0D,
                null,
                null,
                null);
        }

        // this is the first factor considered
        if (joinTree.getFactorTree() instanceof LoptJoinTree.Leaf) {
            RelOptCost cost = mq.getCumulativeCost(joinTree.getJoinTree());
            if (isMysqlJoinReorderFirstTable(joinTree.getJoinTree())) {
                // if table is the MysqlJoinReorderFirstTable, make it zero-cost
                cost = joinTree.getJoinTree().getCluster().getPlanner().getCostFactory().makeZeroCost();
            }

            return new AccessPathInfo(
                null,
                cost.getCpu() + CostModelWeight.INSTANCE.getIoWeight() * cost.getIo(),
                null,
                null,
                cost);
        }

        // the join tree has at least two factors
        RelNode root = joinTree.getJoinTree();
        while ((root instanceof LogicalFilter || root instanceof LogicalProject)) {
            root = ((SingleRel) root).getInput();
        }
        if (!(root instanceof LogicalJoin)) {
            logger.error("it should be a logical join here, but a " + root.getClass());
            return new AccessPathInfo(
                null,
                0D,
                null,
                null,
                null);
        }
        LogicalJoin join = (LogicalJoin) root;
        Index index = findBestRef(join);
        Double prefixRowCount = mq.getRowCount(join.getLeft());
        if (index == null) {
            // FIXME
            RelOptCost cost = mq.getCumulativeCost(join.getRight());
            double readCost =
                prefixRowCount * cost.getCpu() + CostModelWeight.INSTANCE.getMemoryWeight() * cost.getMemory()
                    + CostModelWeight.INSTANCE.getIoWeight() * cost.getIo();
            return new AccessPathInfo(
                null,
                readCost,
                mq.getRowCount(join),
                prefixRowCount,
                cost);
        } else {
            RelOptCost cost = mq.getCumulativeCost(join.getRight());
            return new AccessPathInfo(
                index,
                prefixRowCount * index.getJoinSelectivity() * (cost.getCpu()
                    + CostModelWeight.INSTANCE.getMemoryWeight() * cost.getMemory()
                    + CostModelWeight.INSTANCE.getIoWeight() * cost.getIo()),
                mq.getRowCount(join),
                prefixRowCount,
                cost);
        }
    }

    /**
     * find the best access of the table
     * see mysql Optimize_table_order::find_best_ref
     */
    private Index findBestRef(LogicalJoin join) {
        return IndexUtil.selectJoinIndex(join);
    }

    /**
     * Generate a new join, left side is the origin join tree,
     * right side is the factor. It is possible to have filters
     * and projects on top of the join.
     *
     * @param mq the metadata
     * @param joinTree the join tree we have now
     * @param factorToAdd the factor to add
     * @param filtersToAdd filter unused
     * @return the new join tree generated
     */
    protected LoptJoinTree addToTop(
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        LoptMultiJoin multiJoin,
        LoptJoinTree joinTree,
        int factorToAdd,
        List<RexNode> filtersToAdd) {
        JoinRelType joinType;
        if (multiJoin.isNullGenerating(factorToAdd)) {
            joinType = JoinRelType.LEFT;
        } else {
            joinType = JoinRelType.INNER;
        }

        LoptJoinTree rightTree =
            new LoptJoinTree(
                multiJoin.getJoinFactor(factorToAdd),
                factorToAdd);
        if (joinTree == null) {
            return rightTree;
        }
        // in the case of a left or right outer join, use the specific
        // outer join condition
        RexNode condition;
        if (joinType == JoinRelType.LEFT) {
            condition = multiJoin.getOuterJoinCond(factorToAdd);
        } else {
            condition =
                addFilters(
                    multiJoin,
                    joinTree,
                    -1,
                    rightTree,
                    filtersToAdd,
                    false);
        }

        return createJoinSubtree(
            mq,
            relBuilder,
            multiJoin,
            joinTree,
            rightTree,
            condition,
            joinType,
            filtersToAdd,
            true,
            false);
    }

    class SearchCtx {
        /**
         * the next table to be added in an exhaustive search
         */
        private int idx;
        /**
         * record the ordered list of a join tree
         * from 0 to idx-1
         */
        final private List<Integer> positions;

        /**
         * the depth can be further used in an exhaustive search
         */
        private int leftDepth;
        /**
         * factors added in the partial tree
         */
        final private BitSet factorsAdded;
        /**
         * the cost of the current partial plan
         */
        final private LinkedList<Double> curCost;
        /**
         * the cost of the best partial tree
         */
        private Double minCost;
        /**
         * the list of the best positions
         */
        private List<Integer> bestPositions;
        /**
         * the index used
         */
        final private LinkedList<Index> index;
        /**
         * the read cost of adding new factor
         */
        final private LinkedList<Double> readCost;

        /**
         * rowFetched = rows remains after joining / rows from left side
         * used to determine whether the join is a 1:1 join
         */
        final private LinkedList<Double> rowFetched;

        SearchCtx(int idx,
                  List<Integer> positions,
                  int leftDepth,
                  BitSet factorsAdded) {
            assert leftDepth >= 1;
            this.positions = Lists.newArrayList(positions);
            this.idx = idx;
            this.leftDepth = leftDepth;
            this.factorsAdded = (BitSet) factorsAdded.clone();
            this.minCost = null;
            bestPositions = null;
            curCost = new LinkedList<>();
            index = new LinkedList<>();
            rowFetched = new LinkedList<>();
            readCost = new LinkedList<>();
        }

        public int getIdx() {
            return idx;
        }

        public int getLeftDepth() {
            return leftDepth;
        }

        /**
         * get the number of join factors in total
         */
        public int getLen() {
            return positions.size();
        }

        public List<Integer> getPositions() {
            return positions;
        }

        public int getPosition(int id) {
            return positions.get(id);
        }

        public BitSet getFactorsAdded() {
            return factorsAdded;
        }

        public int getBestPosition(int id) {
            assert bestPositions != null && bestPositions.size() > id;
            return bestPositions.get(id);
        }

        public double getCurCost() {
            if (curCost.isEmpty()) {
                return 0D;
            }
            return curCost.peekLast();
        }

        public Index getIndex() {
            return index.peekLast();
        }

        public Double getReadCost() {
            return readCost.peekLast();
        }

        public Double getRowFetched() {
            return rowFetched.peekLast();
        }

        public Boolean checkJoinFanOut() {
            if (rowFetched.isEmpty()) {
                return true;
            }
            return rowFetched.peekLast() <= 1D;
        }

        /**
         * use a factor.
         * we need to put the factor to positions[idx]
         * and forward to consider the next factor
         */
        void forward(int factorId, Double cost, AccessPathInfo accessPathInfo) {
            Collections.swap(positions, idx, factorId);
            factorsAdded.set(positions.get(idx));
            leftDepth--;
            idx++;
            index.add(accessPathInfo.index);
            readCost.add(accessPathInfo.readCost);
            curCost.add(cost);
            rowFetched.add(accessPathInfo.rowFetched);
        }

        /**
         * backward a factor
         */
        void back(int factorId) {
            rowFetched.pollLast();
            curCost.pollLast();
            readCost.pollLast();
            index.pollLast();
            idx--;
            leftDepth++;
            factorsAdded.clear(positions.get(idx));
            Collections.swap(positions, idx, factorId);
        }

        boolean searchEnd() {
            return idx >= positions.size() || leftDepth == 0;
        }

        /**
         * @param cost the new cost generated
         * @return true if cost is bigger
         */
        boolean isBt(Double cost) {
            if (minCost == null) {
                return true;
            }
            return minCost >= cost;
        }

        /**
         * record the best join tree found so far
         */
        void recordBetter() {
            this.minCost = curCost.peekLast();
            bestPositions = Lists.newArrayList(positions);
        }

        /**
         * @return the last name of the i-th join factor
         */
        String getName(LoptMultiJoin multiJoin, int i) {
            RelNode node = multiJoin.getJoinFactor(i);
            if (node instanceof HepRelVertex) {
                RelNode node1 = ((HepRelVertex) node).getCurrentRel();
                if (node1 instanceof MysqlTableScan) {
                    RelOptTable table = node1.getTable();
                    List<String> names = table.getQualifiedName();
                    if (!names.isEmpty()) {
                        return names.get(names.size() - 1);
                    }
                }
            }
            return "";
        }

        /**
         * print the information of search context
         *
         * @param factorToAdd the new factor added to the current join tree
         * @param cost cost of the new join tree
         */
        void printCur(LoptMultiJoin multiJoin, int factorToAdd, Double cost, AccessPathInfo accessPathInfo) {
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('\n').append("join order: ");
                for (int i = 0; i < idx; i++) {
                    sb.append(getName(multiJoin, positions.get(i))).append(" ");
                }
                sb.append(getName(multiJoin, factorToAdd)).append('\n');
                sb.append("accumulative cost: ");
                for (int i = 0; i < idx; i++) {
                    sb.append(curCost.get(i)).append(" ");
                }
                sb.append(cost).append('\n');
                DecimalFormat df = new DecimalFormat("0.00");
                if (minCost != null) {
                    sb.append(cost).append(" - ").append(df.format(minCost)).append(" = ")
                        .append(df.format(cost - minCost)).append('\n');
                }
                sb.append("information about the last join\n");
                if (accessPathInfo.index != null) {
                    sb.append("index name:").append(accessPathInfo.index.getIndexMeta().getName()).append('\n');
                    sb.append("index len:").append(accessPathInfo.index.getPrefixLen()).append('\n');
                    sb.append("totalSelectivity: ").append(accessPathInfo.index.getTotalSelectivity()).append('\n');
                    sb.append("joinSelectivity: ").append(accessPathInfo.index.getJoinSelectivity()).append('\n');
                } else {
                    sb.append("no index found!\n");
                }
                if (accessPathInfo.relCost != null) {
                    sb.append(accessPathInfo.relCost.toString()).append('\n');
                }
                sb.append("readCost: ").append(accessPathInfo.readCost).append('\n');
                if (accessPathInfo.leftCount != null) {
                    sb.append("leftRow: ").append(accessPathInfo.leftCount).append('\n');
                }
                logger.debug(sb.toString());
                //System.out.println(sb.toString());
            }
        }
    }
}
