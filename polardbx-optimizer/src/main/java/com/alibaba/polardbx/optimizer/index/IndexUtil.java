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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMdSelectivity;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.AccessPathRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.CheckMysqlIndexNLJoinRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlAgg;
import com.alibaba.polardbx.optimizer.core.rel.MysqlHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlMaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class IndexUtil {
    public static Integer getColumnCouldUseIndex(int ref, boolean lookupRight, RelNode lookupNode, int leftBound) {
        if (lookupRight && ref < leftBound) {
            return null;
        } else if (!lookupRight && ref >= leftBound) {
            return null;
        }

        if (lookupRight) {
            ref -= leftBound;
        }

        RelColumnOrigin relColumnOrigin = lookupNode.getCluster().getMetadataQuery().getColumnOrigin(lookupNode, ref);
        if (relColumnOrigin == null) {
            return null;
        }
        return relColumnOrigin.getOriginColumnOrdinal();
    }

    /**
     * find the best index for join
     *
     * @param join the join to be checked
     * @return the index type and it's selectivity
     */
    public static Index selectJoinIndex(Join join, boolean joinSelectivityFirst) {
        if (join.getJoinType() == JoinRelType.ANTI) {
            return null;
        }

        RexNode condition = join.getCondition();
        if (condition == null) {
            return null;
        }

        boolean isMysqlJoin = isMysqlJoin(join);

        RelMetadataQuery relMetadataQuery = join.getCluster().getMetadataQuery();
        int leftBound = join.getLeft().getRowType().getFieldCount();
        boolean lookupRight;

        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
            lookupRight = true;
            break;
        case SEMI:
        case ANTI:
            if (join instanceof SemiBKAJoin) {
                lookupRight = true;
                break;
            } else if (join instanceof MaterializedSemiJoin) {
                lookupRight = false;
                break;
            } else if (join instanceof MysqlSemiIndexNLJoin) {
                lookupRight = true;
                break;
            }
        case RIGHT:
            lookupRight = false;
            break;
        default:
            return null;
        }

        RelNode lookupNode;
        if (lookupRight) {
            lookupNode = join.getRight();
        } else {
            lookupNode = join.getLeft();
        }

        if (lookupNode instanceof RelSubset) {
            lookupNode = Util.first(((RelSubset) lookupNode).getBest(), ((RelSubset) lookupNode).getOriginal());
        }

        if (lookupNode instanceof Gather) {
            lookupNode = ((Gather) lookupNode).getInput();
        }

        RelNode beforeExpandTableLookup = null;
        RelNode afterExpandTableLookup = null;
        if (!isMysqlJoin) {
            if (lookupNode instanceof LogicalTableLookup) {
                // before expand, there will be LogicalTableLookup
                beforeExpandTableLookup = lookupNode;
                lookupNode = ((LogicalTableLookup) lookupNode).getInput();
            } else if (lookupNode instanceof Project) {
                // after expand, there will be Project + BKAJoin(IndexScan, LogicalView)
                afterExpandTableLookup = lookupNode;
                lookupNode = ((Project) lookupNode).getInput();
                if (lookupNode instanceof RelSubset) {
                    // MPP Planner will enter this branch
                    lookupNode = Util.first(((RelSubset) lookupNode).getBest(), ((RelSubset) lookupNode).getOriginal());
                }
                Join joinFromTableLookup = (Join) lookupNode;
                lookupNode = joinFromTableLookup.getLeft();
            }
            if (lookupNode instanceof RelSubset) {
                lookupNode = Util.first(((RelSubset) lookupNode).getBest(), ((RelSubset) lookupNode).getOriginal());
            }

            if (lookupNode instanceof Gather) {
                lookupNode = ((Gather) lookupNode).getInput();
            }
        }

        // only support lookup one table
        Set<RexTableInputRef.RelTableRef> tableRefSet = relMetadataQuery.getTableReferences(lookupNode);

        if (tableRefSet.size() != 1) {
            return null;
        }

        TableMeta tableMeta = CBOUtil.getTableMeta(tableRefSet.iterator().next().getTable());

        CheckMysqlIndexNLJoinRelVisitor checkMysqlIndexNLJoinRelVisitor = new CheckMysqlIndexNLJoinRelVisitor();

        if (lookupNode instanceof LogicalView) {
            ((LogicalView) lookupNode).getMysqlNode().accept(checkMysqlIndexNLJoinRelVisitor);
        } else {
            lookupNode.accept(checkMysqlIndexNLJoinRelVisitor);
        }

        if (!checkMysqlIndexNLJoinRelVisitor.isSupportUseIndexNLJoin()) {
            return null;
        }

        final RelNode lookUpNodeForAnalyzeJoinCondition;
        if (beforeExpandTableLookup != null) {
            lookUpNodeForAnalyzeJoinCondition = beforeExpandTableLookup;
        } else if (afterExpandTableLookup != null) {
            lookUpNodeForAnalyzeJoinCondition = afterExpandTableLookup;
        } else {
            lookUpNodeForAnalyzeJoinCondition = lookupNode;
        }

        MysqlTableScan mysqlTableScan = checkMysqlIndexNLJoinRelVisitor.getMysqlTableScan();

        // extract columns could use index for Join condition
        Set<Integer> joinConditionColumnCouldUseIndex = new HashSet<>();

        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        for (RexNode pred : conjunctions) {
            if (pred.isA(SqlKind.EQUALS) && pred instanceof RexCall) {
                RexCall filterCall = (RexCall) pred;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);
                if (leftRexNode instanceof RexInputRef) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    Integer col = getColumnCouldUseIndex(leftIndex, lookupRight,
                        lookUpNodeForAnalyzeJoinCondition, leftBound);
                    if (col != null) {
                        joinConditionColumnCouldUseIndex.add(col);
                    }
                }
                if (rightRexNode instanceof RexInputRef) {
                    int rightIndex = ((RexInputRef) rightRexNode).getIndex();
                    Integer col = getColumnCouldUseIndex(rightIndex, lookupRight,
                        lookUpNodeForAnalyzeJoinCondition, leftBound);
                    if (col != null) {
                        joinConditionColumnCouldUseIndex.add(col);
                    }
                }
            }
        }

        String schema = PlannerContext.getPlannerContext(join).getSchemaName();

        PriorityQueue<Index> priorityQueue = new PriorityQueue<>(new Comparator<Index>() {
            @Override
            public int compare(Index o1, Index o2) {
                if (o1.getIndexMeta().isPrimaryKeyIndex() && o1.getPrefixLen() == o1.getIndexMeta().getKeyColumns()
                    .size()) {
                    return -1;
                } else if (o1.getIndexMeta().isUniqueIndex() && o1.getPrefixLen() == o1.getIndexMeta().getKeyColumns()
                    .size()) {
                    return -1;
                } else {
                    return joinSelectivityFirst ?
                        (o1.getJoinSelectivity() < o2.getJoinSelectivity() ? -1 : 1) :
                        (o1.getTotalSelectivity() < o2.getTotalSelectivity() ? -1 : 1);
                }
            }
        });

        // extract columns could use index for MysqlTableScan
        final Set<Integer> lookupScanColumnCouldUseIndex;
        final Set<String> canUseIndexSet;
        if (mysqlTableScan != null) {
            lookupScanColumnCouldUseIndex = mysqlTableScan.getColumnCouldUseIndexForJoin(isMysqlJoin);
            canUseIndexSet = getCanUseIndexSet(mysqlTableScan);
        } else {
            lookupScanColumnCouldUseIndex = new HashSet<>();
            canUseIndexSet = new HashSet<>();
        }

        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }
            double totalSelectivity = 1;
            double joinSelectivity = 1;
            int prefixLen = 0;
            for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
                int columnIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta, columnMeta);
                if (joinConditionColumnCouldUseIndex.contains(columnIndex)
                    || lookupScanColumnCouldUseIndex.contains(columnIndex)) {
                    prefixLen++;
                    PlannerContext pc = StatisticUtils.getPlannerContextFromRelNode(join);
                    boolean isNeedTrace = pc.isNeedStatisticTrace();
                    StatisticResult statisticResult =
                        StatisticManager.getInstance()
                            .getCardinality(schema, tableMeta.getTableName(), columnMeta.getName(), true, isNeedTrace);
                    if (isNeedTrace) {
                        pc.recordStatisticTrace(statisticResult.getTrace());
                    }
                    long cardinality = statisticResult.getLongValue();
                    if (cardinality <= 0) {
                        cardinality = (long) (tableMeta.getRowCount(pc) / CostModelWeight.INSTANCE.getAvgTupleMatch());
                    }
                    if (cardinality <= 0) {
                        cardinality = 1;
                    }
                    totalSelectivity *= 1.0 / cardinality;
                    if (joinConditionColumnCouldUseIndex.contains(columnIndex)
                        && !lookupScanColumnCouldUseIndex.contains(columnIndex)) {
                        joinSelectivity *= 1.0 / cardinality;
                    }
                } else {
                    break;
                }
            }
            if (prefixLen > 0 && joinSelectivity <= 1) {
                List<Index.PredicateType> prefixTypeList = new ArrayList<>(prefixLen);
                for (int i = 0; i < prefixLen; i++) {
                    prefixTypeList.add(Index.PredicateType.EQUAL);
                }
                priorityQueue.add(new Index(indexMeta, prefixTypeList, totalSelectivity, joinSelectivity));
            }
        }

        Index index = priorityQueue.poll();
        return index;
    }

    public static Index selectIndexForOrderBy(TableMeta tableMeta, List<Integer> orderByColumn,
                                              Set<String> canUseIndexSet) {
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }
            Index index = canIndexUsedByOrderBy(tableMeta, indexMeta, ImmutableList.of(), orderByColumn);
            if (index != null) {
                return index;
            }
        }
        return null;
    }

    public static Index canIndexUsedByOrderBy(TableMeta tableMeta, IndexMeta indexMeta,
                                              List<Index.PredicateType> prefixTypeList,
                                              List<Integer> orderByColumn) {
        OUTER:
        for (int eachPrefixLen = 0; eachPrefixLen <= prefixTypeList.size(); eachPrefixLen++) {
            if (indexMeta.getKeyColumns().size() < eachPrefixLen + orderByColumn.size()) {
                return null;
            }

            for (int i = 0; i < orderByColumn.size(); i++) {
                int orderByColumnIndex = orderByColumn.get(i);
                int columnIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                    indexMeta.getKeyColumns().get(eachPrefixLen + i));
                if (columnIndex != orderByColumnIndex) {
                    continue OUTER;
                }
            }
            List<Index.PredicateType> newPrefixTypeList = new ArrayList<>();
            for (int i = 0; i < eachPrefixLen; i++) {
                newPrefixTypeList.add(prefixTypeList.get(i));
            }
            for (int i = eachPrefixLen; i < eachPrefixLen + orderByColumn.size(); i++) {
                newPrefixTypeList.add(Index.PredicateType.SORT);
            }
            return new Index(indexMeta, newPrefixTypeList, 1, 1);
        }
        return null;
    }

    public static Index selectIndexForGroupBy(TableMeta tableMeta, List<Integer> groupByColumn,
                                              Set<String> canUseIndexSet) {
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }
            Index index = canIndexUsedByGroupBy(tableMeta, indexMeta, ImmutableList.of(), groupByColumn);
            if (index != null) {
                return index;
            }
        }
        return null;
    }

    public static Index selectIndexForMinMaxAggCall(TableMeta tableMeta, Set<Integer> minMaxAggCallKeyColumnSet,
                                                    Set<String> canUseIndexSet) {
        Index result = MysqlAgg.OPTIMIZED_AWAY_INDEX;
        for (Integer i : minMaxAggCallKeyColumnSet) {
            List<Integer> columnSet = new ArrayList<>();
            columnSet.add(i);
            Index index = selectIndexForGroupBy(tableMeta, columnSet, canUseIndexSet);
            if (index == null) {
                return null;
            }
        }
        return result;
    }

    public static Index canIndexUsedByMinMaxAggCall(TableMeta tableMeta, IndexMeta indexMeta,
                                                    Set<Integer> minMaxAggCallKeyColumnSet) {
        Set<String> canUseIndexSet = new HashSet<>();
        canUseIndexSet.add(indexMeta.getPhysicalIndexName());
        return selectIndexForMinMaxAggCall(tableMeta, minMaxAggCallKeyColumnSet, canUseIndexSet);
    }

    public static Index canIndexUsedByGroupBy(TableMeta tableMeta, IndexMeta indexMeta,
                                              List<Index.PredicateType> prefixTypeList, List<Integer> groupByColumn) {
        if (prefixTypeList.stream().anyMatch(x -> x != Index.PredicateType.EQUAL)) {
            return null;
        }

        OUTER:
        for (int eachPrefixLen = 0; eachPrefixLen <= prefixTypeList.size(); eachPrefixLen++) {
            if (indexMeta.getKeyColumns().size() < eachPrefixLen + groupByColumn.size()) {
                return null;
            }

            for (int i = 0; i < groupByColumn.size(); i++) {
                int groupColumnIndex = groupByColumn.get(i);
                int columnIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                    indexMeta.getKeyColumns().get(eachPrefixLen + i));
                if (columnIndex != groupColumnIndex) {
                    continue OUTER;
                }
            }

            List<Index.PredicateType> newPrefixTypeList = new ArrayList<>();
            for (int i = 0; i < eachPrefixLen; i++) {
                newPrefixTypeList.add(Index.PredicateType.EQUAL);
            }
            for (int i = eachPrefixLen; i < eachPrefixLen + groupByColumn.size(); i++) {
                newPrefixTypeList.add(Index.PredicateType.AGG);
            }
            return new Index(indexMeta, newPrefixTypeList, 1, 1);

        }
        return null;
    }

    public static Set<String> getCanUseIndexSet(TableScan tableScan) {
        Set<String> useIndexSet = AccessPathRule.getUseIndex(tableScan.getIndexNode());
        Set<String> ignoreIndexSet = AccessPathRule.getIgnoreIndex(tableScan.getIndexNode());
        Set<String> forceIndexSet = AccessPathRule.getForceIndex(tableScan.getIndexNode());

        Set<String> canUseIndexSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());

        canUseIndexSet.addAll(
            tableMeta.getIndexes().stream().map(x -> x.getPhysicalIndexName()).collect(Collectors.toSet()));

        if (!ignoreIndexSet.isEmpty()) {
            canUseIndexSet.removeAll(ignoreIndexSet);
        }

        if (!useIndexSet.isEmpty()) {
            canUseIndexSet.retainAll(useIndexSet);
        }

        if (!forceIndexSet.isEmpty()) {
            canUseIndexSet.retainAll(forceIndexSet);
        }

        return canUseIndexSet;
    }

    public static boolean isMysqlJoin(Join join) {
        return join instanceof MysqlIndexNLJoin
            || join instanceof MysqlHashJoin
            || join instanceof MysqlNLJoin
            || join instanceof MysqlSemiHashJoin
            || join instanceof MysqlSemiIndexNLJoin
            || join instanceof MysqlMaterializedSemiJoin;
    }
}
