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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.selectivity.TableScanSelectivityEstimator.LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT;

public class DrdsRelMdDistinctRowCount extends RelMdDistinctRowCount {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTINCT_ROW_COUNT.method, new DrdsRelMdDistinctRowCount());

    public Double getDistinctRowCount(LogicalView rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        return rel.getDistinctRowCount(mq, groupKey, predicate);
    }

    public Double getDistinctRowCount(ViewPlan rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        return mq.getDistinctRowCount(rel.getPlan(), groupKey, predicate);
    }

    public Double getDistinctRowCount(MysqlTableScan rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        return mq.getDistinctRowCount(rel.getNodeForMetaQuery(), groupKey, predicate);
    }

    public Double getDistinctRowCount(LogicalTableScan rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1D;
            }
        }

        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta != null) {
            final StatisticService stats = OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager();
            StatisticResult statisticResult = stats.getRowCount(tableMeta.getTableName());
            final long tableRowCount = Math.max(statisticResult.getLongValue(), 1);

            double n = 1.0;
            for (Integer index : groupKey) {
                final ColumnMeta columnMeta = tableMeta.getAllColumns().get(index);
                StatisticResult statisticResult1 = stats.getCardinality(tableMeta.getTableName(), columnMeta.getName());
                long cardinality = statisticResult1.getLongValue();
                if (cardinality >= 0) {
                    // pass
                } else if (CBOUtil.isIndexColumn(tableMeta, columnMeta)) {
                    // lack of statistics
                    if (tableRowCount < LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT) {
                        cardinality = tableRowCount;
                    } else {
                        cardinality = tableRowCount / LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT;
                    }
                } else {
                    cardinality = 10;
                }
                n *= cardinality;
            }
            double selectivity = mq.getSelectivity(rel, predicate);
            n = Math.max(n, 1);
            return adaptNdvBasedOnSelectivity(tableRowCount, n, selectivity);
        }
        return null;
    }

    private double adaptNdvBasedOnSelectivity(double rowCount, double distinctRowCount, double selectivity) {
        double ndv = Math.min(distinctRowCount, rowCount);
        return Math.max((1 - Math.pow(1 - selectivity, rowCount / ndv)) * ndv, 1.0);
    }

    public Double getDistinctRowCount(GroupJoin rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1D;
            }
        }
        // determine which predicates can be applied on the child of the
        // aggregate
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        RelOptUtil.splitFilters(
            rel.getGroupSet(),
            predicate,
            pushable,
            notPushable);
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode childPreds =
            RexUtil.composeConjunction(rexBuilder, pushable, true);

        // set the bits as they correspond to the child input
        ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey);

        /** difference from calcite we use this aggregate groupSet instead of childKey */
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.copyAsJoin(rel.getTraitSet(), rel.getCondition()), rel.getGroupSet(),
                childPreds);

        if (distinctRowCount == null) {
            return null;
        } else if (notPushable.isEmpty()) {
            return distinctRowCount;
        } else {
            RexNode preds =
                RexUtil.composeConjunction(rexBuilder, notPushable, true);
            return distinctRowCount * RelMdUtil.guessSelectivity(preds);
        }
    }

    public Double getDistinctRowCount(TableLookup rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        return mq.getDistinctRowCount(rel.getProject(), groupKey, predicate);
    }

    public Double getDistinctRowCount(SemiJoin rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1D;
            }
        }
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getDistinctRowCount
        RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(mq, rel);
        if (predicate != null) {
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            newPred =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    newPred,
                    predicate);
        }

        // FIXME: use better way to deal with left semi join
        if (rel.getJoinType() == JoinRelType.LEFT) {
            Double distinctRowCount = RelMdUtil.getJoinDistinctRowCount(mq, rel, rel.getJoinType(),
                groupKey, predicate, false);
            Double rowCount = mq.getRowCount(rel);
            return NumberUtil.min(distinctRowCount, rowCount);
        }

        return mq.getDistinctRowCount(rel.getLeft(), groupKey, newPred);
    }

    public Double getDistinctRowCount(LogicalExpand rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1D;
            }
        }
        RexNode newPredicate;
        if (predicate == null) {
            newPredicate = null;
        } else {
            List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);
            List<RexNode> conjunctionsWithoutExpandId = new ArrayList<>();
            for (RexNode rexNode : conjunctions) {
                ImmutableBitSet inputRefs = RelOptUtil.InputFinder.bits(rexNode);
                if (!inputRefs.toList().contains(rel.getExpandIdIdx())) {
                    conjunctionsWithoutExpandId.add(rexNode);
                }
            }
            // ignore expand_id condition if it exists in predicate
            newPredicate =
                RexUtil.composeConjunction(rel.getCluster().getRexBuilder(), conjunctionsWithoutExpandId, false);
        }

        // ndv of expand = ndv of project1 + ndv of project2 + ... + ndv of projectN-1
        if (groupKey.toList().contains(rel.getExpandIdIdx())) {
            Double ndv = 0.0;
            List<Integer> groupKeyIgnoreExpandId = new ArrayList<>();
            for (Integer index : groupKey) {
                if (index != rel.getExpandIdIdx()) {
                    groupKeyIgnoreExpandId.add(index);
                }
            }
            for (List<RexNode> projects : rel.getProjects()) {
                final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(projects, null);
                Double ndvOfCurrentProject = mq.getDistinctRowCount(rel.getInput(), bits, newPredicate);
                if (ndvOfCurrentProject == null) {
                    return null;
                }
                ndv += ndvOfCurrentProject;
            }
            return ndv;
        } else {
            return mq.getDistinctRowCount(rel.getInput(), groupKey, newPredicate);
        }
    }

    public Double getDistinctRowCount(RuntimeFilterBuilder rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
                                      RexNode predicate) {
        return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate);
    }

    public Double getDistinctRowCount(RelSubset subset, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (!Bug.CALCITE_1048_FIXED) {
            RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
            return mq.getDistinctRowCount(rel, groupKey, predicate);
        }
        Double d = null;
        for (RelNode r2 : subset.getRels()) {
            try {
                Double d2 = mq.getDistinctRowCount(r2, groupKey, predicate);
                d = NumberUtil.min(d, d2);
            } catch (CyclicMetadataException e) {
                // Ignore this relational expression; there will be non-cyclic ones
                // in this set.
            }
        }
        return d;
    }

    @Override
    public Double getDistinctRowCount(Aggregate rel, RelMetadataQuery mq,
                                      ImmutableBitSet groupKey, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1D;
            }
        }
        // determine which predicates can be applied on the child of the
        // aggregate
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        RelOptUtil.splitFilters(
            rel.getGroupSet(),
            predicate,
            pushable,
            notPushable);
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode childPreds =
            RexUtil.composeConjunction(rexBuilder, pushable, true);

        // set the bits as they correspond to the child input
        ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey);

        /** difference from calcite we use this aggregate groupSet instead of childKey */
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), rel.getGroupSet(), childPreds);

        if (distinctRowCount == null) {
            return null;
        } else if (notPushable.isEmpty()) {
            return distinctRowCount;
        } else {
            RexNode preds =
                RexUtil.composeConjunction(rexBuilder, notPushable, true);
            return distinctRowCount * RelMdUtil.guessSelectivity(preds);
        }
    }
}
