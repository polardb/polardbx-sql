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

import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.LookupJoin;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupJoin;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.MysqlAgg;
import com.alibaba.polardbx.optimizer.core.rel.MysqlHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlLimit;
import com.alibaba.polardbx.optimizer.core.rel.MysqlMaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSort;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTopN;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiSortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.core.rel.SortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.LOOKUP_START_UP_NET;
import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class DrdsRelMdCost extends RelMdPercentageOriginalRows {

    private static final DrdsRelMdCost INSTANCE =
        new DrdsRelMdCost();

    public static final RelMetadataProvider SOURCE =
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE),

                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.CUMULATIVE_COST.method, INSTANCE),

                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE),

                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.START_UP_COST.method, INSTANCE)));

    private static final Logger logger = LoggerFactory.getLogger(DrdsRelMdCost.class);

    private DrdsRelMdCost() {
    }

    public RelOptCost getCumulativeCost(LogicalView rel, RelMetadataQuery mq) {
        return rel.getSelfCost(mq);
    }

    public RelOptCost getNonCumulativeCost(LogicalView rel, RelMetadataQuery mq) {
        return rel.getSelfCost(mq);
    }

    public RelOptCost getCumulativeCost(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getPlan());
    }

    // we need the plannerContext but LogicalTableLookup within calcite package.
    // so we just can put the LogicalTableLookup cost code here
    public RelOptCost getNonCumulativeCost(LogicalTableLookup rel, RelMetadataQuery mq) {

        if (rel.getFixedCost() != null) {
            return rel.getFixedCost();
        }

        RelOptPlanner planner = rel.getCluster().getPlanner();

        final RelNode left = rel.getJoin().getLeft();
        final RelNode right = rel.getJoin().getRight();

        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount = mq.getRowCount(right);

        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);

        RelNode lookupNode = left;
        if (lookupNode instanceof RelSubset) {
            lookupNode = Util.first(((RelSubset) left).getBest(), ((RelSubset) left).getOriginal());
        }

        if (lookupNode instanceof LogicalIndexScan) {
            Join join = ((LogicalIndexScan) lookupNode).getJoin();
            if (join != null) {
                // This TableLookup is lookup side of lookup join such as (BKA, Materialized Semi Join)
                return LookupJoin.getLookupCost(mq, rel.getProject());
            }
        }

        double rowCount = leftRowCount + rightRowCount;
        int batchSize = plannerContext.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
        // must match 1 row, we ignore lookup cost
        double avgTupleMatch = 1;
        double driveSideRowCount = leftRowCount;
        RelDataType driveRowType = left.getRowType();

        double cpu = driveSideRowCount * avgTupleMatch;
        double memory = driveSideRowCount * MemoryEstimator.estimateRowSizeInHashTable(driveRowType);
        double net = Math.ceil(driveSideRowCount / batchSize);

        Double io = 0D;
        Index index = IndexUtil.selectJoinIndex(rel.getJoin(), true);
        if (index != null) {
            RelOptTable table = rel.getPrimaryTable();
            double size =
                TableScanIOEstimator.estimateRowSize(table.getRowType()) * table.getRowCount()
                    * index.getTotalSelectivity();
            double lookupIo = Math.ceil(size / CostModelWeight.RAND_IO_PAGE_SIZE);
            io = Math.ceil(driveSideRowCount * lookupIo / CostModelWeight.LOOKUP_NUM_PER_IO);
        }

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, io, net);
    }

    public Double getPercentageOriginalRows(LogicalView rel, RelMetadataQuery mq) {
        return mq.getPercentageOriginalRows(rel.getOptimizedPushedRelNodeForMetaQuery());
    }

    public Double getPercentageOriginalRows(LogicalTableLookup rel, RelMetadataQuery mq) {
        if (rel.isRelPushedToPrimary()) {
            return mq.getPercentageOriginalRows(rel.getProject());
        } else {
            return mq.getPercentageOriginalRows(rel.getJoin().getLeft());
        }
    }

    public RelOptCost getCumulativeCost(MysqlAgg rel, RelMetadataQuery mq) {
        if (MysqlAgg.OPTIMIZED_AWAY_INDEX == rel.canUseIndex(mq)) {
            return mq.getNonCumulativeCost(rel);
        } else {
            // fallback
            return getCumulativeCost((RelNode) rel, mq);
        }
    }

    public RelOptCost getCumulativeCost(MysqlLimit rel, RelMetadataQuery mq) {

        if (rel.findMysqlLimitBlockingPoint()) {
            // fallback
            return getCumulativeCost((RelNode) rel, mq);
        }

        RelOptCost limitCost = mq.getNonCumulativeCost(rel);
        RelOptCost inputCost = mq.getCumulativeCost(rel.getInput());
        Double inputRowCount = mq.getRowCount(rel.getInput());

        Map<Integer, ParameterContext> params = PlannerContext.getPlannerContext(rel).getParams().getCurrentParameter();
        long skipPlusFetch = 0;
        if (rel.fetch != null) {
            skipPlusFetch += getRexParam(rel.fetch, params);

            if (rel.offset != null) {
                skipPlusFetch += getRexParam(rel.offset, params);
            }
        }

        if (inputRowCount == null || inputRowCount == 0) {
            return null;
        }

        double fraction = Math.min(1, skipPlusFetch / inputRowCount) * 0.9;

        RelOptCost cost = new DrdsRelOptCostImpl(inputCost.getRows(),
            Math.ceil(inputCost.getCpu() * fraction),
            Math.ceil(inputCost.getMemory() * fraction),
            Math.ceil(inputCost.getIo() * fraction),
            inputCost.getNet()
        ).plus(limitCost);

        return cost;
    }

    public RelOptCost getCumulativeCost(Limit rel, RelMetadataQuery mq) {
        return getCumulativeLimitCost(rel, mq);
    }

    public RelOptCost getCumulativeCost(MergeSort rel, RelMetadataQuery mq) {
        if (rel.withLimit()) {
            return getCumulativeLimitCost(rel, mq);
        } else {
            // fallback
            return getCumulativeCost((RelNode) rel, mq);
        }
    }

    private RelOptCost getCumulativeLimitCost(Sort rel, RelMetadataQuery mq) {

        RelOptCost limitCost = mq.getNonCumulativeCost(rel);
        RelOptCost inputCost = mq.getCumulativeCost(rel.getInput());
        Double inputRowCount = mq.getRowCount(rel.getInput());

        Map<Integer, ParameterContext> params = PlannerContext.getPlannerContext(rel).getParams().getCurrentParameter();
        long skipPlusFetch = 0;
        if (rel.fetch != null) {
            skipPlusFetch += getRexParam(rel.fetch, params);

            if (rel.offset != null) {
                skipPlusFetch += getRexParam(rel.offset, params);
            }
        }

        if (inputRowCount == null || inputRowCount == 0) {
            return null;
        }

        double fraction = Math.min(1, skipPlusFetch / inputRowCount);

        RelOptCost startUpCost = mq.getStartUpCost(rel);

        RelOptCost cost = startUpCost.plus(inputCost.minus(startUpCost).multiplyBy(fraction)).plus(limitCost);

        return cost;
    }

    public RelOptCost getCumulativeCost(MysqlTopN rel, RelMetadataQuery mq) {
        RelOptCost topNCost = mq.getNonCumulativeCost(rel);
        RelOptCost inputCost = mq.getCumulativeCost(rel.getInput());
        Double inputRowCount = mq.getRowCount(rel.getInput());
        Double outputRowCount = mq.getRowCount(rel);
        if (inputRowCount == null || outputRowCount == null) {
            return null;
        }

        if (rel.canUseIndex(mq) == null) {
            // fallback
            return getCumulativeCost((RelNode) rel, mq);
        }

        Map<Integer, ParameterContext> params = PlannerContext.getPlannerContext(rel).getParams().getCurrentParameter();
        long skipPlusFetch = 0;
        if (rel.fetch != null) {
            skipPlusFetch += getRexParam(rel.fetch, params);

            if (rel.offset != null) {
                skipPlusFetch += getRexParam(rel.offset, params);
            }
        }

        if (inputRowCount == null || inputRowCount == 0) {
            return null;
        }

        double fraction = Math.min(1, skipPlusFetch / inputRowCount) * 0.9;

        RelOptCost cost = new DrdsRelOptCostImpl(inputCost.getRows(),
            Math.ceil(inputCost.getCpu() * fraction),
            Math.ceil(inputCost.getMemory() * fraction),
            Math.ceil(inputCost.getIo() * fraction),
            inputCost.getNet()
        ).plus(topNCost);

        return cost;
    }

    public RelOptCost getCumulativeCost(RelSubset rel, RelMetadataQuery mq) {
        RelNode bestStartUpRel = rel.getBestStartUpRel();
        RelNode bestTotalRel = rel.getBest();
        if (bestTotalRel == null || bestStartUpRel == null) {
            // must be infinite cost
            return rel.getBestCost();
        } else {
            if (mq.isUseRelSubsetBestCostForCumulativeCost()) {
                return rel.getBestCost();
            } else {
                // use mq.getCumulativeCost instead of RelSubset.getBestCost
                // because RelSubSet.getBestCost is Best Total Cost
                // but we want to get the actual Cost like Limit
                RelOptCost cost1 = mq.getCumulativeCost(bestTotalRel);
                RelOptCost cost2 = null;
                try {
                    cost2 = mq.getCumulativeCost(bestStartUpRel);
                } catch (CyclicMetadataException e) {
                    return cost1;
                    // in case of CyclicMetadataException, we just use bestTotalRel Cost
                }

                if (cost1.isLt(cost2)) {
                    return cost1;
                } else {
                    return cost2;
                }
            }
        }
    }

    /**
     * StartUpCost
     */
    public RelOptCost getStartUpCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost startUpCost = rel.getCluster().getPlanner().getCostFactory().makeTinyCost();
        List<RelNode> inputs = rel.getInputs();
        for (RelNode input : inputs) {
            startUpCost = startUpCost.plus(mq.getStartUpCost(input));
        }
        return startUpCost;
    }

    public RelOptCost getStartUpCost(BKAJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                if (rel.getJoinType() == JoinRelType.RIGHT) {
                    return mq.getStartUpCost(rel.getRight()).plus(rel.getFixedCost());
                } else {
                    return mq.getStartUpCost(rel.getLeft()).plus(rel.getFixedCost());
                }
            }
        }
        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        if (plannerContext.getJoinCount() > plannerContext
            .getParamManager().getInt(ConnectionParams.CBO_BUSHY_TREE_JOIN_LIMIT)) {
            if (rel.getJoinType() == JoinRelType.RIGHT) {
                return mq.getStartUpCost(rel.getRight()).plus(mq.getCumulativeCost(rel.getLeft()));
            } else {
                return mq.getStartUpCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
            }
        }

        RelOptCost lookupSideCost;
        double lookupRowCount;
        RelDataType lookupRowType;
        double driveSideRowCount;

        if (rel.getJoinType() == JoinRelType.RIGHT) {
            lookupSideCost = rel.getLookupCost(mq);
            lookupRowCount = mq.getRowCount(rel.getLeft());
            lookupRowType = rel.getLeft().getRowType();
            driveSideRowCount = mq.getRowCount(rel.getRight());
        } else {
            lookupSideCost = rel.getLookupCost(mq);
            lookupRowCount = mq.getRowCount(rel.getRight());
            lookupRowType = rel.getRight().getRowType();
            driveSideRowCount = mq.getRowCount(rel.getLeft());
        }

        Index index = rel.getLookupIndex();
        double selectivity = 1;
        if (index != null) {
            selectivity = index.getJoinSelectivity();
        }

        int batchSize =
            PlannerContext.getPlannerContext(rel).getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);

        double io =
            Math.ceil(
                Math.min(driveSideRowCount, batchSize * LOOKUP_START_UP_NET) * lookupSideCost.getIo()
                    / CostModelWeight.LOOKUP_NUM_PER_IO);

        double net = Math.min(Math.ceil(driveSideRowCount / batchSize), LOOKUP_START_UP_NET)
            * Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            lookupRowType) * lookupRowCount * selectivity / CostModelWeight.NET_BUFFER_SIZE);

        lookupSideCost =
            rel.getCluster().getPlanner().getCostFactory().makeCost(lookupRowCount, lookupSideCost.getCpu(),
                lookupSideCost.getMemory(), io, net);

        if (rel.getJoinType() == JoinRelType.RIGHT) {
            return mq.getStartUpCost(rel.getRight()).plus(lookupSideCost);
        } else {
            return mq.getStartUpCost(rel.getLeft()).plus(lookupSideCost);
        }
    }

    public RelOptCost getStartUpCost(SemiBKAJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return mq.getStartUpCost(rel.getLeft()).plus(rel.getFixedCost());
            }
        }
        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        if (plannerContext.getJoinCount() > plannerContext
            .getParamManager().getInt(ConnectionParams.CBO_BUSHY_TREE_JOIN_LIMIT)) {
            return mq.getStartUpCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
        }

        RelOptCost lookupSideCost;
        double lookupRowCount;
        RelDataType lookupRowType;
        double driveSideRowCount = mq.getRowCount(rel.getLeft());

        lookupSideCost = rel.getLookupCost(mq);
        lookupRowCount = mq.getRowCount(rel.getRight());
        lookupRowType = rel.getRight().getRowType();

        Index index = rel.getLookupIndex();
        double selectivity = 1;
        if (index != null) {
            selectivity = index.getJoinSelectivity();
        }

        int batchSize =
            PlannerContext.getPlannerContext(rel).getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);

        double io =
            Math.ceil(
                Math.min(driveSideRowCount, batchSize * LOOKUP_START_UP_NET) * lookupSideCost.getIo()
                    / CostModelWeight.LOOKUP_NUM_PER_IO);

        double net = Math.min(Math.ceil(driveSideRowCount / batchSize), LOOKUP_START_UP_NET)
            * Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            lookupRowType) * lookupRowCount * selectivity / CostModelWeight.NET_BUFFER_SIZE);

        lookupSideCost =
            rel.getCluster().getPlanner().getCostFactory().makeCost(lookupRowCount, lookupSideCost.getCpu(),
                lookupSideCost.getMemory(), io, net);

        return mq.getStartUpCost(rel.getLeft()).plus(lookupSideCost);
    }

    public RelOptCost getStartUpCost(LogicalTableLookup rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost().plus(mq.getStartUpCost(rel.getInput()));
            }
        }
        return mq.getStartUpCost(rel.getInput()).multiplyBy(2);
    }

    public RelOptCost getStartUpCost(HashJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                if (rel.getJoinType() == JoinRelType.RIGHT) {
                    if (rel.isOuterBuild()) {
                        return rel.getFixedCost().plus(mq.getStartUpCost(rel.getLeft()));
                    } else {
                        return rel.getFixedCost().plus(mq.getStartUpCost(rel.getRight()));
                    }
                } else {
                    if (rel.isOuterBuild()) {
                        return rel.getFixedCost().plus(mq.getStartUpCost(rel.getRight()));
                    } else {
                        return rel.getFixedCost().plus(mq.getStartUpCost(rel.getLeft()));
                    }
                }
            }
        }
        if (rel.getJoinType() == JoinRelType.RIGHT) {
            if (rel.isOuterBuild()) {
                return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft()));
            } else {
                return mq.getCumulativeCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
            }
        } else {
            if (rel.isOuterBuild()) {
                return mq.getCumulativeCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
            } else {
                return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft()));
            }
        }
    }

    public RelOptCost getStartUpCost(HashGroupJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost().plus(mq.getCumulativeCost(rel.getRight()));
            }
        }
        return mq.getCumulativeCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
    }

    public RelOptCost getStartUpCost(SemiHashJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                if (!rel.isOuterBuild()) {
                    return rel.getFixedCost().plus(mq.getStartUpCost(rel.getLeft()));
                } else {
                    return rel.getFixedCost().plus(mq.getStartUpCost(rel.getRight()));
                }
            }
        }
        if (!rel.isOuterBuild()) {
            return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft()));
        } else {
            return mq.getCumulativeCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
        }
    }

    public RelOptCost getStartUpCost(NLJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                if (rel.getJoinType() == JoinRelType.RIGHT) {
                    return rel.getFixedCost().plus(mq.getStartUpCost(rel.getRight())).multiplyBy(2);
                } else {
                    return rel.getFixedCost().plus(mq.getStartUpCost(rel.getLeft())).multiplyBy(2);
                }
            }
        }
        if (rel.getJoinType() == JoinRelType.RIGHT) {
            return mq.getCumulativeCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight())).multiplyBy(2);
        } else {
            return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft())).multiplyBy(2);
        }
    }

    public RelOptCost getStartUpCost(SemiNLJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost().plus(mq.getStartUpCost(rel.getLeft())).multiplyBy(2);
            }
        }
        return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft())).multiplyBy(2);
    }

    public RelOptCost getStartUpCost(MaterializedSemiJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost();
            }
        }
        // TODO: distinct semi and anti ?
        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        if (plannerContext.getJoinCount() > plannerContext
            .getParamManager().getInt(ConnectionParams.CBO_BUSHY_TREE_JOIN_LIMIT)) {
            return mq.getCumulativeCost(rel.getRight());
        }

        RelOptCost lookupSideCost;
        double lookupRowCount;
        RelDataType lookupRowType;
        double driveSideRowCount = mq.getRowCount(rel.getRight());

        lookupSideCost = rel.getLookupCost(mq);
        lookupRowCount = mq.getRowCount(rel.getLeft());
        lookupRowType = rel.getLeft().getRowType();

        Index index = rel.getLookupIndex();
        double selectivity = 1;
        if (index != null) {
            selectivity = index.getJoinSelectivity();
        }

        int batchSize =
            PlannerContext.getPlannerContext(rel).getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);

        double io =
            Math.ceil(
                Math.min(driveSideRowCount, batchSize * LOOKUP_START_UP_NET) * lookupSideCost.getIo()
                    / CostModelWeight.LOOKUP_NUM_PER_IO);

        double net = Math.min(Math.ceil(driveSideRowCount / batchSize), LOOKUP_START_UP_NET)
            * Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            lookupRowType) * lookupRowCount * selectivity / CostModelWeight.NET_BUFFER_SIZE);

        lookupSideCost =
            rel.getCluster().getPlanner().getCostFactory().makeCost(lookupRowCount, lookupSideCost.getCpu(),
                lookupSideCost.getMemory(), io, net);

        return mq.getCumulativeCost(rel.getRight()).plus(lookupSideCost);
    }

    public RelOptCost getStartUpCost(SortMergeJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost()
                    .plus(mq.getStartUpCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight())));
            }
        }
        // FIXME: Now executor doesn't support SortMergeJoin streaming
        return mq.getCumulativeCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
//        return mq.getStartUpCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
    }

    public RelOptCost getStartUpCost(SemiSortMergeJoin rel, RelMetadataQuery mq) {
        if (rel.getFixedCost() != null) {
            if (rel.getFixedCost().isHuge() || rel.getFixedCost().isInfinite()) {
                return rel.getFixedCost();
            } else {
                return rel.getFixedCost()
                    .plus(mq.getStartUpCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight())));
            }
        }
        // FIXME: Now executor doesn't support SemiSortMergeJoin streaming
        return mq.getCumulativeCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
//        return mq.getStartUpCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
    }

    public RelOptCost getStartUpCost(Project rel, RelMetadataQuery mq) {
        //TODO: deal with subquery
        return mq.getStartUpCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(LogicalFilter rel, RelMetadataQuery mq) {
        //TODO: deal with subquery
        return mq.getStartUpCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(Limit rel, RelMetadataQuery mq) {
        return mq.getStartUpCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(TopN rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(MemSort rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(HashAgg rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(SortAgg rel, RelMetadataQuery mq) {
        // FIXME: Now executor doesn't support SortAgg streaming
        return mq.getCumulativeCost(rel.getInput());
//        return mq.getStartUpCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(SortWindow rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(HashWindow rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getInput());
    }

    public RelOptCost getStartUpCost(RelSubset rel, RelMetadataQuery mq) {
        return rel.getBestStartUpCost();
    }

    public RelOptCost getStartUpCost(LogicalView rel, RelMetadataQuery mq) {
        int shardUpperBound = rel.calShardUpperBound();
        RelOptCost startUpCost = mq.getStartUpCost(rel.getMysqlNode());

        RelOptCost smallerCost = rel.getCluster().getPlanner().getCostFactory()
            .makeCost(startUpCost.getRows(), startUpCost.getCpu() * 0.9, startUpCost.getMemory() * 0.9,
                startUpCost.getIo(), startUpCost.getNet());

        smallerCost = smallerCost.plus(rel.getCluster().getPlanner().getCostFactory().makeCost(0, 0, 0, 0,
            1 + (shardUpperBound - 1) * CostModelWeight.INSTANCE.getShardWeight()));
        return smallerCost;
    }

    public RelOptCost getStartUpCost(MysqlIndexNLJoin rel, RelMetadataQuery mq) {
        if (rel.getJoinType() == JoinRelType.RIGHT) {
            return mq.getStartUpCost(rel.getRight()).plus(mq.getCumulativeCost(rel.getLeft()));
        } else {
            return mq.getStartUpCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
        }
    }

    public RelOptCost getStartUpCost(MysqlNLJoin rel, RelMetadataQuery mq) {
        if (rel.getJoinType() == JoinRelType.RIGHT) {
            return mq.getStartUpCost(rel.getRight()).plus(mq.getCumulativeCost(rel.getLeft()));
        } else {
            return mq.getStartUpCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
        }
    }

    public RelOptCost getStartUpCost(MysqlHashJoin rel, RelMetadataQuery mq) {
        if (rel.getJoinType() == JoinRelType.RIGHT) {
            return mq.getCumulativeCost(rel.getLeft()).plus(mq.getStartUpCost(rel.getRight()));
        } else {
            return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft()));
        }
    }

    public RelOptCost getStartUpCost(MysqlSemiHashJoin rel, RelMetadataQuery mq) {
        return mq.getCumulativeCost(rel.getRight()).plus(mq.getStartUpCost(rel.getLeft()));
    }

    public RelOptCost getStartUpCost(MysqlSemiIndexNLJoin rel, RelMetadataQuery mq) {
        return mq.getStartUpCost(rel.getLeft()).plus(mq.getCumulativeCost(rel.getRight()));
    }

    public RelOptCost getStartUpCost(MysqlMaterializedSemiJoin rel, RelMetadataQuery mq) {
        // TODO: distinct semi and anti ?
        return mq.getCumulativeCost(rel.getRight());
    }

    public RelOptCost getStartUpCost(MysqlAgg rel, RelMetadataQuery mq) {
        if (rel.canUseIndex(mq) != null) {
            return mq.getStartUpCost(rel.getInput());
        } else {
            return mq.getCumulativeCost(rel.getInput());
        }
    }

    public RelOptCost getStartUpCost(MysqlSort rel, RelMetadataQuery mq) {
        if (rel.canUseIndex(mq) != null) {
            return mq.getStartUpCost(rel.getInput());
        } else {
            return mq.getCumulativeCost(rel.getInput());
        }
    }

    public RelOptCost getStartUpCost(MysqlTopN rel, RelMetadataQuery mq) {
        if (rel.canUseIndex(mq) != null) {
            return mq.getStartUpCost(rel.getInput()).multiplyBy(0.9);
        } else {
            // prefer MysqlTopN than leaving limit above logicalview
            return mq.getCumulativeCost(rel.getInput()).multiplyBy(0.9);
        }
    }

    public RelOptCost getStartUpCost(MysqlLimit rel, RelMetadataQuery mq) {
        // [left join + limit]
        // we prefer push down Limit into Mysql to reduce cost of closing connection for start-up cost
        return mq.getStartUpCost(rel.getInput()).multiplyBy(0.9);
    }

    public RelOptCost getStartUpCost(MysqlTableScan rel, RelMetadataQuery mq) {
        // [left join + limit]
        // for mysql table scan StartUpCost need to consider the cost of closing connection
        return rel.getCluster().getPlanner().getCostFactory().makeCost(10000, 10000, 0, 1, 0);
    }
}
