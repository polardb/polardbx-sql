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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdLowerBoundCost;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.TUPLE_HEADER_SIZE;

/**
 * @author dylan
 */
public class DrdsRelMdLowerBoundCost extends RelMdLowerBoundCost {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DrdsRelMdLowerBoundCost(), BuiltInMethod.LOWER_BOUND_COST.method);

    @Override
    public RelOptCost getLowerBoundCost(RelSubset subset,
                                        RelMetadataQuery mq, VolcanoPlanner planner) {

        if (planner.isLogical(subset)) {
            // by now impossible path, because calcite will only call getLowerBoundCost for Physical Node
            return mq.getLowerBoundCost(subset.getOriginal(), planner);
        }

        RelOptCost winnerCost = subset.getWinnerCost();

        if (winnerCost == null) {
            if (planner.isLogical(subset.getOriginal())) {
                return mq.getLowerBoundCost(subset.getOriginal(), planner);
            } else {
                // LogicalView with join(like: BKAJoin)
                return null;
            }
        } else {
            return winnerCost;
        }
    }

    @Override
    public RelOptCost getLowerBoundCost(RelNode node,
                                        RelMetadataQuery mq, VolcanoPlanner planner) {
        RelOptCost selfCost;
        if (planner.isLogical(node)) {
            if (node instanceof Join) {
                Join join = (Join) node;

                RelOptCost hashJoinCumulativeCost = hashJoinCumulativeCostLowerBound(join, mq, planner);

                RelOptCost bkaJoinCumulativeCost = bkaJoinCumulativeCostLowerBound(join, mq, planner);

                RelOptCost sortMergeJoinCumulativeCost = sortMergeJoinCumulativeCostLowerBound(join, mq, planner);

                RelOptCost winner = bkaJoinCumulativeCost.isLe(hashJoinCumulativeCost) ? bkaJoinCumulativeCost :
                    hashJoinCumulativeCost;

                return winner.isLe(sortMergeJoinCumulativeCost) ? winner : sortMergeJoinCumulativeCost;
            } else {
                selfCost = planner.getCostFactory().makeZeroCost();
            }
        } else {
            selfCost = mq.getNonCumulativeCost(node);
        }

        if (selfCost != null && selfCost.isInfinite()) {
            selfCost = null;
        }
        for (RelNode input : node.getInputs()) {
            RelOptCost lb = mq.getLowerBoundCost(input, planner);
            if (lb != null) {
                selfCost = selfCost == null ? lb : selfCost.plus(lb);
            }
        }
        return selfCost;
    }

    private RelOptCost bkaJoinCumulativeCostLowerBound(Join join, RelMetadataQuery mq, VolcanoPlanner planner) {
        final double leftRowCount = mq.getRowCount(join.getLeft());
        final double rightRowCount = mq.getRowCount(join.getRight());

        final RelDataType lookupSideRowType;
        final RelDataType driveSideRowType;

        final double lookupRowCount;
        final double driveSideRowCount;

        if (leftRowCount < rightRowCount) {
            driveSideRowCount = leftRowCount;
            lookupRowCount = rightRowCount;
            driveSideRowType = join.getLeft().getRowType();
            lookupSideRowType = join.getRight().getRowType();
        } else {
            driveSideRowCount = rightRowCount;
            lookupRowCount = leftRowCount;
            driveSideRowType = join.getRight().getRowType();
            lookupSideRowType = join.getLeft().getRowType();
        }

        // BKAJoin
        double bkaJoinMemory = driveSideRowCount * MemoryEstimator.estimateRowSizeInArrayList(driveSideRowType);
        double bkaJoinIo = Math.ceil(driveSideRowCount * 1 / CostModelWeight.LOOKUP_NUM_PER_IO);

        // lookup cpu cost
        double bkaJoinCpu = driveSideRowCount;
        // hash join cpu cost
        bkaJoinCpu += CostModelWeight.INSTANCE.getProbeWeight() * driveSideRowCount +
            CostModelWeight.INSTANCE.getBuildWeight() * driveSideRowCount;

        int batchSize =
            PlannerContext.getPlannerContext(join).getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);

        double lookupCount = Math.ceil(driveSideRowCount / batchSize);
        double eachLookupNet = Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            driveSideRowType) / CostModelWeight.NET_BUFFER_SIZE) + 1;
        double bkaJoinNet = lookupCount * eachLookupNet;
        bkaJoinNet -= 1; // minus lookupSide net to correct cumulative cost

        RelOptCost bkaJoinCost = planner.getCostFactory().makeCost(driveSideRowCount, bkaJoinCpu, bkaJoinMemory,
            bkaJoinIo, bkaJoinNet);

        RelOptCost bkaJoinCumulativeCost = bkaJoinCost
            .plus(planner.getCostFactory().makeCost(driveSideRowCount, driveSideRowCount, 0,
                driveSideRowCount * (TUPLE_HEADER_SIZE + TableScanIOEstimator.estimateRowSize(driveSideRowType))
                    / CostModelWeight.SEQ_IO_PAGE_SIZE,
                Math.ceil(TableScanIOEstimator.estimateRowSize(driveSideRowType)) * driveSideRowCount
                    / CostModelWeight.NET_BUFFER_SIZE));

        return bkaJoinCumulativeCost;
    }

    private RelOptCost hashJoinCumulativeCostLowerBound(Join join, RelMetadataQuery mq, VolcanoPlanner planner) {
        final double leftRowCount = mq.getRowCount(join.getLeft());
        final double rightRowCount = mq.getRowCount(join.getRight());

        final RelDataType probeSideRowType;
        final RelDataType buildSideRowType;

        final double probeRowCount;
        final double buildRowCount;

        if (leftRowCount < rightRowCount) {
            buildRowCount = leftRowCount;
            probeRowCount = rightRowCount;
            buildSideRowType = join.getLeft().getRowType();
            probeSideRowType = join.getRight().getRowType();
        } else {
            buildRowCount = rightRowCount;
            probeRowCount = leftRowCount;
            buildSideRowType = join.getRight().getRowType();
            probeSideRowType = join.getLeft().getRowType();
        }

        double buildWeight = CostModelWeight.INSTANCE.getBuildWeight();
        double probeWeight = CostModelWeight.INSTANCE.getProbeWeight();

        double rowCount = probeRowCount + buildRowCount;

        double hashJoinCpu = buildWeight * buildRowCount + probeWeight * probeRowCount;

        double hashJoinMemory =
            MemoryEstimator.estimateRowSizeInHashTable(buildSideRowType) * buildRowCount;

        RelOptCost hashJoinCost =
            planner.getCostFactory().makeCost(rowCount, hashJoinCpu, hashJoinMemory, 0, 0);

        RelOptCost hashJoinCumulativeCost = hashJoinCost
            .plus(planner.getCostFactory().makeCost(buildRowCount, buildRowCount, 0,
                buildRowCount * (TUPLE_HEADER_SIZE + TableScanIOEstimator
                    .estimateRowSize(buildSideRowType)) / CostModelWeight.SEQ_IO_PAGE_SIZE,
                Math.ceil(TableScanIOEstimator.estimateRowSize(buildSideRowType) * buildRowCount
                    / CostModelWeight.NET_BUFFER_SIZE)))
            .plus(planner.getCostFactory().makeCost(probeRowCount, probeRowCount, 0,
                probeRowCount * (TUPLE_HEADER_SIZE + TableScanIOEstimator
                    .estimateRowSize(probeSideRowType)) / CostModelWeight.SEQ_IO_PAGE_SIZE,
                Math.ceil(TableScanIOEstimator.estimateRowSize(probeSideRowType) * probeRowCount
                    / CostModelWeight.NET_BUFFER_SIZE)));

        return hashJoinCumulativeCost;
    }

    private RelOptCost sortMergeJoinCumulativeCostLowerBound(Join join, RelMetadataQuery mq, VolcanoPlanner planner) {
        final double leftRowCount = mq.getRowCount(join.getLeft());
        final double rightRowCount = mq.getRowCount(join.getRight());

        final RelDataType leftRowType = join.getLeft().getRowType();
        final RelDataType rightRowType = join.getRight().getRowType();

        double rowCount = leftRowCount + rightRowCount;

        double sortMergeJoinCpu = CostModelWeight.INSTANCE.getMergeWeight() * (leftRowCount + rightRowCount);

        RelOptCost sortMergeJoinCost =
            planner.getCostFactory().makeCost(rowCount, sortMergeJoinCpu, 0, 0, 0);

        RelOptCost sortMergeJoinCumulativeCost = sortMergeJoinCost
            .plus(planner.getCostFactory().makeCost(leftRowCount, leftRowCount, 0,
                leftRowCount * (TUPLE_HEADER_SIZE + TableScanIOEstimator
                    .estimateRowSize(leftRowType)) / CostModelWeight.SEQ_IO_PAGE_SIZE,
                Math.ceil(TableScanIOEstimator.estimateRowSize(leftRowType) * leftRowCount
                    / CostModelWeight.NET_BUFFER_SIZE)))
            .plus(planner.getCostFactory().makeCost(rightRowCount, rightRowCount, 0,
                rightRowCount * (TUPLE_HEADER_SIZE + TableScanIOEstimator
                    .estimateRowSize(rightRowType)) / CostModelWeight.SEQ_IO_PAGE_SIZE,
                Math.ceil(TableScanIOEstimator.estimateRowSize(rightRowType) * rightRowCount
                    / CostModelWeight.NET_BUFFER_SIZE)));

        return sortMergeJoinCumulativeCost;
    }
}
