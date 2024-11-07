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

package com.alibaba.polardbx.optimizer.selectivity;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class JoinSelectivityEstimator extends AbstractSelectivityEstimator {

    private final Join join;
    private final Double leftRowCount;
    private final Double rightRowCount;
    private final int leftBound;

    public JoinSelectivityEstimator(Join join, RelMetadataQuery metadataQuery) {
        super(metadataQuery, join.getCluster().getRexBuilder(), PlannerContext.getPlannerContext(join));
        this.join = join;
        this.leftRowCount = metadataQuery.getRowCount(join.getLeft());
        this.rightRowCount = metadataQuery.getRowCount(join.getRight());
        this.leftBound = join.getLeft().getRowType().getFieldCount();
    }

    @Override
    public Double visitCall(RexCall call) {
        if (call.getOperator() == SqlStdOperatorTable.AND) {

            // TODO: consider composite primary key
            Double selectivityAnd = estimateCompositeEqualSelectivity(call);
            if (selectivityAnd != null) {
                return selectivityAnd;
            }
            // TODO: use (a, b) -> a * b instead of (a, b) -> Math.min(a, b)
            selectivityAnd =
                call.getOperands().stream().map(rexNode -> this.evaluate(rexNode))
                    .reduce(1.0, (a, b) -> Math.min(a, b));
            return normalize(selectivityAnd);
        } else if (call.getOperator() == SqlStdOperatorTable.OR) {
            Double selectivityOr =
                call.getOperands().stream().map(rexNode -> this.evaluate(rexNode)).reduce(0.0, (a, b) -> a + b - a * b);
            return normalize(selectivityOr);
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            Double selectivity = this.evaluate(call.getOperands().get(0));
            return normalize(1 - selectivity);
        } else if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
            return estimateEqualSelectivity(call);
        } else {
            // TODO: add more predicate
            return RelMdUtil.guessSelectivity(call);
        }
    }

    private Double estimateCompositeEqualSelectivity(RexCall call) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(call);

        List<Integer> leftIndexes = Lists.newArrayList();
        List<Integer> rightIndexes = Lists.newArrayList();
        for (RexNode node : conjunctions) {
            if (!node.isA(SqlKind.EQUALS)) {
                continue;
            }
            if (!(node instanceof RexCall)) {
                continue;
            }
            RexNode leftRexNode = ((RexCall) node).getOperands().get(0);
            RexNode rightRexNode = ((RexCall) node).getOperands().get(1);
            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                if (((RexInputRef) leftRexNode).getIndex() < leftBound
                    && ((RexInputRef) rightRexNode).getIndex() >= leftBound) {
                    leftIndexes.add(((RexInputRef) leftRexNode).getIndex());
                    rightIndexes.add(((RexInputRef) rightRexNode).getIndex() - leftBound);
                }
                if (((RexInputRef) leftRexNode).getIndex() >= leftBound
                    && ((RexInputRef) rightRexNode).getIndex() < leftBound) {
                    leftIndexes.add(((RexInputRef) rightRexNode).getIndex());
                    rightIndexes.add(((RexInputRef) leftRexNode).getIndex() - leftBound);
                }
            }
        }
        if (leftIndexes.size() <= 1) {
            return null;
        }

        Double minSelectivity = null;
        // unique column
        Boolean leftUnique = metadataQuery.areColumnsUnique(join.getLeft(), ImmutableBitSet.of(leftIndexes));
        if (leftUnique != null && leftUnique) {
            minSelectivity = 1.0 / leftRowCount;
        }

        Boolean rightUnique = metadataQuery.areColumnsUnique(join.getRight(), ImmutableBitSet.of(rightIndexes));
        if (rightUnique != null && rightUnique) {
            if (minSelectivity == null) {
                minSelectivity = 1.0 / rightRowCount;
            } else {
                minSelectivity = Math.min(minSelectivity, 1.0 / rightRowCount);
            }
        }
        return minSelectivity;
    }

    private double estimateEqualSelectivity(RexCall call) {
        assert call.getOperator() == SqlStdOperatorTable.EQUALS;
        RexNode leftRexNode = call.getOperands().get(0);
        RexNode rightRexNode = call.getOperands().get(1);

        Integer leftIndex = null;
        Integer rightIndex = null;
        Boolean leftUnique = null;
        Boolean rightUnique = null;
        Double leftNdv = null;
        Double rightNdv = null;
        if (leftRexNode instanceof RexInputRef) {
            int index = ((RexInputRef) leftRexNode).getIndex();
            if (index < leftBound) {
                leftIndex = index;
                leftNdv = metadataQuery.getDistinctRowCount(join.getLeft(), ImmutableBitSet.of(index), null);
            } else {
                rightIndex = index;
                rightNdv =
                    metadataQuery.getDistinctRowCount(join.getRight(), ImmutableBitSet.of(index - leftBound), null);
            }
        }

        if (rightRexNode instanceof RexInputRef) {
            int index = ((RexInputRef) rightRexNode).getIndex();
            if (index < leftBound) {
                leftIndex = index;
                leftNdv = metadataQuery.getDistinctRowCount(join.getLeft(), ImmutableBitSet.of(index), null);
            } else {
                rightIndex = index;
                rightNdv =
                    metadataQuery.getDistinctRowCount(join.getRight(), ImmutableBitSet.of(index - leftBound), null);
            }
        }

        if (leftNdv != null && rightNdv != null) {
            return 1.0 / Math.max(leftNdv, rightNdv);
        }

        if (leftIndex != null) {
            leftUnique = metadataQuery.areColumnsUnique(join.getLeft(), ImmutableBitSet.of(leftIndex));
        }

        if (rightIndex != null) {
            rightUnique = metadataQuery.areColumnsUnique(join.getRight(), ImmutableBitSet.of(rightIndex - leftBound));
        }

        if (Boolean.TRUE.equals(leftUnique) && Boolean.TRUE.equals(rightUnique)) {
            return 1.0 / Math.max(leftRowCount, rightRowCount);
        } else if (Boolean.TRUE.equals(leftUnique)) {
            return 1.0 / leftRowCount;
        } else if (Boolean.TRUE.equals(rightUnique)) {
            return 1.0 / rightRowCount;
        }

        if (leftRexNode instanceof RexInputRef) {
            if ((DrdsRexFolder.fold(rightRexNode, PlannerContext.getPlannerContext(join))) != null) {
                return 1;
            }
        }
        if (rightRexNode instanceof RexInputRef) {
            if ((DrdsRexFolder.fold(leftRexNode, PlannerContext.getPlannerContext(join))) != null) {
                return 1;
            }
        }
        return RelMdUtil.guessSelectivity(call);
    }
}
