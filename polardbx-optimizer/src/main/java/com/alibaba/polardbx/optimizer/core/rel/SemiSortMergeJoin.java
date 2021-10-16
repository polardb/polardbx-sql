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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SemiSortMergeJoin extends SemiJoin implements PhysicalNode {

    private SqlOperator operator;
    private RelNode pushDownRelNode;
    private List<RexNode> operands;
    private String subqueryPosition;
    private RelOptCost fixedCost;
    private final List<Integer> leftColumns;
    private final List<Integer> rightColumns;
    private final RexNode otherCondition;

    private final RelCollation collation;

    // only contains the equal part
    private JoinInfo joinInfo;

    // ~ Constructors -----------------------------------------------------------

    public SemiSortMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinRelType,
        List<RexNode> operands,
        Set<CorrelationId> variablesSet,
        SqlNodeList hints,
        SqlOperator operator,
        RelNode pushDownRelNode,
        String subqueryPosition,
        final List<Integer> leftColumns,
        final List<Integer> rightColumns,
        final RelCollation relCollation,
        RexNode otherCondition
    ) {
        super(
            cluster,
            traitSet,
            left,
            right,
            condition,
            leftKeys,
            rightKeys,
            variablesSet,
            joinRelType,
            hints);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.operands = operands;
        this.operator = operator;
        this.pushDownRelNode = pushDownRelNode;
        this.subqueryPosition = subqueryPosition;
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        this.otherCondition = otherCondition;
        this.collation = relCollation;
        this.joinInfo = JoinInfo.of(ImmutableIntList.copyOf(leftColumns), ImmutableIntList.copyOf(rightColumns));
    }

    public static SemiSortMergeJoin create(
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RelCollation relCollation,
        RexNode condition,
        LogicalSemiJoin semiJoin,
        final List<Integer> leftColumns,
        final List<Integer> rightColumns,
        RexNode otherCondition) {
        final RelOptCluster cluster = left.getCluster();

        return new SemiSortMergeJoin(
            cluster,
            traitSet.plus(relCollation),
            left,
            right,
            condition,
            ImmutableIntList.copyOf(leftColumns),
            ImmutableIntList.copyOf(rightColumns),
            semiJoin.getJoinType(),
            semiJoin.getOperands(),
            semiJoin.getVariablesSet(),
            semiJoin.getHints(),
            semiJoin.getOperator(),
            semiJoin.getPushDownRelNode(),
            semiJoin.getSubqueryPosition(),
            leftColumns,
            rightColumns,
            relCollation,
            otherCondition);
    }

    public SemiSortMergeJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableIntList.copyOf(relInput.getIntegerList("leftColumns")),
            ImmutableIntList.copyOf(relInput.getIntegerList("rightColumns")),
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        this.otherCondition = relInput.getExpression("otherCondition");
        this.leftColumns = relInput.getIntegerList("leftColumns");
        this.rightColumns = relInput.getIntegerList("rightColumns");
        if (relInput.get("collation") == null) {
            this.collation = CBOUtil.createRelCollation(leftColumns);
        } else {
            this.collation = relInput.getCollation();
        }
        if (relInput.get("operands") == null) {
            this.operands = ImmutableList.of();
        } else {
            this.operands = relInput.getExpressionList("operands");
        }
        this.joinInfo = JoinInfo.of(ImmutableIntList.copyOf(leftColumns), ImmutableIntList.copyOf(rightColumns));
    }

    @Override
    public SemiSortMergeJoin copy(
        RelTraitSet traitSet,
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone) {
        SemiSortMergeJoin semiSortMergeJoin =
            new SemiSortMergeJoin(
                getCluster(),
                traitSet,
                left,
                right,
                condition,
                ImmutableIntList.copyOf(leftColumns),
                ImmutableIntList.copyOf(rightColumns),
                joinType,
                operands,
                variablesSet,
                hints,
                operator,
                pushDownRelNode,
                subqueryPosition,
                leftColumns,
                rightColumns,
                collation,
                otherCondition
            );
        semiSortMergeJoin.setFixedCost(fixedCost);
        return semiSortMergeJoin;
    }

    public void setFixedCost(RelOptCost fixedCost) {
        this.fixedCost = fixedCost;
    }

    public RelOptCost getFixedCost() {
        return fixedCost;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (fixedCost != null) {
            return fixedCost;
        }
        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount = mq.getRowCount(right);
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        double mergeWeight = CostModelWeight.INSTANCE.getMergeWeight();
        double rowCount = leftRowCount + rightRowCount;
        double cpu = mergeWeight * (leftRowCount + rightRowCount);

        return planner.getCostFactory().makeCost(rowCount, cpu, 0, 0, 0);
    }

    public RexNode getOtherCondition() {
        return otherCondition;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("leftColumns", leftColumns)
            .item("rightColumns", rightColumns)
            .itemIf("otherCondition", otherCondition, otherCondition != null)
            .itemIf("operands", operands, operands != null && !operands.isEmpty())
            .item("collation", collation);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "SemiSortMergeJoin";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    public List<Integer> getLeftColumns() {
        return leftColumns;
    }

    public List<Integer> getRightColumns() {
        return rightColumns;
    }

    public List<RexNode> getOperands() {
        return operands;
    }

    public RelCollation getCollation() {
        return collation;
    }

    @Override
    public RelNode passThrough(RelTraitSet required) {
        Pair<RelTraitSet, List<RelTraitSet>> p = passThroughTraits(required);
        if (p == null) {
            return null;
        }
        int size = getInputs().size();
        assert size == p.right.size();
        List<RelNode> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            RelNode n = RelOptRule.convert(getInput(i), p.right.get(i));
            list.add(n);
        }

        List<RelFieldCollation> leftRelFieldCollationList = p.right.get(0).getCollation().getFieldCollations();
        List<RelFieldCollation> rightRelFieldCollationList = p.right.get(1).getCollation().getFieldCollations();

        List<RelFieldCollation> relFieldCollationList = new ArrayList<>();

        List<Integer> newLeftColumns = new ArrayList<>();
        List<Integer> newRightColumns = new ArrayList<>();

        for (int i = 0; i < Math.min(leftRelFieldCollationList.size(), rightRelFieldCollationList.size()); i++) {
            relFieldCollationList.add(leftRelFieldCollationList.get(i));
            newLeftColumns.add(leftRelFieldCollationList.get(i).getFieldIndex());
            newRightColumns.add(rightRelFieldCollationList.get(i).getFieldIndex());
        }
        RelCollation collation = RelCollations.of(relFieldCollationList);

        return new SemiSortMergeJoin(getCluster(), p.left, list.get(0), list.get(1), condition,
            ImmutableIntList.copyOf(newLeftColumns), ImmutableIntList.copyOf(rightColumns), joinType, operands,
            variablesSet,
            hints, operator,
            pushDownRelNode, subqueryPosition, newLeftColumns, newRightColumns, collation, otherCondition);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        return SortMergeJoin.sortMergeJoinPassThroughTraits(required, this, joinInfo);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(final RelTraitSet childTraits, final int childId) {
        return SortMergeJoin.sortMergeJoinDeriveTraits(childTraits, childId, joinInfo, getTraitSet(), left, right);
    }

    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.LEFT_FIRST;
    }

    public JoinInfo getJoinInfo() {
        return joinInfo;
    }
}

