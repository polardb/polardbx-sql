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

import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Set;

public class SemiHashJoin extends SemiJoin implements PhysicalNode {

    private SqlOperator operator;
    private RelNode pushDownRelNode;
    private List<RexNode> operands;
    private String subqueryPosition;
    private RelOptCost fixedCost;
    private final RexNode equalCondition;
    private final RexNode otherCondition;
    private final boolean runtimeFilterPushedDown;

    private boolean outerBuild;

    private boolean keepPartition = false;

    // ~ Constructors -----------------------------------------------------------

    public SemiHashJoin(
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
        RexNode equalCondition,
        RexNode otherCondition,
        boolean runtimeFilterPushedDown,
        boolean driverBuilder
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
        this.equalCondition = equalCondition;
        this.otherCondition = otherCondition;
        this.runtimeFilterPushedDown = runtimeFilterPushedDown;
        if (JoinRelType.SEMI == joinType || JoinRelType.ANTI == joinType) {
            this.outerBuild = driverBuilder;
        }
    }

    public SemiHashJoin(
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
        RexNode equalCondition,
        RexNode otherCondition,
        boolean driverBuilder
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
        this.equalCondition = equalCondition;
        this.otherCondition = otherCondition;
        this.runtimeFilterPushedDown = false;
        if (JoinRelType.SEMI == joinType || JoinRelType.ANTI == joinType) {
            this.outerBuild = driverBuilder;
        }
    }

    public SemiHashJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).leftKeys,
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).rightKeys,
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE).replace(relInput.getPartitionWise());
        this.equalCondition = relInput.getExpression("equalCondition");
        this.otherCondition = relInput.getExpression("otherCondition");
        if (relInput.get("operands") == null) {
            this.operands = ImmutableList.of();
        } else {
            this.operands = relInput.getExpressionList("operands");
        }
        this.runtimeFilterPushedDown = relInput.getBoolean("insertRf", false);
        if (JoinRelType.SEMI == joinType || JoinRelType.ANTI == joinType) {
            outerBuild = relInput.getBoolean("driverBuilder", false);
        }
        this.keepPartition = relInput.getBoolean("keepPartition", false);
    }

    public static SemiHashJoin create(
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        LogicalSemiJoin semiJoin,
        RexNode equalCondition,
        RexNode otherCondition,
        boolean driverBuilder) {
        final RelOptCluster cluster = left.getCluster();
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        return new SemiHashJoin(
            cluster,
            traitSet,
            left,
            right,
            condition,
            joinInfo.leftKeys,
            joinInfo.rightKeys,
            semiJoin.getJoinType(),
            semiJoin.getOperands(),
            semiJoin.getVariablesSet(),
            semiJoin.getHints(),
            semiJoin.getOperator(),
            semiJoin.getPushDownRelNode(),
            semiJoin.getSubqueryPosition(),
            equalCondition,
            otherCondition,
            driverBuilder);
    }

    @Override
    public SemiHashJoin copy(
        RelTraitSet traitSet,
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone) {
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        SemiHashJoin semiHashJoin =
            new SemiHashJoin(
                getCluster(),
                traitSet,
                left,
                right,
                condition,
                joinInfo.leftKeys,
                joinInfo.rightKeys,
                joinType,
                operands,
                variablesSet,
                hints,
                operator,
                pushDownRelNode,
                subqueryPosition,
                equalCondition,
                otherCondition,
                runtimeFilterPushedDown,
                outerBuild
            );
        semiHashJoin.keepPartition = keepPartition;
        semiHashJoin.setFixedCost(fixedCost);
        return semiHashJoin;
    }

    public SemiHashJoin copy(
        RelTraitSet traitSet,
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone,
        boolean runtimeFilterPushedDown) {
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        SemiHashJoin semiHashJoin =
            new SemiHashJoin(
                getCluster(),
                traitSet,
                left,
                right,
                condition,
                joinInfo.leftKeys,
                joinInfo.rightKeys,
                joinType,
                operands,
                variablesSet,
                hints,
                operator,
                pushDownRelNode,
                subqueryPosition,
                equalCondition,
                otherCondition,
                runtimeFilterPushedDown,
                outerBuild
            );
        semiHashJoin.keepPartition = keepPartition;
        semiHashJoin.setFixedCost(fixedCost);
        return semiHashJoin;
    }

    public boolean isOuterBuild() {
        return outerBuild;
    }

    public RelNode getBuildNode() {
        return outerBuild ? getOuter() : getInner();
    }

    public RelNode getProbeNode() {
        return outerBuild ? getInner() : getOuter();
    }

    public boolean isRuntimeFilterPushedDown() {
        return runtimeFilterPushedDown;
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
        if (outerBuild) {
            return computeSelfCost(planner, mq, true, joinType == JoinRelType.ANTI);
        } else {
            return computeSelfCost(planner, mq, false, false);
        }
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq, boolean outerBuild, boolean isReverseAnti) {

        final RelNode probe = !outerBuild ? left : right;
        final RelNode build = !outerBuild ? right : left;
        final double probeRowCount = mq.getRowCount(probe);
        final double buildRowCount = mq.getRowCount(build);

        double rowCount = probeRowCount + buildRowCount;
        double buildWeight = CostModelWeight.INSTANCE.getBuildWeight();
        double probeWeight = CostModelWeight.INSTANCE.getProbeWeight();

        // Increase cost weight of reverse semi/anti hash join
        if (outerBuild) {
            probeWeight = isReverseAnti ? CostModelWeight.INSTANCE.getReverseAntiProbeWeight()
                : CostModelWeight.INSTANCE.getReverseSemiProbeWeight();
        }

        double cpu = buildWeight * buildRowCount + probeWeight * probeRowCount;
        double memory = MemoryEstimator.estimateRowSizeInHashTable(build.getRowType()) * buildRowCount;

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    public RexNode getEqualCondition() {
        return equalCondition;
    }

    public RexNode getOtherCondition() {
        return otherCondition;
    }

    public boolean isKeepPartition() {
        return keepPartition;
    }

    public void setKeepPartition(boolean keepPartition) {
        this.keepPartition = keepPartition;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("equalCondition", equalCondition, equalCondition != null)
            .itemIf("otherCondition", otherCondition, otherCondition != null)
            .itemIf("operands", operands, operands != null && !operands.isEmpty())
            .itemIf("insertRf", runtimeFilterPushedDown, runtimeFilterPushedDown)
            .itemIf("driverBuilder", outerBuild, outerBuild)
            .itemIf("keepPartition", keepPartition, keepPartition)
            .itemIf("partitionWise", this.traitSet.getPartitionWise(), !this.traitSet.getPartitionWise().isTop());
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "SemiHashJoin";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);

        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty())
            .item("build", !outerBuild ? "inner" : "outer")
            .itemIf("partition", traitSet.getPartitionWise(), !traitSet.getPartitionWise().isTop());
    }

    public List<RexNode> getOperands() {
        return operands;
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        if (outerBuild) {
            return null;
        }
        return CBOUtil.passThroughTraitsForJoin(
            required, this, joinType, left.getRowType().getFieldCount(), getTraitSet());
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        final RelTraitSet childTraits, final int childId) {
        if (outerBuild) {
            return null;
        }
        return HashJoin.deriveTraitsForJoin(childTraits, childId, getTraitSet(), left, right);
    }

    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.LEFT_FIRST;
    }
}
