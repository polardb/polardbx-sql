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
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.base.Preconditions;
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
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Set;

public class NLJoin extends Join implements PhysicalNode {
    //~ Instance fields --------------------------------------------------------

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;
    private RelOptCost fixedCost;

    //~ Constructors -----------------------------------------------------------

    public NLJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        boolean semiJoinDone,
        ImmutableList<RelDataTypeField> systemFieldList,
        SqlNodeList hints
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, hints);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
    }

    public NLJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
    }

    public static NLJoin create(RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
                                Set<CorrelationId> variablesSet,
                                JoinRelType joinType, boolean semiJoinDone,
                                ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        return new NLJoin(cluster, traitSet, left, right, condition,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public NLJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                       RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        NLJoin nlJoin = new NLJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints);
        nlJoin.setFixedCost(this.fixedCost);
        return nlJoin;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
    }

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override
    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "NlJoin";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    public void setFixedCost(RelOptCost cost) {
        fixedCost = cost;
    }

    public RelOptCost getFixedCost() {
        return fixedCost;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        if (fixedCost != null) {
            return fixedCost;
        }

        final RelNode streamInput;
        final RelNode buildInput;
        if (joinType != JoinRelType.RIGHT) {
            streamInput = left;
            buildInput = right;
        } else {
            streamInput = right;
            buildInput = left;
        }

        double streamRowCount = mq.getRowCount(streamInput);
        double buildRowCount = mq.getRowCount(buildInput);
        if (LogicalJoinToNLJoinRule.canUseHash(this, getCondition())) {
            // add constant to prefer hash join rather then nl join
            streamRowCount += 5;
            buildRowCount += 5;
        }

        if (Double.isInfinite(streamRowCount) || Double.isInfinite(buildRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        double rowCount = streamRowCount + buildRowCount;
        double cpu = CostModelWeight.INSTANCE.getNlWeight() * streamRowCount * buildRowCount + buildRowCount;
        double memory = buildRowCount * MemoryEstimator.estimateRowSizeInArrayList(buildInput.getRowType());

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        return CBOUtil.passThroughTraitsForJoin(
            required, this, joinType, left.getRowType().getFieldCount(), getTraitSet());
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        final RelTraitSet childTraits, final int childId) {
        return HashJoin.deriveTraitsForJoin(childTraits, childId, getTraitSet(), left, right);
    }

    @Override
    public DeriveMode getDeriveMode() {
        if (traitSet.getConvention() == MppConvention.INSTANCE) {
            return DeriveMode.BOTH;
        }

        if (joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT) {
            return DeriveMode.PROHIBITED;
        }

        return DeriveMode.LEFT_FIRST;
    }
}
