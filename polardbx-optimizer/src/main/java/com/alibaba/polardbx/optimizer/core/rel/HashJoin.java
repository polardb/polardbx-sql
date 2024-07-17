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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
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
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;

public class HashJoin extends Join implements PhysicalNode {
    //~ Instance fields --------------------------------------------------------

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;
    private RexNode equalCondition;
    private RexNode otherCondition;
    private final boolean runtimeFilterPushedDown;
    private RelOptCost fixedCost;
    private boolean outerBuild;

    private boolean keepPartition = false;

    //~ Constructors -----------------------------------------------------------

    public HashJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        boolean semiJoinDone,
        ImmutableList<RelDataTypeField> systemFieldList,
        SqlNodeList hints,
        RexNode equalCondition,
        RexNode otherCondition,
        boolean runtimeFilterPushedDown,
        boolean driverBuilder
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, hints);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
        this.equalCondition = equalCondition;
        this.otherCondition = otherCondition;
        this.runtimeFilterPushedDown = runtimeFilterPushedDown;
        if (JoinRelType.LEFT == joinType || JoinRelType.RIGHT == joinType) {
            this.outerBuild = driverBuilder;
        }
    }

    public HashJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE).replace(relInput.getPartitionWise());
        this.equalCondition = relInput.getExpression("equalCondition");
        this.otherCondition = relInput.getExpression("otherCondition");
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
        this.runtimeFilterPushedDown = relInput.getBoolean("runtimeFilterPushedDown", false);
        if (JoinRelType.LEFT == joinType || JoinRelType.RIGHT == joinType) {
            outerBuild = relInput.getBoolean("driverBuilder", false);
        }
        this.keepPartition = relInput.getBoolean("keepPartition", false);
    }

    public static HashJoin create(RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
                                  Set<CorrelationId> variablesSet,
                                  JoinRelType joinType, boolean semiJoinDone,
                                  ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints,
                                  RexNode equalCondition, RexNode otherCondition, boolean driverBuilder) {
        final RelOptCluster cluster = left.getCluster();
        return new HashJoin(cluster, traitSet, left, right, condition,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints, equalCondition, otherCondition,
            false, driverBuilder);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public HashJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                         RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        HashJoin hashJoin = new HashJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints,
            null, null, runtimeFilterPushedDown, outerBuild);
        hashJoin.keepPartition = keepPartition;
        hashJoin.setFixedCost(this.fixedCost);

        // Reset equalCondition and otherCondition
        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.checkHashJoinCondition(hashJoin, conditionExpr, hashJoin.getLeft().getRowType().getFieldCount(),
            equalConditionHolder, otherConditionHolder);

        RexNode equalCondition = equalConditionHolder.getRexNode();
        RexNode otherCondition = otherConditionHolder.getRexNode();

        hashJoin.setEqualCondition(equalCondition);
        hashJoin.setOtherCondition(otherCondition);

        return hashJoin;
    }

    public HashJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RexNode equalCondition,
                         RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        HashJoin hashJoin = new HashJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints,
            equalCondition, otherCondition, runtimeFilterPushedDown, outerBuild);
        hashJoin.setFixedCost(this.fixedCost);
        hashJoin.keepPartition = keepPartition;
        return hashJoin;
    }

    public HashJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                         RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone,
                         boolean runtimeFilterPushedDown) {
        HashJoin hashJoin = new HashJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints,
            null, null, runtimeFilterPushedDown, outerBuild);
        hashJoin.setFixedCost(this.fixedCost);
        hashJoin.keepPartition = keepPartition;

        // Reset equalCondition and otherCondition
        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.checkHashJoinCondition(hashJoin, conditionExpr, hashJoin.getLeft().getRowType().getFieldCount(),
            equalConditionHolder, otherConditionHolder);

        RexNode equalCondition = equalConditionHolder.getRexNode();
        RexNode otherCondition = otherConditionHolder.getRexNode();

        hashJoin.setEqualCondition(equalCondition);
        hashJoin.setOtherCondition(otherCondition);

        return hashJoin;
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
            .itemIf("equalCondition", equalCondition, equalCondition != null)
            .itemIf("otherCondition", otherCondition, otherCondition != null)
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
            .itemIf("runtimeFilterPushedDown", runtimeFilterPushedDown, runtimeFilterPushedDown)
            .itemIf("driverBuilder", outerBuild, outerBuild)
            .itemIf("partitionWise", this.traitSet.getPartitionWise(), !this.traitSet.getPartitionWise().isTop())
            .itemIf("keepPartition", keepPartition, keepPartition);
    }

    public boolean isRuntimeFilterPushedDown() {
        return runtimeFilterPushedDown;
    }

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override
    public ImmutableList<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "HashJoin";

        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        String buildString = "";
        if (outerBuild) {
            if (joinType == JoinRelType.RIGHT) {
                buildString = "right";
            } else if (joinType == JoinRelType.LEFT) {
                buildString = "left";
            }
        }

        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty())
            .itemIf("build", buildString, StringUtils.isNotEmpty(buildString))
            .itemIf("partition", traitSet.getPartitionWise(), !traitSet.getPartitionWise().isTop());
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
        if (outerBuild && (JoinRelType.LEFT == joinType || JoinRelType.RIGHT == joinType) || outerBuild) {
            this.outerBuild = true;
            return computeSelfCost(planner, mq, true);
        } else {
            return computeSelfCost(planner, mq, false);
        }

    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq, boolean outerBuild) {
        if (fixedCost != null) {
            return fixedCost;
        }

        final RelNode buildInput;
        final RelNode streamInput;
        if (joinType != JoinRelType.RIGHT) {
            if (joinType == JoinRelType.LEFT && outerBuild) {
                streamInput = right;
                buildInput = left;
            } else {
                streamInput = left;
                buildInput = right;
            }
        } else {
            if (outerBuild) {
                streamInput = left;
                buildInput = right;
            } else {
                streamInput = right;
                buildInput = left;
            }
        }

        final double streamRowCount = mq.getRowCount(streamInput);
        final double buildRowCount = mq.getRowCount(buildInput);

        if (Double.isInfinite(streamRowCount) || Double.isInfinite(buildRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        double buildWeight = CostModelWeight.INSTANCE.getBuildWeight();
        double probeWeight = CostModelWeight.INSTANCE.getProbeWeight();

        double rowCount = streamRowCount + buildRowCount;

        double cpu = buildWeight * buildRowCount + probeWeight * streamRowCount;
        //scan null row from array,the arrays length is build size
        if (outerBuild) {
            cpu += buildRowCount;
        }
        double memory = MemoryEstimator.estimateRowSizeInHashTable(buildInput.getRowType()) * buildRowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    public RexNode getEqualCondition() {
        return equalCondition;
    }

    public RexNode getOtherCondition() {
        return otherCondition;
    }

    public boolean isOuterBuild() {
        return outerBuild;
    }

    // TODO: Change this if hybrid is enabled
    public RelNode getBuildNode() {
        return outerBuild ? getOuter() : getInner();
    }

    public RelNode getProbeNode() {
        return outerBuild ? getInner() : getOuter();
    }

    public boolean isKeepPartition() {
        return keepPartition;
    }

    public void setKeepPartition(boolean keepPartition) {
        this.keepPartition = keepPartition;
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
        return deriveTraitsForJoin(childTraits, childId, getTraitSet(), left, right);
    }

    public static Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForJoin(final RelTraitSet childTraits,
                                                                           final int childId,
                                                                           final RelTraitSet joinTraits,
                                                                           RelNode left,
                                                                           RelNode right) {
        if (childTraits.getConvention() == MppConvention.INSTANCE) {
            RelDistribution childDistribution = childTraits.getDistribution();
            RelCollation childCollation = childTraits.getCollation();
            if (childDistribution == RelDistributions.ANY
                || childDistribution == RelDistributions.BROADCAST_DISTRIBUTED) {
                return null;
            }
            if (childId == 0 && right.getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                RelTraitSet joinTraitSet = joinTraits.replace(childDistribution).replace(childCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(childTraits, right.getTraitSet()));
            } else if (childId == 1 && left.getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                int leftFieldCount = left.getRowType().getFieldCount();
                int rightFiledCount = right.getRowType().getFieldCount();
                Mappings.TargetMapping mapping =
                    Mappings.createShiftMapping(rightFiledCount, leftFieldCount, 0, rightFiledCount);
                RelCollation newJoinCollation = childCollation.apply(mapping);
                RelTraitSet joinTraitSet =
                    joinTraits.replace(childDistribution.apply(mapping)).replace(newJoinCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(left.getTraitSet(), childTraits));
            }
        }
        return null;
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

    public void setEqualCondition(RexNode equalCondition) {
        this.equalCondition = equalCondition;
    }

    public void setOtherCondition(RexNode otherCondition) {
        this.otherCondition = otherCondition;
    }
}
