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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

public class BKAJoin extends Join implements LookupJoin, PhysicalNode {
    //~ Instance fields --------------------------------------------------------

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;
    private RelOptCost fixedCost;
    private RelOptCost lookupCost;
    private Index lookupIndex;

    //~ Constructors -----------------------------------------------------------

    public BKAJoin(
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

    /**
     * for externalized
     */
    public BKAJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableSet.of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
        if (this.getInner() instanceof Gather) {
            ((Gather) this.getInner()).setJoin(this);
        } else if (this.getInner() instanceof LogicalView) {
            ((LogicalView) this.getInner()).setJoin(this);
        } else if (this.getInner() instanceof Project) {
            RelNode node = ((BKAJoin) ((Project) this.getInner()).getInput()).getOuter();
            if (node instanceof Gather) {
                node = ((Gather) node).getInput();
            }
            ((LogicalIndexScan) node).setJoin(this);
        }
    }

    public static BKAJoin create(RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
                                 Set<CorrelationId> variablesSet,
                                 JoinRelType joinType, boolean semiJoinDone,
                                 ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        return new BKAJoin(cluster, traitSet, left, right, condition,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public BKAJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                        RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        BKAJoin bkaJoin = new BKAJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints);
        bkaJoin.setFixedCost(this.fixedCost);
        return bkaJoin;
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

    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "BKAJoin");

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

        RelOptCost costIfFromTableLookup = LookupJoin.getCostIfJoinFromTableLookup(mq, this);
        if (costIfFromTableLookup != null) {
            return costIfFromTableLookup;
        }

        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount = mq.getRowCount(right);

        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(this);

        double rowCount = leftRowCount + rightRowCount;
        int batchSize = plannerContext.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
        double driveSideRowCount;
        RelOptCost lookupSideCost;
        RelDataType driveRowType;
        double lookupRowCount;
        RelDataType lookupRowType;

        if (joinType == JoinRelType.RIGHT) {
            driveSideRowCount = rightRowCount;
            driveRowType = right.getRowType();
            lookupSideCost = getLookupCost(mq);
            lookupRowCount = mq.getRowCount(left);
            lookupRowType = left.getRowType();
        } else {
            driveSideRowCount = leftRowCount;
            driveRowType = left.getRowType();
            lookupSideCost = getLookupCost(mq);
            lookupRowCount = mq.getRowCount(right);
            lookupRowType = right.getRowType();
        }

        double memory = driveSideRowCount * MemoryEstimator.estimateRowSizeInArrayList(driveRowType);
        double io = Math.ceil(driveSideRowCount * lookupSideCost.getIo() / CostModelWeight.LOOKUP_NUM_PER_IO);

        Index index = getLookupIndex();
        double selectivity = 1;
        if (index != null) {
            selectivity = index.getJoinSelectivity();
        }

        // lookup cpu cost
        double cpu = driveSideRowCount * lookupSideCost.getCpu();
        // hash join cpu cost
        cpu += CostModelWeight.INSTANCE.getProbeWeight() * driveSideRowCount +
            CostModelWeight.INSTANCE.getBuildWeight() * driveSideRowCount * lookupRowCount * selectivity;

        double lookupCount = Math.ceil(driveSideRowCount / batchSize);
        double eachLookupNet = Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            lookupRowType) * lookupRowCount * selectivity / CostModelWeight.NET_BUFFER_SIZE) + lookupSideCost.getNet();
        double net = lookupCount * eachLookupNet;
        net -= lookupSideCost.getNet(); // minus lookupSide net to correct cumulative cost

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, io, net);
    }

    @Override
    public RelOptCost getLookupCost(RelMetadataQuery mq) {
        if (lookupCost != null) {
            return lookupCost;
        }

        if (joinType == JoinRelType.RIGHT) {
            lookupCost = LookupJoin.getLookupCost(mq, left);
        } else {
            lookupCost = LookupJoin.getLookupCost(mq, right);
        }

        return lookupCost;
    }

    @Override
    public Index getLookupIndex() {
        if (lookupIndex != null) {
            return lookupIndex;
        }
        lookupIndex = IndexUtil.selectJoinIndex(this, true);
        return lookupIndex;
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        return CBOUtil.passThroughTraitsForJoin(
            required, this, joinType, left.getRowType().getFieldCount(), getTraitSet());
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        final RelTraitSet childTraits, final int childId) {
        // should only derive traits (limited to distribution for now)
        if (childTraits.getConvention() == MppConvention.INSTANCE) {
            RelDistribution childDistribution = childTraits.getDistribution();
            RelCollation childCollation = childTraits.getCollation();
            if (childDistribution == RelDistributions.ANY
                || childDistribution == RelDistributions.BROADCAST_DISTRIBUTED) {
                return null;
            }
            if (childId == 0) {
                RelTraitSet joinTraitSet = getTraitSet().replace(childDistribution).replace(childCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(childTraits, right.getTraitSet()));
            } else if (childId == 1) {
                int leftFieldCount = left.getRowType().getFieldCount();
                int rightFiledCount = right.getRowType().getFieldCount();
                Mappings.TargetMapping mapping =
                    Mappings.createShiftMapping(rightFiledCount, leftFieldCount, 0, rightFiledCount);
                RelCollation newJoinCollation = childCollation.apply(mapping);
                RelTraitSet joinTraitSet =
                    getTraitSet().replace(childDistribution.apply(mapping)).replace(newJoinCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(left.getTraitSet(), childTraits));
            }
        }

        return null;
    }

    @Override
    public DeriveMode getDeriveMode() {
        if (traitSet.getConvention() == MppConvention.INSTANCE) {
            if (joinType == JoinRelType.LEFT || joinType == JoinRelType.INNER) {
                return DeriveMode.LEFT_FIRST;
            } else if (joinType == JoinRelType.RIGHT) {
                return DeriveMode.RIGHT_FIRST;
            } else {
                return DeriveMode.PROHIBITED;
            }
        }

        if (joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT) {
            return DeriveMode.PROHIBITED;
        }

        return DeriveMode.LEFT_FIRST;
    }
}
