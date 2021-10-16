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

import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

/**
 * @author hongxi.chx
 */
public class HashGroupJoin extends GroupJoin {
    private final RexNode equalCondition;
    private final RexNode otherCondition;

    private final boolean parallel;
    private final boolean parallelBuild;

    //~ Constructors -----------------------------------------------------------

    public HashGroupJoin(
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
        boolean parallel,
        boolean parallelBuild,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, semiJoinDone, systemFieldList, hints,
            indicator, groupSet, groupSets, aggCalls);
        this.equalCondition = equalCondition;
        this.otherCondition = otherCondition;
        this.parallelBuild = parallelBuild;
        this.parallel = parallel;

    }

    public HashGroupJoin(RelInput relInput) {
        super(relInput);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        this.equalCondition = relInput.getExpression("equalCondition");
        this.otherCondition = relInput.getExpression("otherCondition");
        this.parallel = relInput.getBoolean("parallel", false);
        this.parallelBuild = relInput.getBoolean("parallelBuild", false);
    }

    public static HashGroupJoin create(RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
                                       JoinRelType joinType, boolean semiJoinDone,
                                       ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints,
                                       RexNode equalCondition, RexNode otherCondition, ImmutableBitSet groupSet,
                                       List<ImmutableBitSet> groupSets,
                                       List<AggregateCall> aggCalls) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(DrdsConvention.INSTANCE);
        return new HashGroupJoin(cluster, traitSet, left, right, condition,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints, equalCondition, otherCondition,
            false, false, false, groupSet, groupSets, aggCalls);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public HashGroupJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                              RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        HashGroupJoin hashJoin = new HashGroupJoin(getCluster(),
            traitSet,
            left,
            right,
            conditionExpr,
            variablesSet,
            joinType,
            semiJoinDone,
            systemFieldList,
            hints,
            equalCondition,
            otherCondition,
            parallel,
            parallelBuild,
            indicator,
            groupSet,
            groupSets,
            aggCalls);
        hashJoin.setFixedCost(this.fixedCost);
        return hashJoin;
    }

    @Override
    public HashJoin copyAsJoin(RelTraitSet traitSet, RexNode conditionExpr) {
        HashJoin hashJoin = new HashJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints,
            equalCondition, otherCondition, false, false);
        hashJoin.setFixedCost(this.fixedCost);
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
        final RelWriter relWriter = super.explainTerms(pw)
            .itemIf("equalCondition", equalCondition, equalCondition != null)
            .itemIf("otherCondition", otherCondition, otherCondition != null)
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
            .itemIf("parallel", parallel, parallel)
            .itemIf("parallelBuild", parallelBuild, parallelBuild)
            .item("group", groupSet)
            .itemIf("groups", groupSets, getGroupType() != Aggregate.Group.SIMPLE)
            .itemIf("indicator", indicator, indicator)
            .itemIf("aggs", aggCalls, pw.nest());
        if (!pw.nest()) {
            for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
            }
        }
        return pw;
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
        String name = parallel ? "ParallelHashJoin" : "HashGroupJoin";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        if (fixedCost != null) {
            return fixedCost;
        }

        final RelNode buildInput;
        final RelNode streamInput;
        if (joinType != JoinRelType.RIGHT) {
            streamInput = right;
            buildInput = left;
        } else {
            streamInput = left;
            buildInput = right;
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
        double memory = MemoryEstimator.estimateRowSizeInHashTable(buildInput.getRowType()) * buildRowCount;

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public RelDataType deriveRowType() {
        return super.deriveRowType();
    }

    public RexNode getEqualCondition() {
        return equalCondition;
    }

    public RexNode getOtherCondition() {
        return otherCondition;
    }
}
