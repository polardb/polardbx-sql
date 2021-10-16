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

package org.apache.calcite.rel.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

/**
 * @author hongxi.chx
 */
public abstract class GroupJoin extends Join {

    //TODO final is removed, need add it
    public boolean indicator;
    protected List<AggregateCall> aggCalls;
    protected ImmutableBitSet groupSet;
    public ImmutableList<ImmutableBitSet> groupSets;

    protected final boolean semiJoinDone;
    protected final ImmutableList<RelDataTypeField> systemFieldList;
    protected RelOptCost fixedCost;

    private RelDataType joinRowType;

    //~ Constructors -----------------------------------------------------------

    public GroupJoin(
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
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, hints);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
        this.indicator = indicator; // true is allowed, but discouraged
        this.aggCalls = ImmutableList.copyOf(aggCalls);
        this.groupSet = Preconditions.checkNotNull(groupSet);
        if (groupSets == null) {
            this.groupSets = ImmutableList.of(groupSet);
        } else {
            this.groupSets = ImmutableList.copyOf(groupSets);
            assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
            for (ImmutableBitSet set : groupSets) {
                assert groupSet.contains(set);
            }
        }
        joinRowType = SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
            right.getRowType(), joinType, getCluster().getTypeFactory(), null,
            getSystemFieldList());
        assert groupSet.length() <= joinRowType.getFieldCount();
        for (AggregateCall aggCall : aggCalls) {
//      assert typeMatchesInferred(aggCall, Litmus.THROW);
            Preconditions.checkArgument(aggCall.filterArg < 0
                    || isPredicate(aggCall.filterArg),
                "filter must be BOOLEAN NOT NULL");
        }

    }

    public GroupJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }

        this.indicator = relInput.getBoolean("indicator", false);
        this.groupSet = relInput.getBitSet("group");
        final List<ImmutableBitSet> groups = relInput.getBitSetList("groups");
        if (groups == null) {
            this.groupSets = ImmutableList.of(groupSet);
        } else {
            this.groupSets = ImmutableList.copyOf(groups);
            assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
            for (ImmutableBitSet set : groupSets) {
                assert groupSet.contains(set);
            }
        }
        joinRowType = SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
            right.getRowType(), joinType, getCluster().getTypeFactory(), null,
            getSystemFieldList());
        this.aggCalls = relInput.getAggregateCalls("aggs");

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
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
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

    public Aggregate.Group getGroupType() {
        return Aggregate.Group.induce(groupSet, groupSets);
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
        String name = "GroupJoin";
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

        double buildWeight = 1;
        double probeWeight = 11;

        double rowCount = streamRowCount + buildRowCount;

        double cpu = buildWeight * buildRowCount + probeWeight * streamRowCount;
        double memory = buildRowCount;

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    //from agg
    private boolean isPredicate(int index) {
        final RelDataType type =
            joinRowType.getFieldList().get(index).getType();
        return type.getSqlTypeName() == SqlTypeName.BOOLEAN
            && !type.isNullable();
    }

    public boolean isIndicator() {
        return indicator;
    }

    public List<AggregateCall> getAggCallList() {
        return aggCalls;
    }
    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    public ImmutableList<ImmutableBitSet> getGroupSets() {
        return groupSets;
    }

    public RelDataType getJoinRowType() {
        return joinRowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return LogicalAggregate.deriveRowType(getCluster().getTypeFactory(), super.deriveRowType(),
            indicator, groupSet, groupSets, aggCalls);
    }

    public int getIndicatorCount() {
        return indicator ? groupSet.cardinality() : 0;
    }

    public abstract Join copyAsJoin(RelTraitSet traitSet, RexNode conditionExpr);

    @Override
    public boolean isValid(Litmus litmus, Context context) {
        return litmus.succeed();
    }
}
