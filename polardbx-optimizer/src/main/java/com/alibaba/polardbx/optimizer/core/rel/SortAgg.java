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
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.CPU_START_UP_COST;
import static java.util.Objects.requireNonNull;

public class SortAgg extends Aggregate implements PhysicalNode {
    //~ Constructors -----------------------------------------------------------

    public SortAgg(RelOptCluster cluster,
                   RelTraitSet traitSet,
                   RelNode child,
                   boolean indicator,
                   ImmutableBitSet groupSet,
                   List<ImmutableBitSet> groupSets,
                   List<AggregateCall> aggCalls) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
    }

    public SortAgg(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInput(),
            relInput.getBoolean("indicator", false),
            relInput.getBitSet("group"),
            relInput.getBitSetList("groups"),
            relInput.getAggregateCalls("aggs"));
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
    }

    public static SortAgg create(RelTraitSet traitSet, final RelNode input,
                                 ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                 List<AggregateCall> aggCalls) {
        return create(traitSet, input, false, groupSet, groupSets, aggCalls);
    }

    private static SortAgg create(RelTraitSet traitSet, final RelNode input,
                                  boolean indicator,
                                  ImmutableBitSet groupSet,
                                  List<ImmutableBitSet> groupSets,
                                  List<AggregateCall> aggCalls) {
        final RelOptCluster cluster = input.getCluster();
        return new SortAgg(cluster, traitSet, input, indicator, groupSet,
            groupSets, aggCalls);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public SortAgg copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new SortAgg(getCluster(),
            traitSet,
            input,
            indicator,
            groupSet,
            groupSets,
            aggCalls);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "SortAgg");

        List<String> groupList = new ArrayList<String>(groupSet.length());

        for (int groupIndex : groupSet.asList()) {
            RexExplainVisitor visitor = new RexExplainVisitor(this);
            groupList.add(visitor.getField(groupIndex).getKey());
        }
        pw.itemIf("group", StringUtils.join(groupList, ","), !groupList.isEmpty());
        int groupCount = groupSet.cardinality();
        for (int i = groupCount; i < getRowType().getFieldCount(); i++) {
            RexExplainVisitor visitor = new RexExplainVisitor(this);
            aggCalls.get(i - groupCount).accept(visitor);
            pw.item(rowType.getFieldList().get(i).getKey(), visitor.toSqlString());
        }

        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this.input);
        if (Double.isInfinite(rowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        final double weight;
        if (!groupSet.isEmpty()) {
            weight = CostModelWeight.INSTANCE.getSortAggWeight();
        } else {
            // makes it prefer to HashAgg when there is only one group (aka. scalar aggregate)
            weight = CostModelWeight.INSTANCE.getHashAggWeight() * 1.1;
        }
        final double useAggSize =
            aggCalls.stream().filter(x -> x.getAggregation().kind != SqlKind.__FIRST_VALUE
                && x.getAggregation().getKind() != SqlKind.FIRST_VALUE).count();
        // 1 for grouping
        final double cpu = CPU_START_UP_COST + rowCount * weight * (1 + useAggSize);
        return planner.getCostFactory().makeCost(rowCount, cpu, 0, 0, 0);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        RelTraitSet childTraits, int childId) {
        return null;
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        if (!isSimple(this)) {
            return null;
        }

        if (required.getConvention() != this.getConvention()) {
            return null;
        }

        if (required.getConvention() == MppConvention.INSTANCE) {
            return null;
        }

        final RelDistribution inputDistribution = RelDistributions.ANY;
        ;

        RelTraitSet inputTraits = getInput().getTraitSet();
        RelCollation collation = requireNonNull(required.getCollation(),
            () -> "collation trait is null, required traits are " + required);
        ImmutableBitSet requiredKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
        ImmutableBitSet groupKeys = ImmutableBitSet.range(groupSet.cardinality());

        Mappings.TargetMapping mapping = Mappings.source(groupSet.toList(),
            input.getRowType().getFieldCount());

        if (requiredKeys.equals(groupKeys)) {
            RelCollation inputCollation = RexUtil.apply(mapping, collation);
            return Pair.of(required, ImmutableList.of(inputTraits.replace(inputCollation).replace(inputDistribution)));
        } else if (groupKeys.contains(requiredKeys)) {
            // group by a,b,c order by c,b
            List<RelFieldCollation> list = new ArrayList<>(collation.getFieldCollations());

            // try to keep all column order in same direction
            // because mysql with lower version can not deal with mix-order well
            final RelFieldCollation.Direction direction;
            final RelFieldCollation.NullDirection nullDirection;
            if (!list.isEmpty()) {
                direction = list.get(0).direction;
                nullDirection = list.get(0).nullDirection;
            } else {
                direction = RelFieldCollation.Direction.ASCENDING;
                nullDirection = RelFieldCollation.NullDirection.FIRST;
            }

            groupKeys.except(requiredKeys).forEach(k -> list.add(
                new RelFieldCollation(k, direction, nullDirection)));
            RelCollation aggCollation = RelCollations.of(list);
            RelCollation inputCollation = RexUtil.apply(mapping, aggCollation);
            return Pair.of(traitSet.replace(aggCollation).replace(required.getTrait(RelDistributionTraitDef.INSTANCE)),
                ImmutableList.of(inputTraits.replace(inputCollation).replace(inputDistribution)));
        }

        // Group keys doesn't contain all the required keys, e.g.
        // group by a,b order by a,b,c
        // nothing we can do to propagate traits to child nodes.
        return null;
    }
}

