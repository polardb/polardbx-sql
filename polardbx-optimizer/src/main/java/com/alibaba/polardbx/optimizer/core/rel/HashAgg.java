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
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
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
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.CPU_START_UP_COST;

public class HashAgg extends Aggregate {

    private final boolean passive; // true for V2

    private final boolean partial;
    private final boolean isPushDown;

    //~ Constructors -----------------------------------------------------------

    public HashAgg(RelOptCluster cluster,
                   RelTraitSet traitSet,
                   RelNode child,
                   boolean indicator,
                   ImmutableBitSet groupSet,
                   List<ImmutableBitSet> groupSets,
                   List<AggregateCall> aggCalls,
                   boolean partial,
                   boolean passive,
                   boolean isPushDown) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.partial = partial;
        this.passive = passive;
        this.isPushDown = isPushDown;
    }

    public HashAgg(RelOptCluster cluster,
                   RelTraitSet traitSet,
                   RelNode child,
                   boolean indicator,
                   ImmutableBitSet groupSet,
                   List<ImmutableBitSet> groupSets,
                   List<AggregateCall> aggCalls,
                   boolean partial,
                   boolean passive) {
        this(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls, partial, passive, false);
    }

    public HashAgg(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInput(),
            relInput.getBoolean("indicator", false),
            relInput.getBitSet("group"),
            relInput.getBitSetList("groups"),
            relInput.getAggregateCalls("aggs"));
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        this.partial = relInput.getBoolean("partial", false);
        this.passive = relInput.getBoolean("passive", false);
        this.isPushDown = relInput.getBoolean("isPushDown", false);
    }

    public static HashAgg create(RelTraitSet traitSet, final RelNode input,
                                 ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                 List<AggregateCall> aggCalls) {
        return create(traitSet, input, false, groupSet, groupSets, aggCalls, false);
    }

    private static HashAgg create(RelTraitSet traitSet, final RelNode input,
                                  boolean indicator,
                                  ImmutableBitSet groupSet,
                                  List<ImmutableBitSet> groupSets,
                                  List<AggregateCall> aggCalls,
                                  boolean isPushDown) {
        final RelOptCluster cluster = input.getCluster();
        return new HashAgg(cluster, traitSet, input, indicator, groupSet,
            groupSets, aggCalls, false, false, isPushDown);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public HashAgg copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new HashAgg(getCluster(),
            traitSet,
            input,
            indicator,
            groupSet,
            groupSets,
            aggCalls,
            partial,
            passive,
            isPushDown);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name;
        if (partial) {
            name = "PartialHashAgg";
        } else {
            name = "HashAgg";
        }
        pw.item(RelDrdsWriter.REL_NAME, name);

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

        final double hashAggWeight = CostModelWeight.INSTANCE.getHashAggWeight();
        final double useAggSize =
            aggCalls.stream().filter(x -> x.getAggregation().kind != SqlKind.__FIRST_VALUE
                && x.getAggregation().getKind() != SqlKind.FIRST_VALUE).count();
        final double cpu = CPU_START_UP_COST + rowCount * hashAggWeight * (groupSet.cardinality() + useAggSize);
        final double memory = MemoryEstimator.estimateRowSizeInHashTable(getRowType()) * mq.getRowCount(this);

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("partial", partial, partial)
            .itemIf("passive", passive, passive)
            .itemIf("isPushDown", isPushDown, isPushDown);
    }

    public boolean isPartial() {
        return partial;
    }
}
