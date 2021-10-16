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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author dylan
 */
public class PhysicalFilter extends Filter implements PhysicalNode {
    protected final ImmutableSet<CorrelationId> variablesSet;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PhysicalFilter.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param child Input relational expression
     * @param condition Boolean expression which determines whether a row is
     * allowed to pass
     * @param variablesSet Correlation variables set by this relational expression
     * to be used by nested expressions
     */
    public PhysicalFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition,
        ImmutableSet<CorrelationId> variablesSet) {
        super(cluster, traitSet, child, condition);
        this.variablesSet = Preconditions.checkNotNull(variablesSet);
    }

    public PhysicalFilter(RelInput input) {
        super(input);
        if (input.getIntegerList("variablesSet") != null) {
            Set<CorrelationId> correlationIdSet = new HashSet<>();
            for (Integer id : input.getIntegerList("variablesSet")) {
                correlationIdSet.add(new CorrelationId(id));
            }
            this.variablesSet = ImmutableSet.<CorrelationId>copyOf(correlationIdSet);
        } else {
            this.variablesSet = ImmutableSet.<CorrelationId>of();
        }
    }

    public static PhysicalFilter create(final RelNode input, RexNode condition) {
        return create(input, condition, ImmutableSet.<CorrelationId>of());
    }

    public static PhysicalFilter create(final RelNode input, RexNode condition,
                                        ImmutableSet<CorrelationId> variablesSet) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.filter(mq, input);
                    }
                })
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                new Supplier<RelDistribution>() {
                    public RelDistribution get() {
                        return RelMdDistribution.filter(mq, input);
                    }
                });
        return new PhysicalFilter(cluster, traitSet, input, condition, variablesSet);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }

    @Override
    public PhysicalFilter copy(RelTraitSet traitSet, RelNode input,
                               RexNode condition) {
        return new PhysicalFilter(getCluster(), traitSet, input, condition, variablesSet);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> variableSet) {
        Set<RexFieldAccess> rexFieldAccesses = Sets.newHashSet();
        rexFieldAccesses.addAll(RexUtil.findFieldAccessesDeep(condition));
        rexFieldAccesses.stream().map(RexFieldAccess::getReferenceExpr).map(rex -> ((RexCorrelVariable) rex).getId())
            .forEach(id -> variableSet.add(id));
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "Filter");

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        pw.item("condition", visitor.toSqlString());
        pw.itemIf("cor", variablesSet, !variablesSet.isEmpty());
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // no cost for filter
        return planner.getCostFactory().makeCost(1, 1, 0, 0, 0);
    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        return deepEquals0(obj)
            && variablesSet.equals(((PhysicalFilter) obj).variablesSet);
    }

    @Override
    public int deepHashCode() {
        return Objects.hash(deepHashCode0(), variablesSet);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        RelTraitSet required) {
        RelCollation collation = required.getCollation();
        RelDistribution distribution = required.getDistribution();
        Convention convention = required.getConvention();
        if (collation == null || collation == RelCollations.EMPTY) {
            if (convention == MppConvention.INSTANCE) {
                if (distribution == null || distribution == RelDistributions.ANY) {
                    return null;
                }
            } else {
                return null;
            }
        }
        RelTraitSet traits = traitSet.replace(collation).replace(distribution);
        return Pair.of(traits, ImmutableList.of(traits));
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        final RelTraitSet childTraits, final int childId) {
        if (traitSet.getConvention() == MppConvention.INSTANCE) {
            RelCollation collation = childTraits.getCollation();
            RelDistribution distribution = childTraits.getDistribution();
            if (distribution == null || distribution == RelDistributions.ANY) {
                return null;
            }
            RelTraitSet traits = traitSet.replace(collation).replace(distribution);
            return Pair.of(traits, ImmutableList.of(traits));
        } else {
            return null;
        }
    }
}

