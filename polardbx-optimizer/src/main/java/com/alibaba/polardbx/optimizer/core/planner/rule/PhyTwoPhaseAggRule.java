package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.TwoPhaseAggUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;
import java.util.Map;

public class PhyTwoPhaseAggRule extends RelOptRule {
    public PhyTwoPhaseAggRule(RelOptRuleOperand operand, String description) {
        super(operand, "PhyTwoPhaseAggRule:" + description);
    }

    private static final Predicate<AbstractRelNode> PREDICATE =
        new PredicateImpl<AbstractRelNode>() {
            public boolean test(AbstractRelNode node) {
                if (node instanceof LogicalView) {
                    // don't use two phase agg if agg is pushed
                    return !((LogicalView) node).aggIsPushed();
                }
                if ((node instanceof HashAgg)) {
                    // don't use two phase agg if two phase agg is used
                    return !(((HashAgg) node).isPartial());
                }
                return true;
            }
        };

    public static final PhyTwoPhaseAggRule INSTANCE = new PhyTwoPhaseAggRule(
        operand(HashAgg.class,
            operand(Exchange.class, operand(AbstractRelNode.class, null, PREDICATE, any()))), "COLUMNAR") {
        @Override
        public boolean matches(RelOptRuleCall call) {
            if (!super.matches(call)) {
                return false;
            }
            final HashAgg hashAgg = (HashAgg) call.rels[0];
            final Exchange exchange = (Exchange) call.rels[1];
            return ((!hashAgg.isPartial()) && (exchange.getTraitSet().getCollation().isTop()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            onMatchInstance(call);
        }
    };

    public static final PhyTwoPhaseAggRule PROJECT = new PhyTwoPhaseAggRule(
        operand(HashAgg.class,
            operand(PhysicalProject.class, operand(Exchange.class, any()))), "COLUMNAR_PROJECT") {
        @Override
        public boolean matches(RelOptRuleCall call) {
            if (!super.matches(call)) {
                return false;
            }
            final HashAgg hashAgg = (HashAgg) call.rels[0];
            final Exchange exchange = (Exchange) call.rels[2];
            return ((!hashAgg.isPartial()) && (exchange.getTraitSet().getCollation().isTop()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            onMatchProject(call);
        }
    };

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTIAL_AGG)) {
            return false;
        }
        if (!PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.PREFER_PARTIAL_AGG)) {
            return false;
        }
        // forbid agg with filter
        return !((HashAgg) call.rels[0]).getAggCallList().stream().anyMatch(AggregateCall::hasFilter);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
    }

    public void onMatchInstance(RelOptRuleCall call) {
        final HashAgg hashAgg = (HashAgg) call.rels[0];
        final Exchange exchange = (Exchange) call.rels[1];
        final RelNode relNode = call.rels[2];

        TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent = TwoPhaseAggUtil.splitAgg(hashAgg);
        if (twoPhaseAggComponent == null) {
            return;
        }
        split(call, hashAgg, exchange, relNode, twoPhaseAggComponent);

    }

    public void onMatchProject(RelOptRuleCall call) {
        final HashAgg hashAgg = (HashAgg) call.rels[0];
        final PhysicalProject project = (PhysicalProject) call.rels[1];
        final Exchange exchange = (Exchange) call.rels[2];

        TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent = TwoPhaseAggUtil.splitAgg(hashAgg);
        if (twoPhaseAggComponent == null) {
            return;
        }

        // make sure distribution columns are preserved in project
        Map<Integer, Integer> map = Maps.newHashMap();
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode node = project.getProjects().get(i);
            if (node instanceof RexInputRef) {
                map.put(((RexInputRef) node).getIndex(), i);
            }
        }
        final Mapping mapping = (Mapping) Mappings.target(
            map::get,
            exchange.getRowType().getFieldCount(),
            project.getRowType().getFieldCount());
        RelDistribution distribution = exchange.getDistribution().apply(mapping);
        if (exchange.getDistribution().getType() != distribution.getType()) {
            return;
        }
        PhysicalProject newProject = project.copy(exchange.getInput().getTraitSet(), exchange.getInput(),
            project.getProjects(), project.getRowType());
        Exchange newExchange = createExchange(exchange, newProject, distribution);
        if (newExchange == null) {
            return;
        }

        split(call, hashAgg, newExchange, newProject, twoPhaseAggComponent);
    }

    private void split(RelOptRuleCall call,
                       HashAgg hashAgg,
                       Exchange exchange,
                       RelNode bottom,
                       TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent) {
        List<RexNode> childExps = twoPhaseAggComponent.getProjectChildExps();
        List<AggregateCall> globalAggCalls = twoPhaseAggComponent.getGlobalAggCalls();
        ImmutableBitSet globalAggGroupSet = twoPhaseAggComponent.getGlobalAggGroupSet();
        List<AggregateCall> partialAggCalls = twoPhaseAggComponent.getPartialAggCalls();
        ImmutableBitSet partialAggGroupSet = twoPhaseAggComponent.getPartialAggGroupSet();

        // make sure distribution columns are preserved in partial agg
        Map<Integer, Integer> map = Maps.newHashMap();
        int inputLoc = -1;
        for (int i = 0; i < partialAggGroupSet.cardinality(); i++) {
            inputLoc = partialAggGroupSet.nextSetBit(inputLoc + 1);
            map.put(inputLoc, i);
        }
        final Mapping mapping = (Mapping) Mappings.target(
            map::get,
            bottom.getRowType().getFieldCount(),
            partialAggGroupSet.cardinality());
        RelDistribution distribution = exchange.getDistribution().apply(mapping);
        if (exchange.getDistribution().getType() != distribution.getType()) {
            return;
        }

        RelNode partialHashAgg =
            HashAgg.createPartial(bottom.getTraitSet(),
                bottom,
                partialAggGroupSet,
                hashAgg.getGroupSets(),
                partialAggCalls);

        Exchange newExchange = createExchange(exchange, partialHashAgg, distribution);
        if (newExchange == null) {
            return;
        }

        HashAgg globalHashAgg = hashAgg.copy(hashAgg.getTraitSet(), newExchange, hashAgg.indicator,
            globalAggGroupSet, ImmutableList.of(globalAggGroupSet), globalAggCalls);

        PhysicalProject project = new PhysicalProject(
            hashAgg.getCluster(), globalHashAgg.getTraitSet(), globalHashAgg, childExps, hashAgg.getRowType());

        if (ProjectRemoveRule.isTrivial(project)) {
            call.transformTo(globalHashAgg);
        } else {
            call.transformTo(project);
        }
    }

    Exchange createExchange(Exchange oldExchange, RelNode input, RelDistribution distribution) {
        if (oldExchange instanceof MppExchange) {
            return MppExchange.create(input, distribution);
        }
        if (oldExchange instanceof ColumnarExchange) {
            return ColumnarExchange.create(input, distribution);
        }
        return null;
    }
}

