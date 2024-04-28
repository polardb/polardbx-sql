package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.AggJoinToToHashGroupJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

public class COLAggJoinToToHashGroupJoinRule extends AggJoinToToHashGroupJoinRule {

    public static final AggJoinToToHashGroupJoinRule INSTANCE = new COLAggJoinToToHashGroupJoinRule("INSTANCE");

    public COLAggJoinToToHashGroupJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    protected void createHashGroupJoin(
        RelOptRuleCall call,
        LogicalAggregate agg,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        CBOUtil.RexNodeHolder equalConditionHolder,
        CBOUtil.RexNodeHolder otherConditionHolder) {
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();
        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_JOIN)) {
            List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();
            JoinInfo joinInfo = JoinInfo.of(left, right, equalConditionHolder.getRexNode());
            for (IntPair pair : joinInfo.pairs()) {
                keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
            }
            Mappings.TargetMapping mapping =
                Mappings.createShiftMapping(right.getRowType().getFieldCount(),
                    left.getRowType().getFieldCount(),
                    0,
                    right.getRowType().getFieldCount());

            CBOUtil.columnarHashDistribution(
                keyPairList,
                join,
                left,
                right,
                mapping,
                implementationList);

            CBOUtil.columnarBroadcastDistribution(
                join,
                left,
                right,
                implementationList);
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            HashGroupJoin newAgg = HashGroupJoin.create(
                join.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
                newCondition,
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                (ImmutableList<RelDataTypeField>) join.getSystemFieldList(),
                join.getHints(),
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode(),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());

            RelOptCost fixedCost = CheckJoinHint.check(newAgg, HintType.CMD_HASH_GROUP_JOIN);
            if (fixedCost != null) {
                newAgg.setFixedCost(fixedCost);
            }
            if (newAgg.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(newAgg, newAgg.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newAgg);
            }
        }
    }
}
