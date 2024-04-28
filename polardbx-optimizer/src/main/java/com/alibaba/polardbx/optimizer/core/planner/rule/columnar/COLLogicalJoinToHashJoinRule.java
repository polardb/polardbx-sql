package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalJoinToHashJoinRule extends LogicalJoinToHashJoinRule {
    public static final LogicalJoinToHashJoinRule INSTANCE = new COLLogicalJoinToHashJoinRule("INSTANCE");
    public static final LogicalJoinToHashJoinRule OUTER_INSTANCE =
        new COLLogicalJoinToHashJoinRule(true, "OUTER_INSTANCE");

    COLLogicalJoinToHashJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    COLLogicalJoinToHashJoinRule(boolean outDriver, String desc) {
        super(outDriver, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createHashJoin(RelOptRuleCall call,
                                  LogicalJoin join,
                                  RelNode left,
                                  RelNode right,
                                  RexNode newCondition,
                                  CBOUtil.RexNodeHolder equalConditionHolder,
                                  CBOUtil.RexNodeHolder otherConditionHolder) {
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        JoinInfo joinInfo = JoinInfo.of(left, right, equalConditionHolder.getRexNode());
        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();

        for (IntPair pair : joinInfo.pairs()) {
            keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
        }

        Mappings.TargetMapping mapping =
            Mappings.createShiftMapping(right.getRowType().getFieldCount(),
                left.getRowType().getFieldCount(),
                0,
                right.getRowType().getFieldCount());

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_JOIN)) {
            CBOUtil.columnarHashDistribution(
                keyPairList,
                join,
                left,
                right,
                mapping,
                implementationList);
        }
        if (CollectionUtils.isEmpty(implementationList)) {
            // default hash join
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                join.getCluster().getTypeFactory(), join, joinInfo.leftKeys, joinInfo.rightKeys);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, joinInfo.leftKeys);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, joinInfo.rightKeys);
            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
            implementationList
                .add(Pair.of(hashRight.getTraitSet().getDistribution().apply(mapping),
                    Pair.of(hashLeft, hashRight)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            CBOUtil.columnarBroadcastDistribution(
                join,
                left,
                right,
                implementationList);
        }
        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            HashJoin hashJoin = HashJoin.create(
                call.getPlanner().emptyTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
                newCondition,
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList()),
                join.getHints(),
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode(),
                outDriver);

            HintType cmdHashJoin = HintType.CMD_HASH_JOIN;
            if (outDriver) {
                cmdHashJoin = HintType.CMD_HASH_OUTER_JOIN;
            }
            RelOptCost fixedCost = CheckJoinHint.check(join, cmdHashJoin);
            if (fixedCost != null) {
                hashJoin.setFixedCost(fixedCost);
            }

            if (join.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(hashJoin, hashJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(hashJoin);
            }
        }
    }
}
