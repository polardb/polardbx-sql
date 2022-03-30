package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.utils.RelUtils.removeHepRelVertex;

/**
 * transform semiJoin and correlate to subQuery in logicalView
 * the rule is used to build a tree interpretable by DN
 *
 * @author shengyu
 */
public abstract class SemiJoinCorrToSubQueryRule extends RelOptRule {

    public SemiJoinCorrToSubQueryRule(RelOptRuleOperand operand, String description) {
        super(operand, "SemiJoinToSubQueryRule:" + description);
    }

    /**
     * transform semiJoin to subQuery
     */
    public static final SemiJoinCorrToSubQueryRule SEMI_JOIN = new SemiJoinCorrToSubQueryRule(
        operand(LogicalSemiJoin.class, operand(RelNode.class, any()),
            operand(RelNode.class, any())),
        "SEMI_JOIN") {
        @Override
        public void onMatch(RelOptRuleCall call) {
            handleSemi(call);
        }
    };

    protected void handleSemi(RelOptRuleCall call) {
        LogicalSemiJoin join = call.rel(0);
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        List<RexNode> leftFilters = new ArrayList<>();
        List<RexNode> rightFilters = new ArrayList<>();
        PushJoinRule.classifyFilters(join, join.getCondition(), leftFilters, rightFilters);

        LogicalSemiJoin newLogicalSemiJoin = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

        RelNode newNode = newLogicalSemiJoin.getPushDownRelNode(removeHepRelVertex(right),
            call.builder(),
            call.builder().getRexBuilder(),
            leftFilters,
            rightFilters,
            true);

        if (newNode == null) {
            return;
        }
        RelUtils.changeRowType(newNode, join.getRowType());
        call.transformTo(newNode);
    }

    /**
     * transform correlate to subQuery
     */
    public static final SemiJoinCorrToSubQueryRule CORRELATE = new SemiJoinCorrToSubQueryRule(
        operand(LogicalSemiJoin.class, operand(RelNode.class, any()),
            operand(RelNode.class, any())),
        "CORRELATE") {
        @Override
        public void onMatch(RelOptRuleCall call) {
            handleCorr(call);
        }
    };

    protected void handleCorr(RelOptRuleCall call) {
        // FIXME: PUSH DOWN CORRELATE TO LOGICALVIEW
        LogicalCorrelate logicalCorrelate = call.rel(0);
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);
    }
}
