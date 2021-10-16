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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.utils.ConditionPropagator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

public class JoinConditionSimplifyRule extends RelOptRule {

    public static final JoinConditionSimplifyRule INSTANCE = new JoinConditionSimplifyRule();

    protected JoinConditionSimplifyRule() {
        super(operand(Join.class, RelOptRule.any()),
            "JoinConditionSimplifyRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);

        final RexNode newJoinCondition = simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());
        if (newJoinCondition == join.getCondition()) {
            return;
        }

        Join newJoin = join.copy(
            join.getTraitSet(),
            newJoinCondition,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

        call.transformTo(newJoin);
    }

    public static RexNode simplifyCondition(RexNode condition, RexBuilder rexBuilder) {
        RexNode simpleCnfCondition = RexUtil.toSimpleCnf(rexBuilder, condition);
        final RexNode newJoinCondition;
        if (simpleCnfCondition != condition) {
            newJoinCondition = simpleCnfCondition;
        } else {
            List<RexNode> originalConjunction = RelOptUtil.conjunctions(simpleCnfCondition);
            List<RexNode> deDuplicateConjunction =
                ConditionPropagator.removeDuplicateCondition(originalConjunction, rexBuilder);
            RexNode deDuplicateCondition =
                RexUtil.composeConjunction(rexBuilder, deDuplicateConjunction, false);
            if (originalConjunction.size() == deDuplicateConjunction.size()) {
                return condition;
            } else {
                newJoinCondition = deDuplicateCondition;
            }
        }
        return newJoinCondition;
    }
}

