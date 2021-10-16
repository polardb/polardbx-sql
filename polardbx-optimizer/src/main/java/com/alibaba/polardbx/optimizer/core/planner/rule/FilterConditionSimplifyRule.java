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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

/**
 * @author dylan
 */
public class FilterConditionSimplifyRule extends RelOptRule {

    public static final FilterConditionSimplifyRule INSTANCE = new FilterConditionSimplifyRule();

    protected FilterConditionSimplifyRule() {
        super(operand(Filter.class, RelOptRule.any()),
            "FilterConditionSimplifyRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);

        RexBuilder rb = call.builder().getRexBuilder();

        RexNode condition = simplifyCondition(filter.getCondition(), filter.getCluster().getRexBuilder());
        boolean changed = filter.getCondition() != condition;

        final List<RexNode> conjunctions = new ArrayList<>();
        changed |= decomposeConjunction(condition, conjunctions);

        if (changed == false) {
            return;
        }

        RexNode newCondition = null;
        for (RexNode conjunction : conjunctions) {
            newCondition = (newCondition == null ?
                conjunction : RexUtil.flatten(rb, rb.makeCall(AND, newCondition, conjunction)));
        }
        if (newCondition == null) {
            call.transformTo(filter.getInput());
        } else {
            call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition));
        }
    }

    public static RexNode simplifyCondition(RexNode condition, RexBuilder rexBuilder) {
        List<RexNode> originalConjunction = RelOptUtil.conjunctions(condition);
        List<RexNode> deDuplicateConjunction =
            ConditionPropagator.removeDuplicateCondition(originalConjunction, rexBuilder);
        RexNode deDuplicateCondition =
            RexUtil.composeConjunction(rexBuilder, deDuplicateConjunction, false);
        if (originalConjunction.size() == deDuplicateConjunction.size()) {
            return condition;
        } else {
            return deDuplicateCondition;
        }
    }

    public static boolean decomposeConjunction(
        RexNode rexPredicate,
        List<RexNode> rexList) {
        if (rexPredicate == null) {
            return false;
        } else if (rexPredicate.isAlwaysTrue()) {
            return true;
        }

        boolean changed = false;

        if (rexPredicate.isA(SqlKind.AND)) {
            for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                changed |= decomposeConjunction(operand, rexList);
            }
        } else {
            rexList.add(rexPredicate);
        }

        return changed;
    }
}

