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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.utils.ConditionPropagator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinConditionPruningRule extends RelOptRule {

    public static final JoinConditionPruningRule INSTANCE =
        new JoinConditionPruningRule(Join.class);

    //~ Constructors -----------------------------------------------------------

    public JoinConditionPruningRule(
        Class<? extends Join> joinClass) {
        super(operand(joinClass, any()));
    }

    //~ Methods ----------------------------------------------------------------

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_JOIN_CONDITION_PRUNING);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        if (join.getJoinType() != JoinRelType.INNER || !(join instanceof HashJoin)) {
            return;
        }

        // remove some conditions
        List<RexNode> joinConditions = new ArrayList<>();
        List<RexNode> conjunctions = RelOptUtil.conjunctions(join.getCondition());

        if (conjunctions.size() < 2
            || conjunctions.stream().anyMatch(r -> r.getKind() != SqlKind.EQUALS)) {
            return;
        }

        // Get the total equivalence sets by all conditions from the pull-up.
        List<BitSet> equitySets = ConditionPropagator.buildMergedEquitySet(conjunctions);

        // Record the hit count of each equivalence set
        Map<BitSet, Integer> hitCounts = new HashMap<>();
        equitySets.forEach(bitset -> hitCounts.putIfAbsent(bitset, 0));

        for (int i = 0; i < conjunctions.size(); i++) {
            RexNode joinCondition = conjunctions.get(i);
            if (joinCondition.getKind() == SqlKind.EQUALS && joinCondition instanceof RexCall) {
                // for equal-condition
                RexCall filterCall = (RexCall) joinCondition;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);

                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    int rightIndex = ((RexInputRef) rightRexNode).getIndex();

                    // check all distinct equivalence set
                    BitSet equivalenceSet = hitCounts.keySet().stream()
                        .filter(bitSet -> bitSet.get(leftIndex) && bitSet.get(rightIndex))
                        .findFirst().orElse(null);

                    if (equivalenceSet != null) {
                        int hits = hitCounts.get(equivalenceSet);
                        if (hits == 0) {
                            // preserve this condition
                            joinConditions.add(joinCondition);
                        }

                        hitCounts.put(equivalenceSet, ++hits);
                    } else {
                        joinConditions.add(joinCondition);
                    }
                }
            } else {
                joinConditions.add(joinCondition);
            }
        }

        if (joinConditions.size() == conjunctions.size()) {
            // No condition has been removed.
            return;
        }

        RexNode conditionExpr = RexUtil.composeConjunction(join.getCluster().getRexBuilder(), joinConditions, false);
        RelNode newRel = ((HashJoin) join).copy(join.getTraitSet(),
            conditionExpr, conditionExpr,
            join.getLeft(), join.getRight(),
            join.getJoinType(), join.isSemiJoinDone());
        call.getPlanner().onCopy(join, newRel);
        call.transformTo(newRel);
    }

}