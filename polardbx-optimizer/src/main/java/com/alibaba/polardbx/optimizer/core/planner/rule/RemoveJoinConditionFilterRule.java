/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class RemoveJoinConditionFilterRule extends RelOptRule {

    public static final RemoveJoinConditionFilterRule INSTANCE =
        new RemoveJoinConditionFilterRule(Join.class);

    //~ Constructors -----------------------------------------------------------

    public RemoveJoinConditionFilterRule(
        Class<? extends Join> joinClass) {
        super(operand(joinClass, any()));
    }

    //~ Methods ----------------------------------------------------------------

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_REMOVE_JOIN_CONDITION);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

        Join join = call.rel(0);
        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
        case RIGHT:
            break;
        default:
            return;
        }
        final RelMetadataQuery mq = call.getMetadataQuery();
        RelOptPredicateList leftPreds = mq.getPulledUpPredicates(join.getLeft());
        RelOptPredicateList rightPreds = mq.getPulledUpPredicates(join.getRight());
        if (leftPreds.pulledUpPredicates.isEmpty() || rightPreds.pulledUpPredicates.isEmpty()) {
            return;
        }

        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        final int rightFieldCount = join.getRight().getRowType().getFieldCount();

        //left condition mapping

        Mappings.TargetMapping lmapping =
            Mappings.createShiftMapping(leftFieldCount, 0, 0, leftFieldCount);

        final RexPermuteInputsShuttle lpermute = RexPermuteInputsShuttle.of(lmapping);
        List<RexNode> lJoinCondition = leftPreds.pulledUpPredicates.stream().map(p ->
            p.accept(lpermute)).collect(Collectors.toList());

        //right condition mapping

        Mappings.TargetMapping rmapping =
            Mappings.createShiftMapping(rightFieldCount, leftFieldCount, 0, rightFieldCount);

        final RexPermuteInputsShuttle rpermute = RexPermuteInputsShuttle.of(rmapping);
        List<RexNode> rJoinCondition = rightPreds.pulledUpPredicates.stream().map(p ->
            p.accept(rpermute)).collect(Collectors.toList());

        HashMap<String, List<RexNode>> nodeMap = new HashMap();
        findRexLiterNode(lJoinCondition, nodeMap);
        findRexLiterNode(rJoinCondition, nodeMap);

        List<RexNode> joinConditions = new ArrayList<>();
        List<RexNode> conjunctions = RelOptUtil.conjunctions(join.getCondition());

        for (RexNode rexNode : conjunctions) {
            boolean bret = false;
            if (rexNode.isA(SqlKind.EQUALS) && rexNode instanceof RexCall && ((RexCall) rexNode).operands.size() == 2) {
                RexNode firstNode = ((RexCall) rexNode).getOperands().get(0);
                RexNode secondNode = ((RexCall) rexNode).getOperands().get(1);
                bret = nodeMap.values().stream().anyMatch(t -> t.contains(firstNode) && t.contains(secondNode));
            }
            if (!bret) {
                joinConditions.add(rexNode);
            }
        }

        if (joinConditions.size() >= 1 && joinConditions.size() != conjunctions.size()) {
            RexNode conditionExpr =
                RexUtil.composeConjunction(join.getCluster().getRexBuilder(), joinConditions, false);
            RelNode newRel = join.copy(join.getTraitSet(), conditionExpr,
                join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone());
            call.getPlanner().onCopy(join, newRel);
            call.transformTo(newRel);
        }
    }

    /**
     * The Result Type parameters:
     * <K> – the key of the map must RexLiteral or RexDynamicParam
     * <V> – the value of the map the other operands.
     */
    private void findRexLiterNode(List<RexNode> inputs, HashMap<String, List<RexNode>> nodeMap) {
        for (RexNode rexNode : inputs) {
            if (rexNode.isA(SqlKind.EQUALS) && rexNode instanceof RexCall
                && ((RexCall) rexNode).operands.size() == 2) {
                if (((RexCall) rexNode).getOperands().get(0) instanceof RexDynamicParam ||
                    ((RexCall) rexNode).getOperands().get(0) instanceof RexLiteral) {
                    String flag = ((RexCall) rexNode).getOperands().get(0).toString();
                    if (!nodeMap.containsKey(flag)) {
                        nodeMap.put(flag, new ArrayList<>());
                    }
                    nodeMap.get(flag).add(((RexCall) rexNode).getOperands().get(1));
                } else if (((RexCall) rexNode).getOperands().get(1) instanceof RexDynamicParam ||
                    ((RexCall) rexNode).getOperands().get(1) instanceof RexLiteral) {
                    String flag = ((RexCall) rexNode).getOperands().get(1).toString();
                    if (!nodeMap.containsKey(flag)) {
                        nodeMap.put(flag, new ArrayList<>());
                    }
                    nodeMap.get(flag).add(((RexCall) rexNode).getOperands().get(0));
                }
            }
        }
    }
}

// End FilterProjectTransposeRule.java
