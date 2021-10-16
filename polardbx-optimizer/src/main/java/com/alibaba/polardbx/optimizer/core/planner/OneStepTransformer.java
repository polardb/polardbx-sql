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

package com.alibaba.polardbx.optimizer.core.planner;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneStepTransformer {
    private static class FakeRuleCall extends RelOptRuleCall {
        //~ Instance fields --------------------------------------------------------

        private List<RelNode> results;

        //~ Constructors -----------------------------------------------------------

        FakeRuleCall(
            RelOptPlanner planner,
            RelOptRuleOperand operand,
            RelNode[] rels,
            Map<RelNode, List<RelNode>> nodeChildren,
            List<RelNode> parents) {
            super(planner, operand, rels, nodeChildren, parents);

            results = new ArrayList<RelNode>();
        }

        //~ Methods ----------------------------------------------------------------

        // implement RelOptRuleCall
        public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
            final RelNode rel0 = rels[0];
            RelOptUtil.verifyTypeEquivalence(rel0, rel, rel0);
            results.add(rel);
        }

        List<RelNode> getResults() {
            return results;
        }

    }

    public static RelNode transform(RelNode input, RelOptRule rule) {

        final List<RelNode> bindings = new ArrayList<>();
        final Map<RelNode, List<RelNode>> nodeChildren = new HashMap<>();
        boolean match = matchOperands(
            rule.getOperand(),
            input,
            bindings,
            nodeChildren);

        // match nothing
        if (!match) {
            return input;
        }

        FakeRuleCall fakeCall =
            new FakeRuleCall(
                input.getCluster().getPlanner(),
                rule.getOperand(),
                bindings.toArray(new RelNode[bindings.size()]),
                nodeChildren,
                new ArrayList<>()/* empty parents*/);

        if (rule.matches(fakeCall)) {
            rule.onMatch(fakeCall);
            List<RelNode> results = fakeCall.getResults();
            if (!results.isEmpty()) {
                return results.get(0);
            }
        }
        // match nothing
        return input;
    }

    private static boolean matchOperands(
        RelOptRuleOperand operand,
        RelNode rel,
        List<RelNode> bindings,
        Map<RelNode, List<RelNode>> nodeChildren) {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        @SuppressWarnings("unchecked")
        List<RelNode> childRels = (List) rel.getInputs();
        switch (operand.childPolicy) {
        case ANY:
            return true;
        case UNORDERED:
            // For each operand, at least one child must match. If
            // matchAnyChildren, usually there's just one operand.
            for (RelOptRuleOperand childOperand : operand.getChildOperands()) {
                boolean match = false;
                for (RelNode childRel : childRels) {
                    match =
                        matchOperands(
                            childOperand,
                            childRel,
                            bindings,
                            nodeChildren);
                    if (match) {
                        break;
                    }
                }
                if (!match) {
                    return false;
                }
            }
            final List<RelNode> children = new ArrayList<>(childRels.size());
            for (RelNode childRel : childRels) {
                children.add(childRel);
            }
            nodeChildren.put(rel, children);
            return true;
        default:
            int n = operand.getChildOperands().size();
            if (childRels.size() < n) {
                return false;
            }
            for (Pair<RelNode, RelOptRuleOperand> pair
                : Pair.zip(childRels, operand.getChildOperands())) {
                boolean match =
                    matchOperands(
                        pair.right,
                        pair.left,
                        bindings,
                        nodeChildren);
                if (!match) {
                    return false;
                }
            }
            return true;
        }
    }
}
