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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class LimitUnionTransposeRule extends RelOptRule {

    public static final LimitUnionTransposeRule INSTANCE = new LimitUnionTransposeRule();

    private LimitUnionTransposeRule() {
        this(RelFactories.LOGICAL_BUILDER,
            "LimitUnionTransposeRule");
    }

    public LimitUnionTransposeRule(
        RelBuilderFactory relBuilderFactory,
        String description) {
        super(
            operand(LogicalSort.class,
                operand(LogicalUnion.class, any())),
            relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalSort sort = call.rel(0);
        final LogicalUnion union = call.rel(1);
        return union.all && sort.withLimit() && !sort.isIgnore();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSort limit = call.rel(0);
        final LogicalUnion union = call.rel(1);

        List<RelNode> newInputs = new ArrayList<>();
        RexNode fetch = CBOUtil.calPushDownFetch(limit);
        for (RelNode relNode : union.getInputs()) {
            newInputs.add(LogicalSort.create(relNode, limit.getCollation(), null, fetch));
        }

        // create new union and sort
        LogicalUnion newUnion = union
            .copy(union.getTraitSet(), newInputs, union.all);
        LogicalSort result = limit.copy(limit.getTraitSet(), newUnion, limit.getCollation(),
            limit.offset, limit.fetch);
        result.setIgnore(true);
        call.transformTo(result);
    }
}