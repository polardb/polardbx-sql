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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

public class COLLogicalSortToSortRule extends LogicalSortToSortRule {
    public static final COLLogicalSortToSortRule INSTANCE = new COLLogicalSortToSortRule(false, "INSTANCE");

    public static final COLLogicalSortToSortRule TOPN = new COLLogicalSortToSortRule(true, "TOPN");

    private COLLogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(isTopNRule, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (isTopNRule) {
            return PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_TOPN);
        }
        return true;
    }

    @Override
    protected void createSort(RelOptRuleCall call, LogicalSort sort) {
        if (isTopNRule) {
            convertTopN(call, sort);
        } else {
            convertLimit(call, sort);
        }
    }

    private void convertTopN(RelOptRuleCall call, LogicalSort sort) {
        // convert sort to topN
        if (sort.withLimit() && sort.withOrderBy()) {
            RelNode input = convert(sort.getInput(), call.getPlanner().emptyTraitSet().replace(outConvention));

            TopN partialTopN = TopN.create(
                input.getTraitSet().replace(sort.getCollation()),
                input, sort.getCollation(), null, RuleUtils.getPartialFetch(sort));

            RelNode ensureNode = convert(partialTopN, partialTopN.getTraitSet().replace(RelDistributions.SINGLETON));

            call.transformTo(
                TopN.create(ensureNode.getTraitSet(), ensureNode, sort.getCollation(), sort.offset, sort.fetch));
        }
    }

    private void convertLimit(RelOptRuleCall call, LogicalSort sort) {
        RelNode input =
            convert(sort.getInput(), call.getPlanner().emptyTraitSet().replace(outConvention));
        final boolean hasOrdering = sort.withOrderBy();
        final boolean hasLimit = sort.withLimit();

        if (hasOrdering && !hasLimit) {
            call.transformTo(convert(input,
                input.getTraitSet()
                    .replace(RelDistributions.SINGLETON)
                    .replace(sort.getCollation())));
            return;
        }
        ParamManager pm = PlannerContext.getPlannerContext(call).getParamManager();
        if (!pm.getBoolean(ConnectionParams.ENABLE_LIMIT)) {
            return;
        }

        if (hasOrdering) {
            Limit limit = Limit.create(
                sort.getTraitSet().replace(outConvention).replace(RelDistributions.SINGLETON),
                convert(input,
                    input.getTraitSet()
                        .replace(RelDistributions.SINGLETON)
                        .replace(sort.getCollation())), sort.offset, sort.fetch);
            call.transformTo(limit);
            return;
        }

        // now sort has no order
        // one phase limit
        Limit limit = Limit.create(
            sort.getTraitSet().replace(outConvention).replace(RelDistributions.SINGLETON),
            convert(input,
                input.getTraitSet()
                    .replace(outConvention)
                    .replace(RelDistributions.SINGLETON)), sort.offset, sort.fetch);
        call.transformTo(limit);

        // tow phase limit
        if (!pm.getBoolean(ConnectionParams.ENABLE_PARTIAL_LIMIT)) {
            return;
        }
        Limit localLimit = Limit.create(
            input.getTraitSet(),
            input,
            null,
            RuleUtils.getPartialFetch(sort));
        Limit globalLimit = Limit.create(
            sort.getTraitSet().replace(outConvention).replace(RelDistributions.SINGLETON),
            convert(localLimit, localLimit.getTraitSet().replace(RelDistributions.SINGLETON)),
            sort.offset,
            sort.fetch);
        call.transformTo(globalLimit);
    }
}

