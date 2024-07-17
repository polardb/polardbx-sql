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

package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;

public abstract class LogicalSortToSortRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected final boolean isTopNRule;

    protected LogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(operand(LogicalSort.class, Convention.NONE, any()), "DrdsSortConvertRule:" + desc);
        this.isTopNRule = isTopNRule;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    public void onMatch(RelOptRuleCall call) {
        final LogicalSort sort = call.rel(0);
        createSort(call, sort);
    }

    protected abstract void createSort(RelOptRuleCall call, LogicalSort sort);
}

