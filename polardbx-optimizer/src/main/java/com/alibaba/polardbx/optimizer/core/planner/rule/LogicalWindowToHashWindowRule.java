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
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;

public abstract class LogicalWindowToHashWindowRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalWindowToHashWindowRule(String desc) {
        super(operand(LogicalWindow.class, some(operand(RelSubset.class, any()))),
            "LogicalWindowToHashWindowRule:" + desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        if (!PlannerContext.getPlannerContext(rel).getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_WINDOW)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = (LogicalWindow) call.rels[0];

        // window should be split before
        if (window.groups.size() != 1) {
            return;
        }

        if (containsOrderKey(window) || !allUnboundedFrame(window) || containBannedWindowFunc(window)) {
            return;
        }

        RelNode input = call.rels[1];

        RelNode newInput =
            convert(input, input.getTraitSet().replace(outConvention).replace(RelCollations.EMPTY));

        createHashWindow(call, window, newInput);
    }

    protected abstract void createHashWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput);

    private boolean containsOrderKey(LogicalWindow window) {
        return !window.groups.get(0).orderKeys.getFieldCollations().isEmpty();
    }

    private boolean allUnboundedFrame(LogicalWindow window) {
        Window.Group group = window.groups.get(0);
        return group.lowerBound == RexWindowBound.UNBOUNDED_PRECEDING &&
            group.upperBound == RexWindowBound.UNBOUNDED_FOLLOWING;
    }

    private boolean containBannedWindowFunc(LogicalWindow window) {
        return window.groups.get(0).aggCalls.stream()
            .anyMatch(agg -> agg.isA(SqlKind.WINDOW_AGG) && !allowedWindowFuncInHashWindow(agg));
    }

    /**
     * most pure window function has no meaning in hash window
     */
    private boolean allowedWindowFuncInHashWindow(Window.RexWinAggCall aggCall) {
        return aggCall.isA(SqlKind.FIRST_VALUE) || aggCall.isA(SqlKind.LAST_VALUE);
    }
}
