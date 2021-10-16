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
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalSemiJoin;

public class CBOPushSemiJoinRule extends PushSemiJoinRule {

    public CBOPushSemiJoinRule(RelOptRuleOperand operand, String description) {
        super(operand, "CBOPushSemiJoinRule:" + description);
    }

    public static final CBOPushSemiJoinRule INSTANCE = new CBOPushSemiJoinRule(
        operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            some(
                operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none()),
                operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none())
            )), "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CBO_PUSH_JOIN);
    }
}

