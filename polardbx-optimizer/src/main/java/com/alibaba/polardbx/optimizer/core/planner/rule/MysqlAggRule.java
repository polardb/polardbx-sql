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

import com.alibaba.polardbx.optimizer.core.rel.MysqlAgg;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

/**
 * @author dylan
 */
public class MysqlAggRule extends RelOptRule {

    public static final MysqlAggRule INSTANCE = new MysqlAggRule();

    protected MysqlAggRule() {
        super(operand(LogicalAggregate.class, RelOptRule.none()),
            "MysqlAggRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate logicalAggregate = call.rel(0);
        MysqlAgg mysqlAgg = MysqlAgg.create(
            logicalAggregate.getInput(), logicalAggregate.getGroupSet(), logicalAggregate.getGroupSets(),
            logicalAggregate.getAggCallList());
        call.transformTo(mysqlAgg);
    }
}


