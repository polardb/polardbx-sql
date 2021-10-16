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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableLookup;

/**
 * @author chenmo.cm
 */
public class TableLookupExpandRule extends RelOptRule {

    public static final TableLookupExpandRule INSTANCE = new TableLookupExpandRule();

    public TableLookupExpandRule() {
        super(operand(LogicalTableLookup.class, any()), "TableLookupExpandRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableLookup tableLookup = call.rel(0);
        final ProjectJoinTranspose projectJoinTranspose = new ProjectJoinTranspose(call,
            tableLookup.getProject(),
            tableLookup.getJoin()).invoke();

        if (projectJoinTranspose.isSkipped()) {
            call.transformTo(tableLookup.getProject());
        } else {
            call.transformTo(projectJoinTranspose.getNewTopProject());
        }
    }
}
