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

import com.alibaba.polardbx.optimizer.core.rel.MysqlLimit;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSort;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTopN;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * @author dylan
 */
public class MysqlSortRule extends RelOptRule {

    public static final MysqlSortRule INSTANCE = new MysqlSortRule();

    protected MysqlSortRule() {
        super(operand(LogicalSort.class, RelOptRule.none()),
            "MysqlSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        if (logicalSort.withLimit() && !logicalSort.withOrderBy()) {
            MysqlLimit mysqlLimit = MysqlLimit.create(
                logicalSort.getInput(), logicalSort.getCollation(), logicalSort.offset, logicalSort.fetch);
            call.transformTo(mysqlLimit);
        } else if (!logicalSort.withLimit() && logicalSort.withOrderBy()) {
            MysqlSort mysqlSort = MysqlSort.create(
                logicalSort.getInput(), logicalSort.getCollation(), logicalSort.offset, logicalSort.fetch);
            call.transformTo(mysqlSort);
        } else if (logicalSort.withLimit() && logicalSort.withOrderBy()) {
            MysqlTopN mysqlTopN = MysqlTopN.create(
                logicalSort.getInput(), logicalSort.getCollation(), logicalSort.offset, logicalSort.fetch);
            call.transformTo(mysqlTopN);
        }
    }
}


