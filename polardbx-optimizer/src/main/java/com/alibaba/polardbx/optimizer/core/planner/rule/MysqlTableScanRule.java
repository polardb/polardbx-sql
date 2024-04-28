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

import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * @author dylan
 */
public class MysqlTableScanRule extends RelOptRule {

    public static final MysqlTableScanRule FILTER_TABLESCAN = new MysqlTableScanRule(
        operand(LogicalFilter.class, operand(LogicalTableScan.class, RelOptRule.none())),
        "MysqlTableScanRule:FILTER_TABLESCAN", true);

    public static final MysqlTableScanRule TABLESCAN = new MysqlTableScanRule(
        operand(LogicalTableScan.class, RelOptRule.none()),
        "MysqlTableScanRule:TABLESCAN", false);

    private boolean withFilter;

    protected MysqlTableScanRule(RelOptRuleOperand operand, String description, boolean withFilter) {
        super(operand, description);
        this.withFilter = withFilter;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MysqlTableScan mysqlTableScan;
        if (withFilter) {
            LogicalFilter logicalFilter = call.rel(0);
            LogicalTableScan logicalTableScan = call.rel(1);
            mysqlTableScan = MysqlTableScan
                .create(logicalTableScan.getCluster(), logicalTableScan.getTable(), logicalFilter.getChildExps(),
                    logicalTableScan.getHints(), logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                    logicalTableScan.getPartitions());
        } else {
            LogicalTableScan logicalTableScan = call.rel(0);
            mysqlTableScan = MysqlTableScan
                .create(logicalTableScan.getCluster(), logicalTableScan.getTable(), ImmutableList.of(),
                    logicalTableScan.getHints(), logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                    logicalTableScan.getPartitions());
        }
        call.transformTo(mysqlTableScan);
    }
}

