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

package com.alibaba.polardbx.optimizer.core.rel.segmented.rule;

import com.alibaba.polardbx.optimizer.core.rel.Gather;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * add just like pk > ? and pk <= ? condition filter to table scan
 *
 * @author hongxi.chx
 * @create 2019-11-04 上午11:21
 */
public class BetweenTableScanSplitRule extends TableScanSplitRule {
    public static BetweenTableScanSplitRule INSTANCE = new BetweenTableScanSplitRule(RelFactories.LOGICAL_BUILDER);

    /**
     *
     * Creates a TableScanSplitRule.
     */
    public BetweenTableScanSplitRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory,"BetweenTableScanSplitRule");

    }

    @Override
    protected RelNode getSplitFilter(TableScan tableScan) {
        RexNode rexNode1 = getRexNode(tableScan, SqlStdOperatorTable.GREATER_THAN,0);
        RexNode rexNode2 = getRexNode(tableScan, SqlStdOperatorTable.LESS_THAN_OR_EQUAL,1);
        RexNode condition = makeAnd(tableScan.getCluster().getRexBuilder(), rexNode1,rexNode2);
        LogicalFilter filter =LogicalFilter.create(Gather.create(tableScan), condition);
        return filter;
    }
}
