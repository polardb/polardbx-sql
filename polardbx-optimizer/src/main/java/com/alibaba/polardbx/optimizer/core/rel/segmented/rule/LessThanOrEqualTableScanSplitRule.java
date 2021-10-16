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
 * add just like pk <= ? condition filter to table scan
 *
 * @author hongxi.chx
 * @create 2019-11-04 上午11:20
 */
public class LessThanOrEqualTableScanSplitRule extends TableScanSplitRule {

    public static LessThanOrEqualTableScanSplitRule INSTANCE = new LessThanOrEqualTableScanSplitRule(RelFactories.LOGICAL_BUILDER);

    /**
     *
     * Creates a TableScanSplitRule.
     */
    public LessThanOrEqualTableScanSplitRule(RelBuilderFactory relBuilderFactory) {
        super( relBuilderFactory,"LessThanOrEqualTableScanSplitRule");
        
    }
    
    @Override
    protected RelNode getSplitFilter(TableScan tableScan) {
        RexNode rexNode = getRexNode(tableScan, SqlStdOperatorTable.LESS_THAN_OR_EQUAL,0);
        RexNode condition = makeAnd(tableScan.getCluster().getRexBuilder(), rexNode);
        LogicalFilter filter =LogicalFilter.create(Gather.create(tableScan), condition);
        return filter;
    }

}
