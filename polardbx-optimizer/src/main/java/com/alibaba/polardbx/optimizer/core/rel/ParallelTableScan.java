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

package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class ParallelTableScan extends TableScan {

    private boolean paralleled;

    public ParallelTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlNodeList hints,
                             SqlNode indexNode, RexNode flashback, SqlOperator flashbackOperator, SqlNode partitions) {
        super(cluster, traitSet, table, hints, indexNode, flashback, flashbackOperator, partitions);
    }

    public boolean isParalleled() {
        return paralleled;
    }

    public void setParalleled(boolean paralleled) {
        this.paralleled = paralleled;
    }

    public static ParallelTableScan copy(TableScan tableScan, boolean paralleled) {
        final ParallelTableScan parallelTableScan =
            new ParallelTableScan(tableScan.getCluster(), tableScan.getTraitSet(), tableScan.getTable(),
                tableScan.getHints(), tableScan.getIndexNode(), tableScan.getFlashback(),
                tableScan.getFlashbackOperator(), tableScan.getPartitions());
        parallelTableScan.setParalleled(paralleled);
        return parallelTableScan;
    }

}
