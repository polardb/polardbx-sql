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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder.PhyTableShardResult;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Writer for INSERT or REPLACE on single table
 *
 * @author luoyanxin
 */
public class ReplicateSingleInsertGsiWriter extends ReplicateSingleInsertWriter {

    public ReplicateSingleInsertGsiWriter(RelOptTable targetTable,
                                          LogicalInsert insert,
                                          TableMeta tableMeta,
                                          TableRule tableRule) {
        super(targetTable, insert, tableMeta, tableRule);
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        if (ComplexTaskPlanUtils.canWrite(this.tableMeta)) {
            return super.getInput(executionContext);
        }

        return new ArrayList<>();
    }
}
