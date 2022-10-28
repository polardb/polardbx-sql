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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupAddPartitionProxyHandler extends LogicalAlterTableGroupAddPartitionHandler {
    public LogicalAlterTableGroupAddPartitionProxyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupAddPartition alterTableGroupAddPartition =
            (LogicalAlterTableGroupAddPartition) logicalDdlPlan;

        SqlAlterTableGroup sqlNode = (SqlAlterTableGroup) (logicalDdlPlan.relDdl.getSqlNode());
        PartitionInfo partitionInfo =
            AlterTableGroupUtils.getPartitionInfo(PartitionNameUtil.toLowerCase(sqlNode.getTableGroupName().toString()),
                logicalDdlPlan.getSchemaName());

        if (!isListStrategyAndContainDefaultPartition(partitionInfo)) {
            return super.buildDdlJob(logicalDdlPlan, executionContext);
        } else {
            /**
             * 1. convert
             *      sql 'add partition (partition p2 values in(3,4), partition p3 values in(5,6))'
             *    to
             *      sql 'split partition default_part into (partition p2 values in(3,4), partition p3 values in(5,6), partition default_part values in(default))'
             * 2. use new sql to build split partition subJob
             *  */

            String splitSql = AlterTableGroupUtils.convertAddListRelToSplitListSql(alterTableGroupAddPartition, false,
                executionContext);
            return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), splitSql);
        }
    }

    protected boolean isListStrategyAndContainDefaultPartition(PartitionInfo partitionInfo) {
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        boolean isList = (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS);
        boolean hasDefaultPartition = false;
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
            if (spec.getIsDefaultPartition()) {
                hasDefaultPartition = true;
                break;
            }
        }
        return isList && hasDefaultPartition;
    }

}
