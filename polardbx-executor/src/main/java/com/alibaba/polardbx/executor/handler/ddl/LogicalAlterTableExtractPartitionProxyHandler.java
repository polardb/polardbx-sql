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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExtractPartition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableExtractPartitionProxyHandler extends LogicalAlterTableExtractPartitionHandler {
    public LogicalAlterTableExtractPartitionProxyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableExtractPartition alterTableExtractPartition =
            (LogicalAlterTableExtractPartition) logicalDdlPlan;

        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        String objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager(logicalDdlPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        if (strategy != PartitionStrategy.LIST && strategy != PartitionStrategy.LIST_COLUMNS) {
            /**
             * 1. convert
             *      sql 'extract to partition [newPartitionName] by hot value(10)'
             *    to
             *      sql 'split into [newPartitionName] partitions 1 by hot value(10)'
             * 2. use new sql to build split partition subJob
             * */
            String splitSql =
                AlterTableGroupUtils.convertExtractPartitionToSplitPartitionSql(alterTableExtractPartition, true,
                    executionContext);
            return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), splitSql);
        } else {
            /**
             * 1. convert
             *      sql 'extract to p11 by hot value(10)'
             *    to
             *      sql 'split partition p1 into (partition p11 values in(10), partition p1 values in(xxx))'
             * 2. use new sql to build split partition subJob
             * */
            String splitSql = AlterTableGroupUtils.convertExtractListRelToSplitListSql(alterTableExtractPartition, true,
                executionContext);

            return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), splitSql);
        }
    }
}
