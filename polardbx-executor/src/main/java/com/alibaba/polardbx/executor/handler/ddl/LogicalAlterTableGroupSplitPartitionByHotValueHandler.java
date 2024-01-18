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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupSplitPartitionByHotValueJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupSplitPartitionByHotValueHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupSplitPartitionByHotValueHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (LogicalAlterTableSplitPartitionByHotValue) logicalDdlPlan;
        alterTableGroupSplitPartitionByHotValue.preparedData(executionContext);
        AlterTableGroupSplitPartitionByHotValuePreparedData preparedData =
            alterTableGroupSplitPartitionByHotValue.getPreparedData();

        ExecutableDdlJob executableDdlJob = AlterTableGroupSplitPartitionByHotValueJobFactory
            .create(alterTableGroupSplitPartitionByHotValue.relDdl,
                preparedData,
                executionContext);
        if (executableDdlJob instanceof TransientDdlJob) {
            if (preparedData.hotPartitionNameNeedChange()) {
                AlterTableGroupRenamePartitionPreparedData renamePartitionPreparedData =
                    new AlterTableGroupRenamePartitionPreparedData();
                renamePartitionPreparedData.setSchemaName(preparedData.getSchemaName());
                renamePartitionPreparedData.setTableGroupName(preparedData.getTableGroupName());
                renamePartitionPreparedData.setChangePartitionsPair(preparedData.getChangeHotPartitionNames());
                executableDdlJob = AlterTableGroupRenamePartitionJobFactory
                    .create(alterTableGroupSplitPartitionByHotValue.relDdl, renamePartitionPreparedData,
                        executionContext);
            }
        }
        return executableDdlJob;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) (logicalDdlPlan.relDdl
                .getSqlNode()),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return false;
    }

}
