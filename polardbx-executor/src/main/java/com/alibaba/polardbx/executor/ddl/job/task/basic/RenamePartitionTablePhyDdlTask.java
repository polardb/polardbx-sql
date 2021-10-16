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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

@TaskName(name = "RenamePartitionTablePhyDdlTask")
public class RenamePartitionTablePhyDdlTask extends BasePhyDdlTask {

    @JSONCreator
    public RenamePartitionTablePhyDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        checkTableNamePatternForRename(schemaName, physicalPlanData.getLogicalTableName(), executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        super.executeImpl(executionContext);
    }

    private void checkTableNamePatternForRename(String schemaName, String logicalTableName,
                                                ExecutionContext executionContext) {

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        boolean isPartitioned = !(partitionInfo.isBroadcastTable() || partitionInfo.isSingleTable());
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        if (isPartitioned) {
            executionContext.setPhyTableRenamed(false);
        }
    }

}
