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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupBackfill;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@TaskName(name = "AlterTableGroupBackFillTask")
@Getter
public class AlterTableGroupBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    String logicalTableName;
    Map<String, Set<String>> sourcePhyTables;
    Map<String, Set<String>> targetPhyTables;
    boolean broadcast;
    boolean movePartitions;

    @JSONCreator
    public AlterTableGroupBackFillTask(String schemaName,
                                       String logicalTableName,
                                       Map<String, Set<String>> sourcePhyTables,
                                       Map<String, Set<String>> targetPhyTables,
                                       boolean broadcast,
                                       boolean movePartitions) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.broadcast = broadcast;
        this.movePartitions = movePartitions;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setSchemaName(schemaName);
        AlterTableGroupBackfill backFillPlan =
            AlterTableGroupBackfill
                .createAlterTableGroupBackfill(schemaName, logicalTableName, executionContext, sourcePhyTables,
                    targetPhyTables, broadcast, movePartitions);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(backFillPlan, executionContext);
    }

    public static String getTaskName() {
        return "AlterTableGroupBackFillTask";
    }
}
