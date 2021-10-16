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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterTableGroupCleanupTask")
public class AlterTableGroupCleanupTask extends BaseDdlTask {

    @JSONCreator
    public AlterTableGroupCleanupTask(String schemaName) {
        super(schemaName);
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConnection);
        complexTaskOutlineAccessor.deleteComplexTaskByJobId(getJobId());
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        updateSupportedCommands(true, false, metaDbConnection);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }
}
