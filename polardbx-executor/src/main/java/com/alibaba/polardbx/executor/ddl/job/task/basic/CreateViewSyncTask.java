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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.CreateViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "CreateViewSyncTask")
@Getter
public class CreateViewSyncTask extends BaseDdlTask {
    final private String schemaName;
    final private String viewName;

    @JSONCreator
    public CreateViewSyncTask(String schemaName, String viewName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new CreateViewSyncAction(schemaName, viewName), schemaName, SyncScope.ALL);
        } catch (Throwable ignore) {
            LOGGER.error(
                "error occurs while execute GsiStatisticsSyncAction"
            );
        }
    }
}
