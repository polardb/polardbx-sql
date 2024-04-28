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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.GlobalAcquireMdlLockInDbSyncAction;
import com.alibaba.polardbx.executor.sync.GlobalReleaseMdlLockInDbSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "GlobalAcquireMdlLockInDbSyncTask")
public class GlobalAcquireMdlLockInDbSyncTask extends BaseSyncTask {
    final Set<String> schemaNames;

    public GlobalAcquireMdlLockInDbSyncTask(String srcSchemaName, Set<String> schemaNames) {
        super(srcSchemaName);
        this.schemaNames = schemaNames;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new GlobalAcquireMdlLockInDbSyncAction(schemaNames), SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while lock tables meta, schemaNames:%s", schemaNames));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new GlobalReleaseMdlLockInDbSyncAction(schemaNames), SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while unlock tables meta, schemaName:%s", schemaNames));
            throw GeneralUtil.nestedException(t);
        }
    }
}
