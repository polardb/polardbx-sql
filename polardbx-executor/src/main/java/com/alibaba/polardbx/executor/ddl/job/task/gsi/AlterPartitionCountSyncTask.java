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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.sync.AlterPartitionCountSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Map;

@Getter
@TaskName(name = "AlterPartitionCountSyncTask")
public class AlterPartitionCountSyncTask extends BaseSyncTask {

    private final String primaryTableName;
    private final Map<String, String> tableNameMap;

    public AlterPartitionCountSyncTask(final String schemaName,
                                       final String primaryTableName,
                                       Map<String, String> tableNameMap) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.tableNameMap = tableNameMap;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        GsiValidator.validateEnableMDL(executionContext);

        try {
            LOGGER.info(
                String.format("start sync meta during cutover for primary table: %s.%s", schemaName, primaryTableName)
            );
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            // Sync will reload and clear cross status transaction.
            SyncManagerHelper.sync(
                new AlterPartitionCountSyncAction(schemaName,
                    primaryTableName,
                    executionContext.getConnId(),
                    executionContext.getTraceId()
                ),
                schemaName,
                SyncScope.ALL
            );

            LOGGER.info(
                String.format("finish sync meta during cutover for primary table: %s.%s", schemaName, primaryTableName)
            );

        } catch (Exception e) {
            String errMsg = String.format(
                "error occurs while sync gsi meta during cutover, tableName:%s, gsiNames:%s",
                primaryTableName, tableNameMap.keySet()
            );
            LOGGER.error(errMsg);
            throw GeneralUtil.nestedException(e);
        }
    }
}
