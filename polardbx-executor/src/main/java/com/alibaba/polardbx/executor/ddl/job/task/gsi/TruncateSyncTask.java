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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TruncateSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Set;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRUNCATE_SYNC_FAILED;

@TaskName(name = "TruncateSyncTask")
@Getter
public class TruncateSyncTask extends BaseSyncTask {

    final String primaryTableName;
    final String tmpPrimaryTableName;

    final Set<String> indexTableNames;

    @JSONCreator
    public TruncateSyncTask(String schemaName, String primaryTableName, String tmpPrimaryTableName,
                            Set<String> indexTableNames) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.tmpPrimaryTableName = tmpPrimaryTableName;
        this.indexTableNames = indexTableNames;
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
                new TruncateSyncAction(schemaName,
                    primaryTableName,
                    tmpPrimaryTableName,
                    indexTableNames,
                    executionContext.isTestMode(),
                    (Long) executionContext.getConnId(),
                    executionContext.getTraceId()
                ),
                schemaName
            );
            FailPoint.injectException(FP_TRUNCATE_SYNC_FAILED);

            LOGGER.info(
                String.format("finish sync meta during cutover for primary table: %s.%s", schemaName, primaryTableName)
            );

        } catch (Exception e) {
            String errMsg = String.format(
                "error occurs while sync gsi meta during cutover, tableName:%s, tmpTableName:%s",
                primaryTableName, tmpPrimaryTableName
            );
            LOGGER.error(errMsg);
            throw GeneralUtil.nestedException(e);
        }
    }

}