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
import com.alibaba.polardbx.executor.ddl.job.meta.misc.TruncateMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRUNCATE_CUTOVER_FAILED;

@Getter
@TaskName(name = "TruncateCutOverTask")
public class TruncateCutOverTask extends BaseGmsTask {

    final String logicalTableName;
    final Map<String, String> tmpIndexTableMap;
    final String tmpPrimaryTableName;

    public TruncateCutOverTask(final String schemaName,
                               final String logicalTableName,
                               final Map<String, String> tmpIndexTableMap,
                               final String tmpPrimaryTableName) {
        super(schemaName, logicalTableName);
        this.logicalTableName = logicalTableName;
        this.tmpIndexTableMap = tmpIndexTableMap;
        this.tmpPrimaryTableName = tmpPrimaryTableName;
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);

        LOGGER.info(
            String.format("start write meta during truncate cutover for table: %s.%s", schemaName, logicalTableName)
        );

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        // BaseGmsTask will only update primary table version
        try {
            TruncateMetaChanger.cutOver(
                metaDbConnection,
                schemaName,
                logicalTableName,
                tmpPrimaryTableName,
                isNewPartDb
            );

            TableInfoManager.updateTableVersion(schemaName, tmpPrimaryTableName, metaDbConnection);

            for (Map.Entry<String, String> entry : tmpIndexTableMap.entrySet()) {
                TruncateMetaChanger.cutOver(
                    metaDbConnection,
                    schemaName,
                    entry.getKey(),
                    entry.getValue(),
                    isNewPartDb
                );
                TableInfoManager.updateTableVersion(schemaName, entry.getKey(), metaDbConnection);
                TableInfoManager.updateTableVersion(schemaName, entry.getValue(), metaDbConnection);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        LOGGER.info(
            String
                .format("finish write meta during truncate cutover for primary table: %s.%s", schemaName, logicalTableName)
        );

        FailPoint.injectException(FP_TRUNCATE_CUTOVER_FAILED);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
        try {
            TableInfoManager.updateTableVersion4Repartition(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
    }

}