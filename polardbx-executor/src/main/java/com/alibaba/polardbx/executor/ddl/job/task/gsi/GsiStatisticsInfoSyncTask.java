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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@TaskName(name = "GsiStatisticsInfoSyncTask")
@Getter
public class GsiStatisticsInfoSyncTask extends BaseDdlTask {
    final private String schemaName;
    final private String tableName;
    final private String gsiName;

    final private String newValue;

    final private int alterKind;

    @JSONCreator
    public GsiStatisticsInfoSyncTask(String schemaName, String tableName, String gsiName, int alterKind,
                                     String newValue) {
        super(schemaName);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.gsiName = gsiName;
        this.alterKind = alterKind;
        this.newValue = newValue;
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new GsiStatisticsSyncAction(schemaName, gsiName, newValue, alterKind));
        } catch (Throwable ignore) {
            LOGGER.error(
                "error occurs while execute GsiStatisticsSyncAction"
            );
        }
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        try {
            if (alterKind == GsiStatisticsSyncAction.RENAME_RECORD) {
                SyncManagerHelper.sync(new GsiStatisticsSyncAction(schemaName, newValue, gsiName, alterKind));
            } else {
                SyncManagerHelper.sync(new GsiStatisticsSyncAction(schemaName, gsiName, newValue, alterKind));
            }
        } catch (Throwable ignore) {
            LOGGER.error(
                "error occurs while execute GsiStatisticsSyncAction"
            );
        }
    }
}
