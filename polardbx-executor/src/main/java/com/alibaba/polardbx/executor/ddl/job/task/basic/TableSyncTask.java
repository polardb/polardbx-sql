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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Getter
@TaskName(name = "TableSyncTask")
public class TableSyncTask extends BaseSyncTask {

    final String tableName;
    final boolean preemptive;
    final Long initWait;
    final Long interval;
    final TimeUnit timeUnit;

    public TableSyncTask(String schemaName,
                         String tableName,
                         boolean preemptive,
                         Long initWait,
                         Long interval,
                         TimeUnit timeUnit) {
        super(schemaName);
        this.tableName = tableName;
        this.preemptive = preemptive;
        this.initWait = initWait;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public TableSyncTask(String schemaName,
                         String tableName) {
        super(schemaName);
        this.tableName = tableName;
        this.preemptive = true;
        this.initWait = null;
        this.interval = null;
        this.timeUnit = null;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        boolean throwExceptions = !isFromCDC();
        try {
            boolean enablePreemptiveMdl =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
            Long initWait = Optional.ofNullable(this.initWait)
                .orElse(executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT));
            Long interval = Optional.ofNullable(this.interval)
                .orElse(executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL));
            TimeUnit timeUnit = Optional.ofNullable(this.timeUnit).orElse(TimeUnit.MILLISECONDS);
            if (!preemptive || !enablePreemptiveMdl) {
                SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, tableName), SyncScope.ALL,
                    throwExceptions);
            } else {
                SyncManagerHelper.sync(
                    new TableMetaChangePreemptiveSyncAction(schemaName, tableName, initWait, interval, timeUnit),
                    SyncScope.ALL,
                    throwExceptions);
            }
            FailPoint.injectSuspendFromHint("FP_TABLE_SYNC_TASK_SUSPEND", executionContext);
            // Sleep 1s when debug mode. Keep this status for a while.
            final String dbgInfo = executionContext.getParamManager().getString(ConnectionParams.GSI_DEBUG);
            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableName:%s", schemaName, tableName));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        return "|tableName: " + tableName;
    }
}
