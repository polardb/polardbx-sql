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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Optional;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAILED_TABLE_SYNC;

@Getter
@TaskName(name = "TableSyncTask")
public class TableSyncTask extends BaseSyncTask {

    final String tableName;
    final boolean preemptive;
    final PreemptiveTime preemptiveTime;

    public TableSyncTask(String schemaName,
                         String tableName,
                         boolean preemptive,
                         PreemptiveTime preemptiveTime
                         ) {
        super(schemaName);
        this.tableName = tableName;
        this.preemptive = preemptive;
        this.preemptiveTime = preemptiveTime;
    }

    public TableSyncTask(String schemaName,
                         String tableName) {
        super(schemaName);
        this.tableName = tableName;
        this.preemptive = true;
        this.preemptiveTime = null;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        if (FailPoint.isKeyEnable(FP_FAILED_TABLE_SYNC)
            && executionContext.getParamManager().getBoolean(ConnectionParams.FP_FAILED_TABLE_SYNC)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL, "Fail point FP_FAILED_TABLE_SYNC");
        }
        boolean throwExceptions = !isFromCDC();
        try {
            boolean enablePreemptiveMdl =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
            PreemptiveTime preemptiveTime = Optional.ofNullable(this.preemptiveTime).orElse(PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
                ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL));
            if (!preemptive || !enablePreemptiveMdl) {
                SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, tableName), SyncScope.ALL, throwExceptions);
            } else {
                SyncManagerHelper.sync(
                    new TableMetaChangePreemptiveSyncAction(schemaName, tableName, preemptiveTime, false),
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
