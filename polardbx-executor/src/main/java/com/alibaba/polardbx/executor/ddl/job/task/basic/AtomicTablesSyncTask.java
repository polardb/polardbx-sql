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
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangeCrossDBPreemptiveSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Getter
@TaskName(name = "AtomicTablesSyncTask")
public class AtomicTablesSyncTask extends BaseSyncTask {

    private List<String> multiSchemas;
    private List<List<String>> logicalTables;
    final Long initWait;
    final Long interval;
    final TimeUnit timeUnit;

    public AtomicTablesSyncTask(List<String> multiSchemas,
                                List<List<String>> logicalTables,
                                Long initWait,
                                Long interval,
                                TimeUnit timeUnit) {
        super(multiSchemas.get(0));
        this.multiSchemas = multiSchemas;
        this.logicalTables = logicalTables;
        this.initWait = initWait;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(
                new TablesMetaChangeCrossDBPreemptiveSyncAction(schemaName, multiSchemas, logicalTables, initWait,
                    interval, timeUnit),
                SyncScope.ALL,
                true);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableNames:%s", multiSchemas,
                logicalTables.toString()));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(
                new TablesMetaChangeCrossDBPreemptiveSyncAction(schemaName, multiSchemas, logicalTables, initWait,
                    interval, timeUnit),
                SyncScope.ALL,
                true);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableNames:%s", multiSchemas,
                logicalTables.toString()));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < multiSchemas.size(); i++) {
            if (i >= 1) {
                sb.append(", ");
            }
            sb.append("schema: ").append(multiSchemas.get(i));
            sb.append(" on tableNames: [").append(String.join(",", logicalTables.get(i))).append("]");
        }
        return sb.toString();
    }
}
