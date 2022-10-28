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
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.sync.TablesMetaChangeSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Joiner;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Getter
@TaskName(name = "TablesSyncTask")
public class TablesSyncTask extends BaseSyncTask {

    final List<String> tableNames;
    final boolean preemptive;
    final Long initWait;
    final Long interval;
    final TimeUnit timeUnit;

    public TablesSyncTask(String schemaName,
                          List<String> tableNames) {
        super(schemaName);
        this.tableNames = tableNames;
        this.preemptive = false;
        this.initWait = null;
        this.interval = null;
        this.timeUnit = null;
    }

    public TablesSyncTask(String schemaName,
                          List<String> tableNames,
                          boolean preemptive,
                          Long initWait,
                          Long interval,
                          TimeUnit timeUnit) {
        super(schemaName);
        this.tableNames = tableNames;
        this.preemptive = preemptive;
        this.initWait = initWait;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        try {
            if (!preemptive) {
                SyncManagerHelper.sync(new TablesMetaChangeSyncAction(schemaName, tableNames), true);
            } else {
                SyncManagerHelper.sync(
                    new TablesMetaChangePreemptiveSyncAction(schemaName, tableNames, initWait, interval, timeUnit),
                    true);
            }
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableNames:%s", schemaName, tableNames.toString()));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        return "|tableNames: " + Joiner.on(", ").join(tableNames);
    }
}
