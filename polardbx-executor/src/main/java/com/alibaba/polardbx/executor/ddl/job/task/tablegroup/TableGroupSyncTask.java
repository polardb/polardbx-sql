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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "TableGroupSyncTask")
public class TableGroupSyncTask extends BaseSyncTask {

    String tableGroupName;

    public TableGroupSyncTask(String schemaName,
                              String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        syncTableGroup();
    }

    private void syncTableGroup() {
        try {
            SyncManagerHelper.sync(new TableGroupSyncAction(schemaName, tableGroupName), SyncScope.ALL, true);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table group, schemaName:%s, tableGroupName:%s", schemaName, tableGroupName));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        return "|sync tableGroup, tableGroupName: " + tableGroupName;
    }
}
