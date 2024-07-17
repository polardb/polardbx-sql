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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RebuildTableChangeMetaTask")
public class RebuildTableChangeMetaTask extends BaseGmsTask {

    public RebuildTableChangeMetaTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        EventLogger.log(EventType.DDL_INFO, "Online modify column start");

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.updateRebuildingTableFlag(schemaName, logicalTableName, false);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        EventLogger.log(EventType.DDL_WARN, "Online modify column rollback");

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.updateRebuildingTableFlag(schemaName, logicalTableName, true);
    }

    @Override
    public String remark() {
        String sb = "set rebuilding table flag on table " + this.getLogicalTableName();
        return "|" + sb;
    }
}
