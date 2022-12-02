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

package com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn;

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "OnlineModifyColumnStopMultiWriteTask")
public class OnlineModifyColumnStopMultiWriteTask extends BaseGmsTask {
    private final boolean changeColumn;

    private final String newColumnName;
    private final String oldColumnName;

    public OnlineModifyColumnStopMultiWriteTask(String schemaName, String logicalTableName,
                                                boolean changeColumn, String newColumnName,
                                                String oldColumnName) {
        super(schemaName, logicalTableName);
        this.changeColumn = changeColumn;

        this.newColumnName = newColumnName;
        this.oldColumnName = oldColumnName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (changeColumn) {
            TableMetaChanger.onlineModifyColumnStopMultiWrite(metaDbConnection, schemaName, logicalTableName,
                newColumnName, oldColumnName);
        } else {
            TableMetaChanger.onlineModifyColumnStopMultiWrite(metaDbConnection, schemaName, logicalTableName,
                oldColumnName, newColumnName);
        }

        // We have stopped column multi-write, so data may not be same anymore, we can not rollback from now
        updateSupportedCommands(true, false, metaDbConnection);
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("online modify column stop multi-write on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }

}
