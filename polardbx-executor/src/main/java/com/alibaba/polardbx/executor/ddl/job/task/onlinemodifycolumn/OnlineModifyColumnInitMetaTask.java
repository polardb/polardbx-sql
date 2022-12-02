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
import java.util.List;

@Getter
@TaskName(name = "OnlineModifyColumnInitMetaTask")
public class OnlineModifyColumnInitMetaTask extends BaseGmsTask {
    private final String columnName;

    public OnlineModifyColumnInitMetaTask(String schemaName, String logicalTableName, String columnName) {
        super(schemaName, logicalTableName);
        this.columnName = columnName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.onlineModifyColumnInitColumn(metaDbConnection, schemaName, logicalTableName, columnName);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.onlineModifyColumnInitColumnRollBack(metaDbConnection, schemaName, logicalTableName, columnName);
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("online modify column on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }

}
