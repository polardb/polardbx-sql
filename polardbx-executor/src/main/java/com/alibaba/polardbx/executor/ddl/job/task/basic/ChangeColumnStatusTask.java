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

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;

@TaskName(name = "ChangeColumnStatusTask")
@Getter
public class ChangeColumnStatusTask extends BaseGmsTask {
    private final List<String> targetColumns;
    private final int prevStatus;
    private final int nextStatus;

    public ChangeColumnStatusTask(String schemaName, String logicalTableName, List<String> targetColumns,
                                  ColumnStatus prevStatus, ColumnStatus nextStatus) {
        super(schemaName, logicalTableName);
        this.targetColumns = targetColumns;
        this.prevStatus = prevStatus.getValue();
        this.nextStatus = nextStatus.getValue();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.changeColumnStatus(metaDbConnection, schemaName, logicalTableName, targetColumns,
            ColumnStatus.convert(nextStatus));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.changeColumnStatus(metaDbConnection, schemaName, logicalTableName, targetColumns,
            ColumnStatus.convert(prevStatus));
    }

    @Override
    protected String remark() {
        return String.format("change column [%s] status from [%s] to [%s]", targetColumns,
            ColumnStatus.convert(prevStatus), ColumnStatus.convert(nextStatus));
    }
}
