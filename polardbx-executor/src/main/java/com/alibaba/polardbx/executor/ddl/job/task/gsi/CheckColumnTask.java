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

import com.alibaba.polardbx.executor.columns.ColumnChecker;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "CheckColumnTask")
public class CheckColumnTask extends BaseBackfillTask {
    private final String tableName;
    private final String checkerColumn;
    private final String sourceColumn;
    private final String targetColumn;
    private final boolean simpleChecker;

    public CheckColumnTask(String schemaName, String tableName, String checkerColumn, String sourceColumn,
                           String targetColumn, boolean simpleChecker) {
        super(schemaName);
        this.tableName = tableName;
        this.checkerColumn = checkerColumn;
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
        this.simpleChecker = simpleChecker;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        ColumnChecker checker =
            new ColumnChecker(schemaName, tableName, checkerColumn, sourceColumn, targetColumn, simpleChecker,
                ColumnChecker.Params.buildFromExecutionContext(executionContext), executionContext);
        checker.checkInBackfill(executionContext);
    }

    @Override
    public String remark() {
        return String.format("|Check column %s and %s on %s", sourceColumn, targetColumn, tableName);
    }
}
