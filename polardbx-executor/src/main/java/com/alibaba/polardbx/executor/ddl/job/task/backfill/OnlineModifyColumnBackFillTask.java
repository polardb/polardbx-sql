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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ColumnBackFill;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@TaskName(name = "OnlineModifyColumnBackFillTask")
@Getter
public class OnlineModifyColumnBackFillTask extends BaseBackfillTask {

    String tableName;
    String sourceColumn;
    String targetColumn;

    @JSONCreator
    public OnlineModifyColumnBackFillTask(String schemaName, String tableName, String sourceColumn,
                                                    String targetColumn) {
        super(schemaName);
        this.tableName = tableName;
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        ColumnBackFill columnBackFill =
            ColumnBackFill.createColumnBackfill(schemaName, tableName, sourceColumn, targetColumn, executionContext);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(columnBackFill, executionContext);
    }

    @Override
    public String remark() {
        return String.format("|Backfill columns in %s, from %s to %s", tableName, sourceColumn, targetColumn);
    }

}
