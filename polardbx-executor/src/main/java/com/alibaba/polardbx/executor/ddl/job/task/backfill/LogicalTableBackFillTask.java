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
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@TaskName(name = "LogicalTableBackFillTask")
@Getter
public class LogicalTableBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    public String sourceTableName;
    public String targetTableName;
    public Map<String, String> virtualColumns;
    public Map<String, String> backfillColumnMap;
    public List<String> modifyStringColumns;
    public boolean useChangeSet;
    public boolean modifyColumn;
    public boolean mirrorCopy;

    @JSONCreator
    public LogicalTableBackFillTask(String schemaName,
                                    String sourceTableName,
                                    String targetTableName,
                                    Map<String, String> virtualColumns,
                                    Map<String, String> backfillColumnMap,
                                    List<String> modifyStringColumns,
                                    boolean useChangeSet,
                                    boolean mirrorCopy,
                                    boolean modifyColumn) {
        super(schemaName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.virtualColumns = virtualColumns;
        this.backfillColumnMap = backfillColumnMap;
        this.modifyStringColumns = modifyStringColumns;
        this.useChangeSet = useChangeSet;
        this.modifyColumn = modifyColumn;
        this.mirrorCopy = mirrorCopy;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        GsiBackfill backFillPlan =
            GsiBackfill.createGsiBackfill(schemaName, sourceTableName, targetTableName, executionContext);
        backFillPlan.setUseChangeSet(useChangeSet);
        backFillPlan.setModifyColumn(modifyColumn);
        backFillPlan.setMirrorCopy(mirrorCopy);
        backFillPlan.setModifyStringColumns(modifyStringColumns);
        backFillPlan.setBackfillColumnMap(backfillColumnMap);
        backFillPlan.setVirtualColumnMap(virtualColumns);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(backFillPlan, executionContext);
    }

}
