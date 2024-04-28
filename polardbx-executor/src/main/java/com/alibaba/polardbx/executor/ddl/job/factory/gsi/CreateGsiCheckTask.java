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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckGsiTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlSelect;

import java.util.Map;

@TaskName(name = "CreateGsiCheckTask")
@Getter
public class CreateGsiCheckTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private String indexTableName;
    public Map<String, String> virtualColumns;
    public Map<String, String> backfillColumnMap;

    @JSONCreator
    public CreateGsiCheckTask(String schemaName, String logicalTableName, String indexTableName,
                              Map<String, String> virtualColumns, Map<String, String> backfillColumnMap) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexTableName = indexTableName;
        this.virtualColumns = virtualColumns;
        this.backfillColumnMap = backfillColumnMap;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        final boolean skipCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET_CHECKER);

        if (!skipCheck) {
            String lockMode = SqlSelect.LockMode.UNDEF.toString();
            GsiChecker.Params params = GsiChecker.Params.buildFromExecutionContext(executionContext);
            boolean isPrimaryBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(logicalTableName);
            boolean isGsiBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(indexTableName);
            CheckGsiTask checkTask =
                new CheckGsiTask(schemaName, logicalTableName, indexTableName, lockMode, lockMode, params, false, "",
                    isPrimaryBroadCast, isGsiBroadCast, virtualColumns, backfillColumnMap);
            checkTask.checkInBackfill(executionContext);
        }
    }
}