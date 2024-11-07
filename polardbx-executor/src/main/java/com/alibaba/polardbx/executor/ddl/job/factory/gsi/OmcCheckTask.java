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

/**
 * @author wumu
 */
@TaskName(name = "OmcCheckTask")
@Getter
public class OmcCheckTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private String indexTableName;
    final private Map<String, String> srcCheckColumnMap;
    final private Map<String, String> dstCheckColumnMap;

    @JSONCreator
    public OmcCheckTask(String schemaName, String logicalTableName, String indexTableName,
                        Map<String, String> srcCheckColumnMap, Map<String, String> dstCheckColumnMap) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexTableName = indexTableName;
        this.srcCheckColumnMap = srcCheckColumnMap;
        this.dstCheckColumnMap = dstCheckColumnMap;
        onExceptionTryRecoveryThenPause();
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
                    isPrimaryBroadCast, isGsiBroadCast, true);
            checkTask.setSrcCheckColumnMap(srcCheckColumnMap);
            checkTask.setDstCheckColumnMap(dstCheckColumnMap);
            checkTask.checkInBackfill(executionContext);
        }
    }
}