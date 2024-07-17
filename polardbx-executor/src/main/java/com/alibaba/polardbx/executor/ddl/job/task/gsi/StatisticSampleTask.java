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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "StatisticSampleTask")
@Getter
public class StatisticSampleTask extends BaseDdlTask {
    private String logicalTableName;

    @JSONCreator
    public StatisticSampleTask(String schemaName,
                               String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        StatisticUtils.sampleOneTable(schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, LogPattern.PROCESS_END,
            new String[] {"ddl sample task", schemaName + "," + logicalTableName}, LogLevel.NORMAL);

        LOGGER.info(String.format("sample table task. schema:%s, table:%s", schemaName, logicalTableName));

        FailPoint.injectExceptionFromHint("FP_STATISTIC_SAMPLE_ERROR", executionContext);
    }
}
