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
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sampleTable;

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
        sampleTable(schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(String.format("sample table task. schema:%s, table:%s", schemaName, logicalTableName));
    }
}
