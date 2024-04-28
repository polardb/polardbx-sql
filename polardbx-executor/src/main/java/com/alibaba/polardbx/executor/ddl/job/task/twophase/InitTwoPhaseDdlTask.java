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

package com.alibaba.polardbx.executor.ddl.job.task.twophase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper.genHashCodeForPhyTableDDL;

@TaskName(name = "InitTwoPhaseDdlTask")
@Getter
public class InitTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;

    final private String sqlTemplate;
    private Map<String, String> physicalTableHashCodeMap;

    @JSONCreator
    public InitTwoPhaseDdlTask(String schemaName, String logicalTableName,
                               Map<String, Set<String>> sourcePhyTableNames,
                               String sqlTemplate,
                               ComplexTaskMetaManager.ComplexTaskType taskType,
                               Long twoPhaseDdlId,
                               Map<String, String> physicalTableHashCodeMap
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.sqlTemplate = sqlTemplate;
        this.twoPhaseDdlId = twoPhaseDdlId;
        this.physicalTableHashCodeMap = physicalTableHashCodeMap;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
        }
        twoPhaseDdlManager.setJobId(jobId);

        // should be idempotent
        twoPhaseDdlManager.twoPhaseDdlInit(
            logicalTableName,
            executionContext
        );

        physicalTableHashCodeMap = TwoPhaseDdlManager.calPhyTableHashCodeMap(schemaName, sourcePhyTableNames);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
            twoPhaseDdlManager.setJobId(jobId);
        }
        try {
            twoPhaseDdlManager.twoPhaseDdlFinish(
                schemaName,
                logicalTableName,
                executionContext
            );
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public static String getTaskName() {
        return "InitTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|init TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
