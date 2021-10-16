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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TruncateTableWithGsiJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final Map<String, PhysicalPlanData> gsiPhyTruncatePlanMap;
    private final String schemaName;
    private final String logicalTableName;
    private final ExecutionContext executionContext;

    public TruncateTableWithGsiJobFactory(PhysicalPlanData physicalPlanData,
                                          Map<String, PhysicalPlanData> gsiPhyTruncatePlanMap,
                                          ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
        this.gsiPhyTruncatePlanMap = gsiPhyTruncatePlanMap;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);
        GsiValidator.validateAllowTruncateOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DdlTask validateTask = new TruncateTableValidateTask(schemaName, logicalTableName);
        DdlTask primaryPhyTruncateTask = new TruncateTablePhyDdlTask(schemaName, physicalPlanData);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>(gsiPhyTruncatePlanMap.size() * 2);
        taskList.add(validateTask);
        taskList.add(primaryPhyTruncateTask.onExceptionTryRecoveryThenRollback());
        gsiPhyTruncatePlanMap.forEach((gsiName, gsiPhysicalPlanData) -> {
            DdlTask gsiTask = new TruncateTablePhyDdlTask(schemaName, gsiPhysicalPlanData);
            taskList.add(gsiTask.onExceptionTryRecoveryThenRollback());
        });

        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData);
        taskList.add(cdcDdlMarkTask);

        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        if (gsiPhyTruncatePlanMap != null) {
            gsiPhyTruncatePlanMap.keySet().forEach(indexTableName -> {
                resources.add(concatWithDot(schemaName, indexTableName));
            });
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
