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
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenamePartitionTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;

import java.util.Set;

public class RenameTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final String newLogicalTableName;
    private final ExecutionContext executionContext;

    public RenameTableJobFactory(PhysicalPlanData physicalPlanData, ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.newLogicalTableName = physicalPlanData.getNewLogicalTableName();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        GsiValidator.validateAllowRenameOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DdlTask validateTask = new RenameTableValidateTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask addMetaTask = new RenameTableAddMetaTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData);

        DdlTask phyDdlTask;
        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartitionDb) {
            phyDdlTask = new RenamePartitionTablePhyDdlTask(schemaName, physicalPlanData);
        } else {
            phyDdlTask = new RenameTablePhyDdlTask(schemaName, physicalPlanData).onExceptionTryRecoveryThenRollback();
        }
        DdlTask updateMetaTask = new RenameTableUpdateMetaTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask syncTask = new RenameTableSyncTask(schemaName, logicalTableName, newLogicalTableName);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask,
            phyDdlTask,
            cdcDdlMarkTask,
            updateMetaTask,
            syncTask
        ));
        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        resources.add(concatWithDot(schemaName, newLogicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
