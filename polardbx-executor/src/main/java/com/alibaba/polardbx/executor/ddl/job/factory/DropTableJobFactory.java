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
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropTable;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

public class DropTableJobFactory extends DdlJobFactory {

    protected final PhysicalPlanData physicalPlanData;
    protected final String schemaName;
    protected final String logicalTableName;

    public DropTableJobFactory(PhysicalPlanData physicalPlanData) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DropTableValidateTask validateTask = new DropTableValidateTask(schemaName, logicalTableName);
        DropTableRemoveMetaTask removeMetaTask = new DropTableRemoveMetaTask(schemaName, logicalTableName);
        StoreTableLocalityTask storeTableLocalityTask =
            new StoreTableLocalityTask(schemaName, logicalTableName, "", false);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        DropTablePhyDdlTask phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
        CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, false);
        ExecutableDdlJob4DropTable executableDdlJob = new ExecutableDdlJob4DropTable();

        List<DdlTask> taskList = Lists.newArrayList(
            validateTask,
            storeTableLocalityTask,
            removeMetaTask,
            tableSyncTask,
            phyDdlTask,
            cdcDdlMarkTask);

        // sync foreign key table meta
        taskList.addAll(FactoryUtils.getFkTableSyncTasks(schemaName, logicalTableName));

        executableDdlJob.addSequentialTasks(taskList);

        //labels should be replaced by fields in ExecutableDdlJob4DropTable
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setRemoveMetaTask(removeMetaTask);
        executableDdlJob.setTableSyncTaskAfterRemoveMeta(tableSyncTask);
        executableDdlJob.setPhyDdlTask(phyDdlTask);
        executableDdlJob.setCdcDdlMarkTask(cdcDdlMarkTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
