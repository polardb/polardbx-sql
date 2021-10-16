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
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import com.google.common.collect.Lists;

import java.util.Set;

public class DropPartitionTableJobFactory extends DropTableJobFactory {

    public DropPartitionTableJobFactory(PhysicalPlanData physicalPlanData) {
        super(physicalPlanData);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DropTableValidateTask validateTask = new DropTableValidateTask(schemaName, logicalTableName);
        DropTableHideTableMetaTask dropTableHideTableMetaTask =
            new DropTableHideTableMetaTask(schemaName, logicalTableName);
        DropTablePhyDdlTask phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
        CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData);
        DropPartitionTableRemoveMetaTask removeMetaTask =
            new DropPartitionTableRemoveMetaTask(schemaName, logicalTableName);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        StoreTableLocalityTask dropLocality =
            StoreTableLocalityTask.buildDropLocalityTask(schemaName, logicalTableName);

        ExecutableDdlJob4DropPartitionTable executableDdlJob = new ExecutableDdlJob4DropPartitionTable();
        /**
         * todo chenyi
         * DropTableJobFactory中已经把元数据操作都合并到一个Task中了
         * 考虑将DropTableHideTableMetaTask、DropPartitionTableRemoveMetaTask也合并一下？
         */
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            dropLocality,
            dropTableHideTableMetaTask,
            phyDdlTask,
            cdcDdlMarkTask,
            removeMetaTask,
            tableSyncTask
        ));
        //labels should be replaced by fields in ExecutableDdlJob4DropTable
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setDropTableHideTableMetaTask(dropTableHideTableMetaTask);
        executableDdlJob.setPhyDdlTask(phyDdlTask);
        executableDdlJob.setCdcDdlMarkTask(cdcDdlMarkTask);
        executableDdlJob.setRemoveMetaTask(removeMetaTask);
        executableDdlJob.setTableSyncTask(tableSyncTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(physicalPlanData.getSchemaName(), logicalTableName));
    }

}
