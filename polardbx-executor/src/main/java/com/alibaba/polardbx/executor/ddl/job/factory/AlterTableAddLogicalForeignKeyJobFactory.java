/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterForeignKeyTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableAddLogicalForeignKeyValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;

public class AlterTableAddLogicalForeignKeyJobFactory extends AlterTableJobFactory {
    public AlterTableAddLogicalForeignKeyJobFactory(
        PhysicalPlanData physicalPlanData,
        AlterTablePreparedData preparedData,
        LogicalAlterTable logicalAlterTable,
        ExecutionContext executionContext) {
        super(physicalPlanData, preparedData, logicalAlterTable, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob4AlterTable executableDdlJob = new ExecutableDdlJob4AlterTable();

        boolean isForeignKeysDdl =
            !prepareData.getAddedForeignKeys().isEmpty() || !prepareData.getDroppedForeignKeys().isEmpty();
        boolean isForeignKeyCdcMark = isForeignKeysDdl && !executionContext.getDdlContext().isFkRepartition();

        List<DdlTask> taskList = new ArrayList<>();

        AlterTableAddLogicalForeignKeyValidateTask validateTask =
            new AlterTableAddLogicalForeignKeyValidateTask(schemaName, logicalTableName,
                prepareData.getAddedForeignKeys().get(0), prepareData.getTableVersion());
        taskList.add(validateTask);

        DdlTask cdcDdlMarkTask =
            new CdcDdlMarkTask(schemaName, physicalPlanData, false, isForeignKeyCdcMark, DEFAULT_DDL_VERSION_ID);
        taskList.add(cdcDdlMarkTask);

        if (isForeignKeysDdl) {
            DdlTask updateForeignKeysTask =
                new AlterForeignKeyTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                    physicalPlanData.getDefaultPhyTableName(), prepareData.getAddedForeignKeys(),
                    prepareData.getDroppedForeignKeys(), true);
            taskList.add(updateForeignKeysTask);
        }

        // sync foreign key table meta
        syncFkTables(taskList);
        DdlTask tableSyncTaskAfterShowing = new TableSyncTask(schemaName, logicalTableName);
        taskList.add(tableSyncTaskAfterShowing);

        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTaskAfterShowing);

        executableDdlJob.setTableValidateTask((BaseValidateTask) validateTask);
        executableDdlJob.setTableSyncTask((TableSyncTask) tableSyncTaskAfterShowing);

        return executableDdlJob;
    }
}
