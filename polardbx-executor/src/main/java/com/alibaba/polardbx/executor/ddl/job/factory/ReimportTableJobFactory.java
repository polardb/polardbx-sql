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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.ReimportTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ReimportTableJobFactory extends CreatePartitionTableJobFactory {

    public ReimportTableJobFactory(boolean autoPartition, boolean hasTimestampColumnDefault,
                                   Map<String, String> specialDefaultValues,
                                   Map<String, Long> specialDefaultValueFlags,
                                   List<ForeignKeyData> addedForeignKeys,
                                   PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                   CreateTablePreparedData preparedData, PartitionInfo partitionInfo) {
        super(autoPartition, hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags,
            addedForeignKeys,
            physicalPlanData, executionContext, preparedData, partitionInfo, null);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ReimportTableChangeMetaTask reimportTableChangeMetaTask =
            new ReimportTableChangeMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), addedForeignKeys,
                hasTimestampColumnDefault,
                specialDefaultValues, specialDefaultValueFlags);

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ImmutableList.of(
            reimportTableChangeMetaTask,
            tableSyncTask
        ));

        return executableDdlJob;
    }
}
