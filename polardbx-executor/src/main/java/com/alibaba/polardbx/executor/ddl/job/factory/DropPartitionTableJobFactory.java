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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateTableRemoveTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DropPartitionTableJobFactory extends DropTableJobFactory {

    private List<Long> tableGroupIds = new ArrayList<>();

    private ExecutionContext executionContext;

    public DropPartitionTableJobFactory(PhysicalPlanData physicalPlanData, ExecutionContext executionContext) {
        super(physicalPlanData);
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        Long tableGroupId = -1L;
        TableGroupConfig tableGroupConfig = null;
        if (partitionInfo != null) {
            tableGroupId = partitionInfo.getTableGroupId();
            tableGroupConfig = physicalPlanData.getTableGroupConfig();
        }
        List<DdlTask> tasks = new ArrayList<>();
        DropPartitionTableValidateTask validateTask =
            new DropPartitionTableValidateTask(schemaName, logicalTableName, tableGroupIds, tableGroupConfig);
        DropTableHideTableMetaTask dropTableHideTableMetaTask =
            new DropTableHideTableMetaTask(schemaName, logicalTableName);
        DropTablePhyDdlTask phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
        CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, false);
        DropPartitionTableRemoveMetaTask removeMetaTask =
            new DropPartitionTableRemoveMetaTask(schemaName, logicalTableName);

        DdlTask syncTableGroup = null;
        if (tableGroupId != -1) {
            //tableGroupConfig from physicalPlanData is not set tableGroup record
            tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigById(tableGroupId);
            syncTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupConfig.getTableGroupRecord().getTg_name());
        }

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob4DropPartitionTable executableDdlJob = new ExecutableDdlJob4DropPartitionTable();
        /**
         * todo chenyi
         * DropTableJobFactory中已经把元数据操作都合并到一个Task中了
         * 考虑将DropTableHideTableMetaTask、DropPartitionTableRemoveMetaTask也合并一下？
         */
        tasks.add(validateTask);
        tasks.add(dropTableHideTableMetaTask);
        Engine engine =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getEngine();
        if (Engine.isFileStore(engine)) {
            // change file meta task
            final ITimestampOracle timestampOracle =
                executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
            if (null == timestampOracle) {
                throw new UnsupportedOperationException("Do not support timestamp oracle");
            }

            long ts = timestampOracle.nextTimestamp();
            UpdateTableRemoveTsTask updateTableRemoveTsTask =
                new UpdateTableRemoveTsTask(engine.name(), schemaName, logicalTableName, ts);
            tasks.add(updateTableRemoveTsTask);
        }
        tasks.add(phyDdlTask);
        if (!Engine.isFileStore(engine)) {
            tasks.add(cdcDdlMarkTask);
        }
        tasks.add(removeMetaTask);
        if (syncTableGroup != null) {
            tasks.add(syncTableGroup);
        }
        tasks.add(tableSyncTask);

        // sync foreign key table meta
        tasks.addAll(FactoryUtils.getFkTableSyncTasks(schemaName, logicalTableName));

        executableDdlJob.addSequentialTasks(tasks);
        //labels should be replaced by fields in ExecutableDdlJob4DropTable
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setDropTableHideTableMetaTask(dropTableHideTableMetaTask);
        executableDdlJob.setPhyDdlTask(phyDdlTask);
        if (!Engine.isFileStore(engine)) {
            executableDdlJob.setCdcDdlMarkTask(cdcDdlMarkTask);
        }
        executableDdlJob.setRemoveMetaTask(removeMetaTask);
        executableDdlJob.setTableSyncTask(tableSyncTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(physicalPlanData.getSchemaName(), logicalTableName));

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            tableGroupIds.add(partitionInfo.getTableGroupId());
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

}
