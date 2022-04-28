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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OSSTaskUtils {
    public static Pair<String, String> getSingleTopology(String sourceLogicalSchema, String sourceLogicalTable, TableMeta sourceTableMeta) {
        Pair<String, String> singlePhySchemaAndTable = null;
        boolean isSingle =
                OptimizerContext.getContext(sourceLogicalSchema).getRuleManager().isTableInSingleDb(sourceLogicalTable);
        if (isSingle) {
            String singlePhySchema, singlePhyTable;
            for (Map.Entry<String, Set<String>> e : sourceTableMeta.getLatestTopology().entrySet()) {
                singlePhySchema = e.getKey();
                for (String setVal : e.getValue()) {
                    singlePhyTable = setVal;
                    singlePhySchemaAndTable = Pair.of(singlePhySchema, singlePhyTable);
                    break;
                }
            }
        }
        return singlePhySchemaAndTable;
    }

    public static PartitionInfo getSourcePartitionInfo(ExecutionContext executionContext, String sourceLogicalSchema,
                                                       String sourceLogicalTable) {
        PartitionInfoManager partitionInfoManager =
                executionContext.getSchemaManager(sourceLogicalSchema).getTddlRuleManager().getPartitionInfoManager();
        return partitionInfoManager.getPartitionInfo(sourceLogicalTable);
    }

    public static Pair<String, String> getSourcePhyTable(PartitionInfo loadTablePartitionInfo, String partName) {
        List<String> partitionNameList = ImmutableList.of(partName);
        PhysicalPartitionInfo loadTablePhysicalPartitionInfo =
                loadTablePartitionInfo
                        .getPhysicalPartitionTopology(partitionNameList)
                        .values()
                        .stream()
                        .findFirst().get().get(0);
        return Pair.of(loadTablePhysicalPartitionInfo.getGroupKey(), loadTablePhysicalPartitionInfo.getPhyTable());
    }

    public static List<DdlTask> dropTableTasks(Engine engine, String schemaName, String logicalTableName, boolean ifExists, ExecutionContext executionContext) {
        DropTableBuilder dropTableBuilder = DropPartitionTableBuilder.createBuilder(schemaName, logicalTableName, true, executionContext);
        dropTableBuilder.build();
        PhysicalPlanData physicalPlanData = dropTableBuilder.genPhysicalPlanData();

        List<Long> tableGroupIds = new ArrayList<>();

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
        DropOssFilesTask dropOssFilesTask = new DropOssFilesTask(engine.name(), schemaName, logicalTableName, null);
        DropTablePhyDdlTask phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
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
        StoreTableLocalityTask dropLocality =
                StoreTableLocalityTask.buildDropLocalityTask(schemaName, logicalTableName);

        ExecutableDdlJob4DropPartitionTable executableDdlJob = new ExecutableDdlJob4DropPartitionTable();
        /**
         * todo chenyi
         * DropTableJobFactory中已经把元数据操作都合并到一个Task中了
         * 考虑将DropTableHideTableMetaTask、DropPartitionTableRemoveMetaTask也合并一下？
         */
        tasks.add(validateTask);
        tasks.add(dropLocality);
        tasks.add(dropTableHideTableMetaTask);
        tasks.add(dropOssFilesTask);
        tasks.add(phyDdlTask);
        tasks.add(removeMetaTask);
        if (syncTableGroup != null) {
            tasks.add(syncTableGroup);
        }
        tasks.add(tableSyncTask);
        return tasks;
    }
}
