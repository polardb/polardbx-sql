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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropPartitionTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropPartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropPartitionTableWithGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author guxu
 */
public class DropPartitionTableWithGsiJobFactory extends DdlJobFactory {

    final private DropTableWithGsiPreparedData preparedData;
    final private Map<String, List<List<String>>> primaryTableTopology;
    final private List<PhyDdlTableOperation> primaryTablePhysicalPlans;
    final private Map<String, Map<String, List<List<String>>>> indexTableTopologyMap;
    final private ExecutionContext executionContext;

    private final String schemaName;
    private final String primaryTableName;

    private static final String BEGIN_DROP_PRIMARY_TABLE = "BEGIN_DROP_PRIMARY_TABLE";

    public DropPartitionTableWithGsiJobFactory(@Deprecated DDL ddl,
                                               DropTableWithGsiPreparedData preparedData,
                                               ExecutionContext executionContext) {

        DropTableWithGsiBuilder dropTableWithGsiBuilder =
            new DropPartitionTableWithGsiBuilder(ddl, preparedData, executionContext);

        dropTableWithGsiBuilder.build();

        Map<String, List<List<String>>> primaryTableTopology = dropTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = dropTableWithGsiBuilder.getPrimaryTablePhysicalPlans();

        Map<String, Map<String, List<List<String>>>> indexTableTopologyMap =
            dropTableWithGsiBuilder.getIndexTableTopologyMap();

        this.preparedData = preparedData;
        this.primaryTableTopology = primaryTableTopology;
        this.primaryTablePhysicalPlans = primaryTablePhysicalPlans;
        this.indexTableTopologyMap = indexTableTopologyMap;
        this.executionContext = executionContext;

        this.schemaName = preparedData.getPrimaryTablePreparedData().getSchemaName();
        this.primaryTableName = preparedData.getPrimaryTablePreparedData().getTableName();
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob result = new ExecutableDdlJob();
        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(preparedData.getPrimaryTablePreparedData().getTableName(),
            preparedData.getPrimaryTablePreparedData().getTableVersion());

        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(primaryTableTopology, primaryTablePhysicalPlans);

        ExecutableDdlJob4DropPartitionTable dropPrimaryTableJob = (ExecutableDdlJob4DropPartitionTable)
            new DropPartitionTableJobFactory(physicalPlanData, executionContext).create();

        DdlTask validateTask = dropPrimaryTableJob.getValidateTask();
        DdlTask dropPrimaryTableSyncTask = dropPrimaryTableJob.getTableSyncTask();
        DdlTask storeTableLocalityTask = dropPrimaryTableJob.getStoreTableLocalityTask();
        result.combineTasks(dropPrimaryTableJob);

        result.addExcludeResources(dropPrimaryTableJob.getExcludeResources());

        Map<String, DropGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, DropGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final DropGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            final String indexTableName = gsiPreparedData.getIndexTableName();
            ExecutableDdlJob4DropPartitionGsi dropGsiJob = (ExecutableDdlJob4DropPartitionGsi)
                DropPartitionGsiJobFactory.create(gsiPreparedData, executionContext, true, false);

            result.addTaskRelationship(validateTask, dropGsiJob.getValidateTask());
            result.addTaskRelationship(dropGsiJob.getValidateTask(), storeTableLocalityTask);
            result.addTaskRelationship(dropPrimaryTableSyncTask, dropGsiJob.getDropGsiTableHideTableMetaTask());
            result.addTaskRelationship(
                dropGsiJob.getDropGsiTableHideTableMetaTask(), dropGsiJob.getDropGsiPhyDdlTask());
            result.addTaskRelationship(
                dropGsiJob.getDropGsiPhyDdlTask(), dropGsiJob.getGsiDropCleanUpTask());
            result.addTaskRelationship(
                dropGsiJob.getGsiDropCleanUpTask(), dropGsiJob.getDropGsiTableRemoveMetaTask());
            result.addTaskRelationship(
                dropGsiJob.getDropGsiTableRemoveMetaTask(), new TableSyncTask(schemaName, indexTableName));
            result.addExcludeResources(dropGsiJob.getExcludeResources());
            tableVersions.put(gsiPreparedData.getTableName(),
                gsiPreparedData.getTableVersion());
        }
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getPrimaryTablePreparedData().getSchemaName(), tableVersions);

        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, dropPrimaryTableJob.getHead());

        List<String> tableNames = new ArrayList<>();
        tableNames.add(primaryTableName);
        tableNames.addAll(indexTableTopologyMap.keySet());
        List<TableGroupConfig> tableGroupConfigs = FactoryUtils.getTableGroupConfigByTableName(schemaName, tableNames);
        DropPartitionTableWithGsiValidateTask tableGroupValidateTask =
            new DropPartitionTableWithGsiValidateTask(schemaName, primaryTableName,
                new ArrayList<>(indexTableTopologyMap.keySet()), tableGroupConfigs);

        result.addTask(tableGroupValidateTask);
        result.addTaskRelationship(tableGroupValidateTask, validateTableVersionTask);

//        result.setMaxParallelism(gsiPreparedDataMap.size() + 1);

        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        if (indexTableTopologyMap != null) {
            indexTableTopologyMap.keySet().forEach(indexTableName -> {
                resources.add(concatWithDot(schemaName, indexTableName));
            });
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
