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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author guxu
 */
public class CreatePartitionTableWithGsiJobFactory extends DdlJobFactory {

    @Deprecated
    private final DDL ddl;
    private final CreateTableWithGsiPreparedData preparedData;

    Map<String, List<List<String>>> primaryTableTopology;
    List<PhyDdlTableOperation> primaryTablePhysicalPlans;
    Map<String, List<PhyDdlTableOperation>> indexTablePhysicalPlansMap;

    private final String schemaName;
    private final String primaryTableName;

    private final ExecutionContext executionContext;

    public CreatePartitionTableWithGsiJobFactory(@Deprecated DDL ddl,
                                                 CreateTableWithGsiPreparedData preparedData,
                                                 ExecutionContext executionContext) {
        CreatePartitionTableWithGsiBuilder createTableWithGsiBuilder =
            new CreatePartitionTableWithGsiBuilder(ddl, preparedData, executionContext);

        createTableWithGsiBuilder.build();

        Map<String, List<List<String>>> primaryTableTopology = createTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = createTableWithGsiBuilder.getPrimaryTablePhysicalPlans();

        this.ddl = ddl;
        this.preparedData = preparedData;
        this.primaryTableTopology = primaryTableTopology;
        this.primaryTablePhysicalPlans = primaryTablePhysicalPlans;
        this.indexTablePhysicalPlansMap = createTableWithGsiBuilder.getIndexTablePhysicalPlansMap();
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
        boolean isAutoPartition = this.preparedData.getPrimaryTablePreparedData().isAutoPartition();

        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(
                primaryTableTopology,
                primaryTablePhysicalPlans,
                false,
                isAutoPartition);
        ExecutableDdlJob thisParentJob =
            new CreatePartitionTableJobFactory(preparedData.getPrimaryTablePreparedData().isAutoPartition(),
                preparedData.getPrimaryTablePreparedData().isTimestampColumnDefault(),
                preparedData.getPrimaryTablePreparedData().getBinaryColumnDefaultValues(),
                physicalPlanData, executionContext, preparedData.getPrimaryTablePreparedData(), null).create();
        if (preparedData.getPrimaryTablePreparedData().isNeedToGetTableGroupLock()) {
            return thisParentJob;
        }

        ExecutableDdlJob4CreatePartitionTable createTableJob = (ExecutableDdlJob4CreatePartitionTable) thisParentJob;
        ;
        createTableJob.removeTaskRelationship(
            createTableJob.getCreateTableAddTablesMetaTask(),
            createTableJob.getCdcDdlMarkTask()
        );
        result.combineTasks(createTableJob);
        result.addExcludeResources(createTableJob.getExcludeResources());

        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            ExecutableDdlJob thisJob =
                CreatePartitionGsiJobFactory.create4CreateTableWithGsi(ddl, gsiPreparedData, executionContext);
            if (gsiPreparedData.isNeedToGetTableGroupLock()) {
                return thisJob;
            }
            ExecutableDdlJob4CreatePartitionGsi gsiJob = (ExecutableDdlJob4CreatePartitionGsi) thisJob;
            result.combineTasks(gsiJob);
            result.addTaskRelationship(
                createTableJob.getCreateTableAddTablesMetaTask(),
                gsiJob.getCreateGsiValidateTask()
            );
            result.addTaskRelationship(gsiJob.getLastTask(), createTableJob.getCdcDdlMarkTask());
            result.addExcludeResources(gsiJob.getExcludeResources());
            result.addTask(gsiJob.getCreateGsiPreCheckTask());
            result.addTaskRelationship(createTableJob.getCreatePartitionTableValidateTask(),
                gsiJob.getCreateGsiPreCheckTask());
            result.addTaskRelationship(gsiJob.getCreateGsiPreCheckTask(),
                createTableJob.getCreateTableAddTablesPartitionInfoMetaTask());
        }
        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        if (indexTablePhysicalPlansMap != null) {
            indexTablePhysicalPlansMap.keySet().forEach(indexTableName -> {
                resources.add(concatWithDot(schemaName, indexTableName));
            });
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
