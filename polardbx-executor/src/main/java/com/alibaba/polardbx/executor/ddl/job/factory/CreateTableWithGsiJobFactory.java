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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreateGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTable;
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
public class CreateTableWithGsiJobFactory extends DdlJobFactory {

    @Deprecated
    private final DDL ddl;
    private final CreateTableWithGsiPreparedData preparedData;

    Map<String, List<List<String>>> primaryTableTopology;
    List<PhyDdlTableOperation> primaryTablePhysicalPlans;
    Map<String, Map<String, List<List<String>>>> indexTableTopologyMap;

    private final String schemaName;
    private final String primaryTableName;

    private final ExecutionContext executionContext;

    public CreateTableWithGsiJobFactory(@Deprecated DDL ddl,
                                        CreateTableWithGsiPreparedData preparedData,
                                        ExecutionContext executionContext) {
        CreateTableWithGsiBuilder createTableWithGsiBuilder =
            new CreateTableWithGsiBuilder(ddl, preparedData, executionContext);

        createTableWithGsiBuilder.build();

        Map<String, List<List<String>>> primaryTableTopology = createTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = createTableWithGsiBuilder.getPrimaryTablePhysicalPlans();

        Map<String, Map<String, List<List<String>>>> indexTableTopologyMap =
            createTableWithGsiBuilder.getIndexTableTopologyMap();

        this.ddl = ddl;
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

        boolean isAutoPartition = this.preparedData.getPrimaryTablePreparedData().isAutoPartition();
        boolean hasTimestampColumnDefault = this.preparedData.getPrimaryTablePreparedData().isTimestampColumnDefault();
        Map<String, String> binaryColumnDefaultValues =
            this.preparedData.getPrimaryTablePreparedData().getBinaryColumnDefaultValues();
        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(
                primaryTableTopology,
                primaryTablePhysicalPlans,
                false,
                isAutoPartition);
        ExecutableDdlJob4CreateTable createTableJob = (ExecutableDdlJob4CreateTable) new CreateTableJobFactory(
            false,
            hasTimestampColumnDefault,
            binaryColumnDefaultValues,
            physicalPlanData,
            executionContext).create();
        result.combineTasks(createTableJob);
        result.removeTaskRelationship(
            createTableJob.getCreateTableAddTablesMetaTask(),
            createTableJob.getCdcDdlMarkTask());

        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            ExecutableDdlJob4CreateGsi gsiJob = (ExecutableDdlJob4CreateGsi)
                CreateGsiJobFactory.create4CreateTableWithGsi(ddl, gsiPreparedData, executionContext);
            result.combineTasks(gsiJob);
            result.removeTaskRelationship(
                gsiJob.getCreateGsiValidateTask(), gsiJob.getCreateTableAddTablesExtMetaTask());
            result.addTaskRelationship(
                createTableJob.getCreateTableAddTablesExtMetaTask(), gsiJob.getCreateGsiValidateTask());
            result.addTaskRelationship(
                gsiJob.getCreateGsiValidateTask(), createTableJob.getCreateTablePhyDdlTask());
            result.removeTaskRelationship(
                createTableJob.getCreateTableAddTablesExtMetaTask(), createTableJob.getCreateTablePhyDdlTask());
            result.addTaskRelationship(
                createTableJob.getCreateTableAddTablesMetaTask(), gsiJob.getCreateTableAddTablesExtMetaTask());
            result.addTaskRelationship(
                gsiJob.getLastTask(), createTableJob.getCdcDdlMarkTask());
        }

        result.setExceptionActionForAllSuccessor(
            createTableJob.getCdcDdlMarkTask(), DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

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
