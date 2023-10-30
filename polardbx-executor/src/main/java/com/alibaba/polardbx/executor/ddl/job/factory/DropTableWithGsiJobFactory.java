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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiStatisticsInfoSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropTable;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author guxu
 */
public class DropTableWithGsiJobFactory extends DdlJobFactory {

    final private DropTableWithGsiPreparedData preparedData;
    final private Map<String, List<List<String>>> primaryTableTopology;
    final private List<PhyDdlTableOperation> primaryTablePhysicalPlans;
    final private Map<String, Map<String, List<List<String>>>> indexTableTopologyMap;
    final private ExecutionContext executionContext;

    private final String schemaName;
    private final String primaryTableName;

    private static final String BEGIN_DROP_PRIMARY_TABLE = "BEGIN_DROP_PRIMARY_TABLE";

    public DropTableWithGsiJobFactory(@Deprecated DDL ddl,
                                      DropTableWithGsiPreparedData preparedData,
                                      ExecutionContext executionContext) {

        DropTableWithGsiBuilder dropTableWithGsiBuilder =
            new DropTableWithGsiBuilder(ddl, preparedData, executionContext);

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
        ExecutableDdlJob4DropTable dropPrimaryTableJob =
            (ExecutableDdlJob4DropTable) new DropTableJobFactory(physicalPlanData).create();
        result.combineTasks(dropPrimaryTableJob);

        Map<String, DropGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, DropGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final DropGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            ExecutableDdlJob4DropGsi dropGsiJob =
                (ExecutableDdlJob4DropGsi) DropGsiJobFactory.create(gsiPreparedData, executionContext, true, false);

            DdlTask gsiStatisticsInfoTask = new GsiStatisticsInfoSyncTask(
                gsiPreparedData.getSchemaName(),
                gsiPreparedData.getPrimaryTableName(),
                gsiPreparedData.getIndexTableName(),
                GsiStatisticsSyncAction.DELETE_RECORD,
                null);

            result.addTaskRelationship(dropGsiJob.getValidateTask(), dropPrimaryTableJob.getRemoveMetaTask());
            result.addSequentialTasksAfter(dropPrimaryTableJob.getCdcDdlMarkTask(),
                Lists.newArrayList(
                    dropGsiJob.getDropGsiPhyDdlTask(),
                    dropGsiJob.getDropGsiTableRemoveMetaTask(),
                    dropGsiJob.getFinalSyncTask(),
                    gsiStatisticsInfoTask
                )
            );
            tableVersions.put(gsiPreparedData.getTableName(),
                gsiPreparedData.getTableVersion());
        }
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getPrimaryTablePreparedData().getSchemaName(), tableVersions);

        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, dropPrimaryTableJob.getHead());

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