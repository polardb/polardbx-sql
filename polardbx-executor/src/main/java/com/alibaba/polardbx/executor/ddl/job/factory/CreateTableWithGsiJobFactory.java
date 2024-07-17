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
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreateGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.CreateColumnarIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiStatisticsInfoSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateColumnarIndex;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateSelect;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTable;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.LikeTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.HashMap;
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

    protected String selectSql;

    public void setSelectSql(String sql) {
        selectSql = sql;
    }

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
        Map<String, String> specialDefaultValues =
            this.preparedData.getPrimaryTablePreparedData().getSpecialDefaultValues();
        Map<String, Long> specialDefaultValueFlags =
            this.preparedData.getPrimaryTablePreparedData().getSpecialDefaultValueFlags();
        List<ForeignKeyData> addedForeignKeys =
            this.preparedData.getPrimaryTablePreparedData().getAddedForeignKeys();
        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(
                primaryTableTopology,
                primaryTablePhysicalPlans,
                false,
                isAutoPartition,
                executionContext);
        CreateTableJobFactory executableDdlJob4CreateTable = new CreateTableJobFactory(
            false,
            hasTimestampColumnDefault,
            specialDefaultValues,
            specialDefaultValueFlags,
            addedForeignKeys,
            physicalPlanData,
            preparedData.getDdlVersionId(),
            executionContext,
            preparedData.getPrimaryTablePreparedData().getLikeTableInfo()
        );
//        executableDdlJob4CreateTable.setSelectSql(selectSql);
        ExecutableDdlJob4CreateTable createTableJob =
            (ExecutableDdlJob4CreateTable) executableDdlJob4CreateTable.create();
        result.combineTasks(createTableJob);
        result.removeTaskRelationship(
            createTableJob.getCreateTableAddTablesMetaTask(),
            createTableJob.getCdcDdlMarkTask());

        final Map<String, CreateGlobalIndexPreparedData> cciPreparedDataMap = new HashMap<>();

        // Create global secondary index
        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData indexPreparedData = entry.getValue();
            if (indexPreparedData.isColumnarIndex()) {
                // Clustered columnar index
                final ExecutableDdlJob4CreateColumnarIndex cciJob = (ExecutableDdlJob4CreateColumnarIndex)
                    CreateColumnarIndexJobFactory.create4CreateCci(ddl, indexPreparedData, executionContext);
                if (indexPreparedData.isNeedToGetTableGroupLock()) {
                    return cciJob;
                }
                // Add cci tasks
                result.combineTasks(cciJob);

                // Add relationship with before tasks
                result.addTaskRelationship(
                    createTableJob.getCreateTableAddTablesMetaTask(),
                    cciJob.getCreateColumnarIndexValidateTask()
                );

                // Add Relationship with after tasks
                result.addTaskRelationship(cciJob.getLastTask(), createTableJob.getCdcDdlMarkTask());

                // Add exclusive resources
                result.addExcludeResources(cciJob.getExcludeResources());
            } else {
                // Global secondary index
                ExecutableDdlJob4CreateGsi gsiJob = (ExecutableDdlJob4CreateGsi)
                    CreateGsiJobFactory.create4CreateTableWithGsi(ddl, indexPreparedData, executionContext);
                DdlTask gsiStatisticsInfoTask = new GsiStatisticsInfoSyncTask(
                    indexPreparedData.getSchemaName(),
                    indexPreparedData.getPrimaryTableName(),
                    indexPreparedData.getIndexTableName(),
                    GsiStatisticsSyncAction.INSERT_RECORD,
                    null);
                gsiJob.appendTask(gsiStatisticsInfoTask);
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
                result.addTaskRelationship(gsiJob.getLastTask(), createTableJob.getCdcDdlMarkTask());
            }
        }

        result.setExceptionActionForAllSuccessor(
            createTableJob.getCdcDdlMarkTask(), DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        if (selectSql != null) {
            InsertIntoTask insertIntoTask = new InsertIntoTask(schemaName, primaryTableName, selectSql, null, 0);
            affectRows = insertIntoTask.getAffectRows();
            ExecutableDdlJob insertJob = new ExecutableDdlJob();
            insertJob.addTask(insertIntoTask);
            ExecutableDdlJob4CreateSelect ans = new ExecutableDdlJob4CreateSelect();
            ans.appendJob2(result);
            ans.appendJob2(insertJob);
            ans.setInsertTask(insertIntoTask);
            //insert 只能rollback，无法重试
            insertIntoTask.setExceptionAction(DdlExceptionAction.ROLLBACK);
            return ans;
        }

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
        if (preparedData.getPrimaryTablePreparedData() != null
            && preparedData.getPrimaryTablePreparedData().getLikeTableInfo() != null) {
            LikeTableInfo likeTableInfo = preparedData.getPrimaryTablePreparedData().getLikeTableInfo();
            resources.add(concatWithDot(likeTableInfo.getSchemaName(), likeTableInfo.getTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
