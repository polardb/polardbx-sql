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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RepartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterTableRepartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author guxu wumu
 */
public class RepartitionJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String primaryTableName;
    private final String indexTableName;
    private final boolean isSingle;
    private final boolean isBroadcast;
    private final PhysicalPlanData physicalPlanData;
    private final List<String> dropIndexes;
    private final Map<String, List<String>> backfillIndexs;
    private final String primaryTableDefinition;
    private final List<String> changeShardColumnsOnly;
    private final CreateGlobalIndexPreparedData globalIndexPreparedData;
    private final Pair<String, String> addLocalIndexSql;
    private final Pair<String, String> dropLocalIndexSql;

    private final ExecutionContext executionContext;

    public RepartitionJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                 RepartitionPrepareData repartitionPrepareData,
                                 PhysicalPlanData physicalPlanData,
                                 ExecutionContext executionContext) {
        this.schemaName = globalIndexPreparedData.getSchemaName();
        this.primaryTableName = globalIndexPreparedData.getPrimaryTableName();
        this.indexTableName = globalIndexPreparedData.getIndexTableName();
        this.isSingle = globalIndexPreparedData.isSingle();
        this.isBroadcast = globalIndexPreparedData.isBroadcast();
        this.globalIndexPreparedData = globalIndexPreparedData;
        this.backfillIndexs = repartitionPrepareData.getBackFilledIndexes();
        this.dropIndexes = repartitionPrepareData.getDroppedIndexes();
        this.primaryTableDefinition = repartitionPrepareData.getPrimaryTableDefinition();
        this.changeShardColumnsOnly = repartitionPrepareData.getChangeShardColumnsOnly();
        this.addLocalIndexSql = repartitionPrepareData.getAddLocalIndexSql();
        this.dropLocalIndexSql = repartitionPrepareData.getDropLocalIndexSql();
        this.physicalPlanData = physicalPlanData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        com.alibaba.polardbx.common.utils.Pair<Boolean, Boolean> result = FactoryUtils.checkDefaultTableGroup(
            schemaName,
            physicalPlanData.getPartitionInfo(),
            physicalPlanData,
            globalIndexPreparedData.getTableGroupName() == null
        );
        boolean checkSingleTgNotExists = result.getKey();
        boolean checkBroadcastTgNotExists = result.getValue();

        if (physicalPlanData.getTableGroupConfig() != null) {
            TableGroupRecord tableGroupRecord = physicalPlanData.getTableGroupConfig().getTableGroupRecord();
            if (tableGroupRecord != null && (tableGroupRecord.id == null
                || tableGroupRecord.id == TableGroupRecord.INVALID_TABLE_GROUP_ID)
                && tableGroupRecord.getTg_type() == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager()
                    .getTableGroupConfigByName(TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE);
                if (tableGroupConfig != null) {
                    tableGroupRecord.setTg_type(TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG);
                }
            }
        }
        ExecutableDdlJob repartitionJob = new ExecutableDdlJob();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(primaryTableName, globalIndexPreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        //validate
        AlterTableRepartitionValidateTask validateTask =
            new AlterTableRepartitionValidateTask(schemaName, primaryTableName, indexTableName, backfillIndexs,
                dropIndexes, physicalPlanData.getTableGroupConfig(), checkSingleTgNotExists, checkBroadcastTgNotExists);

        // cover partition columns for gsi
        List<ExecutableDdlJob4AlterTable> gsiAddColumnJobs = genGsiAddColumnJobs();

        CreateGsiJobFactory createGsiJobFactory =
            CreateGsiJobFactory.create(globalIndexPreparedData, physicalPlanData, executionContext);
        createGsiJobFactory.stayAtBackFill = true;
        ExecutableDdlJob createGsiJob = createGsiJobFactory.create();

        RepartitionCutOverTask cutOverTask =
            new RepartitionCutOverTask(schemaName, primaryTableName, indexTableName, isSingle, isBroadcast, false);
        RepartitionSyncTask repartitionSyncTask = new RepartitionSyncTask(schemaName, primaryTableName, indexTableName);

        DdlTask cdcDdlMarkTask = new CdcRepartitionMarkTask(schemaName, primaryTableName, SqlKind.ALTER_TABLE);

        DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
            new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexTableName, false);
        dropGlobalIndexPreparedData.setRepartition(true);
        dropGlobalIndexPreparedData.setRepartitionTableName(primaryTableName);
        ExecutableDdlJob dropGsiJob =
            DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, false);
        //rollback is not supported after CutOver
        dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        // 1. partitionInfo of gsi is same as new partitionInfo, the gsi need to be dropped
        // 2. table with gsi will repartition to single table or broadcast table, all gsi need to be dropped
        List<ExecutableDdlJob> dropGlobalIndexJobs = genDropGsiJobs();

        // reload table group
        DdlTask syncTableGroup = genSyncTableGroupTask();

        // 0.repartition validate task
        repartitionJob.addTask(validateTableVersionTask);
        repartitionJob.addTask(validateTask);
        repartitionJob.addTaskRelationship(validateTableVersionTask, validateTask);

        // 1.gsi add column
        gsiAddColumnJobs.forEach(repartitionJob::appendJob2);

        boolean skipCheck = executionContext.getParamManager().getBoolean(ConnectionParams.REPARTITION_SKIP_CHECK);

        // only optimize for key partition
        // do not change topology, only change table meta
        if (!skipCheck && changeShardColumnsOnly != null && !changeShardColumnsOnly.isEmpty()) {
            // add local index subJob
            SubJobTask addIndexSubJobTask = null;
            if (addLocalIndexSql != null && addLocalIndexSql.getKey() != null && addLocalIndexSql.getValue() != null) {
                addIndexSubJobTask =
                    new SubJobTask(schemaName, addLocalIndexSql.getKey(), addLocalIndexSql.getValue());
                addIndexSubJobTask.setParentAcquireResource(true);
            }

            // add local index subJob
            SubJobTask dropIndexSubJobTask = null;
            if (dropLocalIndexSql != null && dropLocalIndexSql.getKey() != null
                && dropLocalIndexSql.getValue() != null) {
                dropIndexSubJobTask =
                    new SubJobTask(schemaName, dropLocalIndexSql.getKey(), dropLocalIndexSql.getValue());
                dropIndexSubJobTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
                dropIndexSubJobTask.setParentAcquireResource(true);
            }

            // change meta
            RepartitionChangeMetaTask repartitionChangeMetaTask = new RepartitionChangeMetaTask(
                schemaName, primaryTableName, changeShardColumnsOnly);
            TableSyncTask tableSyncTask = new TableSyncTask(schemaName, primaryTableName);

            // add tasks
            BaseDdlTask lastTask = gsiAddColumnJobs.isEmpty() ? validateTask :
                gsiAddColumnJobs.get(gsiAddColumnJobs.size() - 1).getTableSyncTask();

            if (addIndexSubJobTask != null) {
                repartitionJob.addTaskRelationship(lastTask, addIndexSubJobTask);
                repartitionJob.addTaskRelationship(addIndexSubJobTask, repartitionChangeMetaTask);
            } else {
                repartitionJob.addTaskRelationship(lastTask, repartitionChangeMetaTask);
            }
            repartitionJob.addTaskRelationship(repartitionChangeMetaTask, tableSyncTask);
            repartitionJob.addTaskRelationship(tableSyncTask, cdcDdlMarkTask);

            if (dropIndexSubJobTask != null) {
                repartitionJob.addTaskRelationship(cdcDdlMarkTask, dropIndexSubJobTask);
            }

            return repartitionJob;
        }

        // 2. create gsi
        repartitionJob.combineTasks(createGsiJob);
        if (!gsiAddColumnJobs.isEmpty()) {
            repartitionJob.addTaskRelationship(
                gsiAddColumnJobs.get(gsiAddColumnJobs.size() - 1).getTableSyncTask(),
                getCreateGsiHeadTask(createGsiJob));
        } else {
            repartitionJob.addTaskRelationship(validateTask, getCreateGsiHeadTask(createGsiJob));
        }

        // 3. cut over
        final boolean skipCutOver = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER), "true");
        if (!skipCutOver) {
            repartitionJob.addTaskRelationship(getCreateGsiLastTask(createGsiJob), cutOverTask);
            repartitionJob.addTaskRelationship(cutOverTask, repartitionSyncTask);
            repartitionJob.addTaskRelationship(repartitionSyncTask, cdcDdlMarkTask);
        } else {
            repartitionJob.addTaskRelationship(getCreateGsiLastTask(createGsiJob), cdcDdlMarkTask);
        }

        // 4. drop gsi table which is old primary table
        final boolean skipCleanUp = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CLEANUP), "true");
        if (!skipCleanUp) {
            repartitionJob.combineTasks(dropGsiJob);
            repartitionJob.addTaskRelationship(cdcDdlMarkTask, getDropGsiHeadTask(dropGsiJob));
        }

        // 5. drop gsi tables
        dropGlobalIndexJobs.forEach(repartitionJob::appendJob2);

        // 6. sync table group
        if (syncTableGroup != null) {
            if (!dropGlobalIndexJobs.isEmpty()) {
                repartitionJob.addTaskRelationship(
                    getDropGsiLastTask(dropGlobalIndexJobs.get(dropGlobalIndexJobs.size() - 1)), syncTableGroup);
            } else {
                repartitionJob.addTaskRelationship(getDropGsiLastTask(dropGsiJob), syncTableGroup);
            }
        }
        return repartitionJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));

        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, primaryTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }

        boolean isSigleTable = false;
        boolean isBroadCastTable = false;
        if (physicalPlanData.getPartitionInfo() != null) {
            isSigleTable = physicalPlanData.getPartitionInfo().isGsiSingleOrSingleTable();
            isBroadCastTable = physicalPlanData.getPartitionInfo().isGsiBroadcastOrBroadcast();
        }

        if (globalIndexPreparedData.getTableGroupName() == null) {
            if (isSigleTable) {
                resources.add(concatWithDot(schemaName, TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE));
            } else if (isBroadCastTable) {
                resources.add(concatWithDot(schemaName, TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE));
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    private List<ExecutableDdlJob4AlterTable> genGsiAddColumnJobs() {
        List<ExecutableDdlJob4AlterTable> gsiAddColumnJobs = new ArrayList<>();
        for (Map.Entry<String, List<String>> backfillColumns : backfillIndexs.entrySet()) {
            gsiAddColumnJobs.add((ExecutableDdlJob4AlterTable) GsiTaskFactory.alterGlobalIndexAddColumnFactory(
                schemaName,
                primaryTableName,
                primaryTableDefinition,
                backfillColumns.getKey(),
                backfillColumns.getValue(),
                executionContext).create());
        }
        return gsiAddColumnJobs;
    }

    private List<ExecutableDdlJob> genDropGsiJobs() {
        List<ExecutableDdlJob> dropGlobalIndexJobs = new ArrayList<>();
        for (String indexName : dropIndexes) {
            DropGlobalIndexPreparedData dropGsiPreparedData =
                new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexName, false);
            ExecutableDdlJob dropGlobalIndexJob =
                DropGsiJobFactory.create(dropGsiPreparedData, executionContext, false, false);
            dropGlobalIndexJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
            dropGlobalIndexJobs.add(dropGlobalIndexJob);
        }
        return dropGlobalIndexJobs;
    }

    private DdlTask genSyncTableGroupTask() {
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            PartitionInfo partitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(primaryTableName);
            if (partitionInfo.getTableGroupId() != -1) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig tableGroupConfig =
                    oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
                return new TableGroupSyncTask(schemaName, tableGroupConfig.getTableGroupRecord().getTg_name());
            }
        }
        return null;
    }

    private DdlTask getCreateGsiLastTask(ExecutableDdlJob createGsiJob) {
        DdlTask createGsiLastTask = null;
        if (createGsiJob instanceof ExecutableDdlJob4CreatePartitionGsi) {
            createGsiLastTask = ((ExecutableDdlJob4CreatePartitionGsi) createGsiJob).getLastTask();
        } else if (createGsiJob instanceof ExecutableDdlJob4CreateGsi) {
            createGsiLastTask = ((ExecutableDdlJob4CreateGsi) createGsiJob).getLastTask();
        }

        return createGsiLastTask;
    }

    private DdlTask getCreateGsiHeadTask(ExecutableDdlJob createGsiJob) {
        DdlTask createGsiHeadTask = null;
        if (createGsiJob instanceof ExecutableDdlJob4CreatePartitionGsi) {
            createGsiHeadTask = ((ExecutableDdlJob4CreatePartitionGsi) createGsiJob).getCreateGsiValidateTask();
        } else if (createGsiJob instanceof ExecutableDdlJob4CreateGsi) {
            createGsiHeadTask = ((ExecutableDdlJob4CreateGsi) createGsiJob).getCreateGsiValidateTask();
        }

        return createGsiHeadTask;
    }

    private DdlTask getDropGsiLastTask(ExecutableDdlJob dropGsiJob) {
        DdlTask dropGsiLastTask = null;
        if (dropGsiJob instanceof ExecutableDdlJob4DropGsi) {
            dropGsiLastTask = ((ExecutableDdlJob4DropGsi) dropGsiJob).getFinalSyncTask();
        } else if (dropGsiJob instanceof ExecutableDdlJob4DropPartitionGsi) {
            dropGsiLastTask = ((ExecutableDdlJob4DropPartitionGsi) dropGsiJob).getFinalSyncTask();
        }

        return dropGsiLastTask;
    }

    private DdlTask getDropGsiHeadTask(ExecutableDdlJob dropGsiJob) {
        DdlTask dropGsiLastTask = null;
        if (dropGsiJob instanceof ExecutableDdlJob4DropGsi) {
            dropGsiLastTask = ((ExecutableDdlJob4DropGsi) dropGsiJob).getValidateTask();
        } else if (dropGsiJob instanceof ExecutableDdlJob4DropPartitionGsi) {
            dropGsiLastTask = ((ExecutableDdlJob4DropPartitionGsi) dropGsiJob).getValidateTask();
        }

        return dropGsiLastTask;
    }

}
