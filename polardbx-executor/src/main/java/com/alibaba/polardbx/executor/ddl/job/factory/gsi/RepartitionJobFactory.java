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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RepartitionChangeForeignKeyMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RepartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
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
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility.Private;
import static com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility.Protected;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLE_SET_TABLEGROUP;

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
    private final List<ForeignKeyData> modifyForeignKeys;
    private final List<Pair<String, String>> addForeignKeySql;
    private final List<Pair<String, String>> dropForeignKeySql;
    private final Map<String, Set<String>> foreignKeyChildTable;
    private final List<Pair<String, String>> addCciSql;
    private final List<Pair<String, String>> dropCciSql;

    private final ExecutionContext executionContext;

    private final RelOptCluster cluster;

    private final Boolean modifyLocality;
    private final Boolean repartitionGsi;

    public RepartitionJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                 RepartitionPrepareData repartitionPrepareData,
                                 PhysicalPlanData physicalPlanData,
                                 ExecutionContext executionContext,
                                 RelOptCluster cluster) {
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
        this.modifyForeignKeys = repartitionPrepareData.getModifyForeignKeys();
        this.addForeignKeySql = repartitionPrepareData.getAddForeignKeySql();
        this.dropForeignKeySql = repartitionPrepareData.getDropForeignKeySql();
        this.foreignKeyChildTable = repartitionPrepareData.getForeignKeyChildTable();
        this.modifyLocality = repartitionPrepareData.getModifyLocality();
        this.repartitionGsi = repartitionPrepareData.getRepartitionGsi();
        this.addCciSql = repartitionPrepareData.getAddCciSql();
        this.dropCciSql = repartitionPrepareData.getDropCciSql();
        this.physicalPlanData = physicalPlanData;
        this.executionContext = executionContext;
        this.cluster = cluster;
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

        globalIndexPreparedData.setRepartition(true);
        CreateGsiJobFactory createGsiJobFactory =
            CreateGsiJobFactory.create(globalIndexPreparedData, physicalPlanData, executionContext);
        createGsiJobFactory.stayAtBackFill = true;
        ExecutableDdlJob createGsiJob = createGsiJobFactory.create();
        if (globalIndexPreparedData.getRelatedTableGroupInfo().values().stream().anyMatch(o -> o.booleanValue())
            || globalIndexPreparedData.isNeedToGetTableGroupLock()) {
            return createGsiJob;
        }

        RepartitionCutOverTask cutOverTask =
            new RepartitionCutOverTask(schemaName, primaryTableName, indexTableName, isSingle, isBroadcast, false,
                repartitionGsi != null && repartitionGsi);
        RepartitionSyncTask repartitionSyncTask = new RepartitionSyncTask(schemaName, primaryTableName, indexTableName);

        DdlTask cdcDdlMarkTask = null;
        if (executionContext.getDdlContext().isSubJob()) {
            DdlContext rootDdlContext = getRootParentDdlContext(executionContext.getDdlContext());
            DdlType rootDdlType = rootDdlContext.getDdlType();
            if (ALTER_TABLE_SET_TABLEGROUP != rootDdlType && ALTER_TABLE != rootDdlType
                && ALTER_TABLEGROUP != rootDdlType) {
                throw new RuntimeException("unexpected parent ddl job " + rootDdlContext.getDdlType());
            }

            CdcDdlMarkVisibility visibility = rootDdlType == ALTER_TABLE ? Protected : Private;
            cdcDdlMarkTask = new CdcRepartitionMarkTask(schemaName, primaryTableName, SqlKind.ALTER_TABLE, visibility);
        } else {
            cdcDdlMarkTask = new CdcRepartitionMarkTask(
                schemaName, primaryTableName, SqlKind.ALTER_TABLE, Protected);
        }

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
        for (ExecutableDdlJob4AlterTable gsiAddColumnJob : gsiAddColumnJobs) {
            repartitionJob.combineTasks(gsiAddColumnJob);
            repartitionJob.addTaskRelationship(validateTask, gsiAddColumnJob.getTableValidateTask());
        }

        boolean skipCheck = executionContext.getParamManager().getBoolean(ConnectionParams.REPARTITION_SKIP_CHECK);

        // only optimize for key partition
        // do not change topology, only change table meta
        if (!skipCheck && changeShardColumnsOnly != null && !changeShardColumnsOnly.isEmpty() && modifyLocality != null
            && !modifyLocality) {
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
            if (addIndexSubJobTask != null) {
                repartitionJob.appendTask(addIndexSubJobTask);
                repartitionJob.addTaskRelationship(addIndexSubJobTask, repartitionChangeMetaTask);
            } else {
                repartitionJob.appendTask(repartitionChangeMetaTask);
            }
            repartitionJob.addTaskRelationship(repartitionChangeMetaTask, tableSyncTask);
            repartitionJob.addTaskRelationship(tableSyncTask, cdcDdlMarkTask);

            if (dropIndexSubJobTask != null) {
                repartitionJob.addTaskRelationship(cdcDdlMarkTask, dropIndexSubJobTask);
            }

            return repartitionJob;
        }

        if (GeneralUtil.isNotEmpty(dropCciSql)) {
            List<SubJobTask> dropCciSubJobTasks = new ArrayList<>();
            for (Pair<String, String> sql : dropCciSql) {
                SubJobTask dropCciSubJobTask =
                    new SubJobTask(schemaName, sql.getKey(), sql.getValue());
                dropCciSubJobTask.setParentAcquireResource(true);
                dropCciSubJobTasks.add(dropCciSubJobTask);
            }

            for (int i = 0; i < dropCciSubJobTasks.size(); i++) {
                if (i == 0) {
                    repartitionJob.addTaskRelationship(validateTask, dropCciSubJobTasks.get(i));
                } else {
                    repartitionJob.addTaskRelationship(dropCciSubJobTasks.get(i - 1), dropCciSubJobTasks.get(i));
                }
            }
        }

        // 2. drop foreign keys on child table
        if (dropForeignKeySql != null && !dropForeignKeySql.isEmpty()) {
            // drop foreign key subJob
            List<SubJobTask> dropFkSubJobTasks = new ArrayList<>();
            for (Pair<String, String> sql : dropForeignKeySql) {
                SubJobTask dropFkSubJobTask =
                    new SubJobTask(schemaName, sql.getKey(), sql.getValue());
                dropFkSubJobTask.setParentAcquireResource(true);
                dropFkSubJobTasks.add(dropFkSubJobTask);
            }

            for (int i = 0; i < dropFkSubJobTasks.size(); i++) {
                if (i == 0) {
                    repartitionJob.addTaskRelationship(validateTask, dropFkSubJobTasks.get(i));
                } else {
                    repartitionJob.addTaskRelationship(dropFkSubJobTasks.get(i - 1), dropFkSubJobTasks.get(i));
                }
            }
        }

        // 3. create gsi
        repartitionJob.appendJob2(createGsiJob);

        // drop cci
//        dropColumnarClusterIndexJobs.forEach(repartitionJob::appendJob2);

        // 4. cut over
        final boolean skipCutOver = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER), "true");
        if (!skipCutOver) {
            repartitionJob.addTaskRelationship(getCreateGsiLastTask(createGsiJob), cutOverTask);
            repartitionJob.addTaskRelationship(cutOverTask, repartitionSyncTask);
            repartitionJob.addTaskRelationship(repartitionSyncTask, cdcDdlMarkTask);
        } else {
            repartitionJob.addTaskRelationship(getCreateGsiLastTask(createGsiJob), cdcDdlMarkTask);
        }

        // 5. drop gsi table which is old primary table
        final boolean skipCleanUp = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CLEANUP), "true");
        if (!skipCleanUp) {
            repartitionJob.combineTasks(dropGsiJob);
            repartitionJob.addTaskRelationship(cdcDdlMarkTask, getDropGsiHeadTask(dropGsiJob));
        }

        // 6. drop gsi tables
        dropGlobalIndexJobs.forEach(repartitionJob::appendJob2);

        // create cci
//        repartitionJob.appendJob2(createCciJob);

        if (GeneralUtil.isNotEmpty(addCciSql)) {
            List<SubJobTask> addCciSubJobTasks = new ArrayList<>();
            for (Pair<String, String> sql : addCciSql) {
                SubJobTask addCciSubJobTask =
                    new SubJobTask(schemaName, sql.getKey(), sql.getValue());
                addCciSubJobTask.setParentAcquireResource(true);
                addCciSubJobTasks.add(addCciSubJobTask);
            }

            for (int i = 0; i < addCciSubJobTasks.size(); i++) {
                if (i == 0) {
                    repartitionJob.appendTask(addCciSubJobTasks.get(i));
                } else {
                    repartitionJob.addTaskRelationship(addCciSubJobTasks.get(i - 1), addCciSubJobTasks.get(i));
                }
            }
        }

        // 7. drop/create fk on related table
        if (addForeignKeySql != null && !addForeignKeySql.isEmpty()) {
            // change fk meta
            RepartitionChangeForeignKeyMetaTask repartitionChangeFkMetaTask = new RepartitionChangeForeignKeyMetaTask(
                schemaName, primaryTableName, modifyForeignKeys);

            repartitionJob.appendTask(repartitionChangeFkMetaTask);

            for (ForeignKeyData fk : modifyForeignKeys) {
                repartitionJob.appendTask(new TableSyncTask(fk.refSchema, fk.refTableName));
            }
            TableSyncTask syncTask = new TableSyncTask(schemaName, primaryTableName);
            repartitionJob.appendTask(syncTask);

            // add foreign key subJob
            List<SubJobTask> addFkSubJobTasks = new ArrayList<>();
            for (Pair<String, String> sql : addForeignKeySql) {
                SubJobTask addFkSubJobTask =
                    new SubJobTask(schemaName, sql.getKey(), sql.getValue());
                addFkSubJobTask.setParentAcquireResource(true);
                addFkSubJobTasks.add(addFkSubJobTask);
            }

            for (int i = 0; i < addFkSubJobTasks.size(); i++) {
                if (i == 0) {
                    repartitionJob.addTaskRelationship(syncTask, addFkSubJobTasks.get(i));
                } else {
                    repartitionJob.addTaskRelationship(addFkSubJobTasks.get(i - 1), addFkSubJobTasks.get(i));
                }
            }
        }

        // 8. sync table group
        if (syncTableGroup != null) {
            repartitionJob.appendTask(syncTableGroup);
        }
        repartitionJob.labelAsHead(validateTableVersionTask);
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
        if (backfillIndexs == null) {
            return gsiAddColumnJobs;
        }
        for (Map.Entry<String, List<String>> backfillColumns : backfillIndexs.entrySet()) {
            gsiAddColumnJobs.add((ExecutableDdlJob4AlterTable) GsiTaskFactory.alterGlobalIndexAddColumnFactory(
                schemaName,
                primaryTableName,
                primaryTableDefinition,
                backfillColumns.getKey(),
                backfillColumns.getValue(),
                executionContext,
                cluster).create());
        }
        return gsiAddColumnJobs;
    }

    private List<ExecutableDdlJob> genDropGsiJobs() {
        List<ExecutableDdlJob> dropGlobalIndexJobs = new ArrayList<>();
        if (dropIndexes == null) {
            return dropGlobalIndexJobs;
        }
        for (String indexName : dropIndexes) {
            DropGlobalIndexPreparedData dropGsiPreparedData =
                new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexName, false);
            dropGsiPreparedData.setRepartition(true);
            dropGsiPreparedData.setRepartitionTableName(indexName);
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

    private DdlContext getRootParentDdlContext(DdlContext ddlContext) {
        if (ddlContext.getParentDdlContext() != null) {
            return getRootParentDdlContext(ddlContext.getParentDdlContext());
        } else {
            return ddlContext;
        }
    }
}
