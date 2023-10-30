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
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcModifyPartitionKeyMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ModifyPartitionKeyCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ModifyPartitionKeySyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ModifyPartitionKeyValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;

/**
 * @author wumu
 */
public class ModifyPartitionKeyJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String primaryTableName;
    private final Map<String, String> tableNameMap;
    private final Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData;
    private final ExecutionContext executionContext;
    private List<String> alterDefaultColumns;

    private boolean needDropImplicitKey;

    private final Map<String, String> virtualColumnMap;
    private final Map<String, String> columnNewDef;
    private final PhysicalPlanData oldPhysicalPlanData;

    public ModifyPartitionKeyJobFactory(String schemaName, String primaryTableName, Map<String, String> tableNameMap,
                                        Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData,
                                        Map<String, String> virtualColumnMap, Map<String, String> columnNewDef,
                                        PhysicalPlanData oldPhysicalPlanData, ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.tableNameMap = tableNameMap;
        this.globalIndexPrepareData = globalIndexPrepareData;
        this.executionContext = executionContext;
        this.needDropImplicitKey = false;
        this.alterDefaultColumns = null;
        this.virtualColumnMap = virtualColumnMap;
        this.columnNewDef = columnNewDef;
        this.oldPhysicalPlanData = oldPhysicalPlanData;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);

        for (String indexTableName : tableNameMap.values()) {
            GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();

        assert !globalIndexPrepareData.isEmpty();

        // alter table partitions validate
        // get old table groups
        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        List<TableGroupConfig> oldTableGroupConfigs =
            FactoryUtils.getTableGroupConfigByTableName(schemaName, new ArrayList<>(tableNameMap.keySet()));
        tableGroupConfigs.addAll(oldTableGroupConfigs);
        // get new table groups
        tableGroupConfigs.addAll(
            globalIndexPrepareData.values().stream()
                .map(PhysicalPlanData::getTableGroupConfig).collect(Collectors.toList())
        );
        DdlTask validateTask =
            new ModifyPartitionKeyValidateTask(schemaName, primaryTableName, tableNameMap, tableGroupConfigs);

        // for modify default column
        DdlTask beginAlterColumnDefault = null;
        DdlTask beginAlterColumnDefaultSyncTask = null;
        if (CollectionUtils.isNotEmpty(alterDefaultColumns)) {
            beginAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, primaryTableName, alterDefaultColumns, true);
            beginAlterColumnDefaultSyncTask = new TableSyncTask(schemaName, primaryTableName);
        }

        List<DdlTask> checkerTasks = genCheckerTasks();

        // create gsi
        List<ExecutableDdlJob> createGsiJobs = new ArrayList<>();
        globalIndexPrepareData.forEach((createGlobalIndexPreparedData, physicalPlanData) -> {
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

            CreateGsiJobFactory createGsiJobFactory =
                CreateGsiJobFactory.create(createGlobalIndexPreparedData, physicalPlanData, executionContext);
            createGsiJobFactory.stayAtBackFill = true;
            createGsiJobFactory.setVirtualColumnMap(virtualColumnMap);
            createGsiJobs.add(createGsiJobFactory.create());
        });

        TableMeta tableMeta = executionContext.getSchemaManager().getTable(primaryTableName);
        // cut over
        ModifyPartitionKeyCutOverTask cutOverTask =
            new ModifyPartitionKeyCutOverTask(schemaName, primaryTableName, tableNameMap, tableMeta.isAutoPartition(),
                tableMeta.getPartitionInfo().isSingleTable(), tableMeta.getPartitionInfo().isBroadcastTable());
        ModifyPartitionKeySyncTask
            modifyPartitionKeySyncTask = new ModifyPartitionKeySyncTask(schemaName, primaryTableName, tableNameMap);

        // cdc
        DdlTask cdcDdlMarkTask = new CdcModifyPartitionKeyMarkTask(schemaName, primaryTableName, SqlKind.ALTER_TABLE);

        // drop gsi
        List<ExecutableDdlJob> dropGsiJobs = new ArrayList<>();

        for (Map.Entry<String, String> entries : tableNameMap.entrySet()) {
            String newIndexTableName = entries.getValue();
            DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
                new DropGlobalIndexPreparedData(schemaName, primaryTableName, newIndexTableName, false);
            dropGlobalIndexPreparedData.setRepartition(true);
            dropGlobalIndexPreparedData.setRepartitionTableName(entries.getKey());
            ExecutableDdlJob dropGsiJob =
                DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, false);
            // rollback is not supported after CutOver
            dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
            dropGsiJobs.add(dropGsiJob);
        }

        // table groups sync task
        TableGroupsSyncTask tableGroupsSyncTask = new TableGroupsSyncTask(schemaName,
            oldTableGroupConfigs.stream()
                .map(e -> e.getTableGroupRecord().getTg_name())
                .collect(Collectors.toList())
        );

        List<DdlTask> ddlTasks = new ArrayList<>();
        ddlTasks.add(validateTask);
        if (CollectionUtils.isNotEmpty(alterDefaultColumns)) {
            ddlTasks.add(beginAlterColumnDefault);
            ddlTasks.add(beginAlterColumnDefaultSyncTask);
        }

        if (CollectionUtils.isNotEmpty(checkerTasks)) {
            ddlTasks.addAll(checkerTasks);
        }
        ddlJob.addSequentialTasks(ddlTasks);
        createGsiJobs.forEach(ddlJob::appendJob2);

        final boolean skipCutOver = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER), "true");
        if (!skipCutOver) {
            ddlJob.appendTask(cutOverTask);
            ddlJob.addTaskRelationship(cutOverTask, modifyPartitionKeySyncTask);
            ddlJob.addTaskRelationship(modifyPartitionKeySyncTask, cdcDdlMarkTask);
        }

        final boolean skipCleanUp = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CLEANUP), "true");
        if (!skipCleanUp) {
            dropGsiJobs.forEach(ddlJob::appendJob2);

            ddlJob.addTaskRelationship(
                ((ExecutableDdlJob4DropPartitionGsi) dropGsiJobs.get(dropGsiJobs.size() - 1)).getFinalSyncTask(),
                tableGroupsSyncTask);

            if (needDropImplicitKey) {
                SubJobTask dropImplicitKeySubJobTask =
                    new SubJobTask(schemaName,
                        String.format("alter table %s drop column %s", primaryTableName, IMPLICIT_COL_NAME),
                        null);
                dropImplicitKeySubJobTask.setParentAcquireResource(true);

                ddlJob.addTaskRelationship(tableGroupsSyncTask, dropImplicitKeySubJobTask);
            }
        } else {
            if (!skipCutOver) {
                ddlJob.addTaskRelationship(cdcDdlMarkTask, tableGroupsSyncTask);
            } else {
                ddlJob.appendTask(tableGroupsSyncTask);
            }
        }

        ddlJob.labelAsHead(validateTask);
        return ddlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        for (String indexTableName : tableNameMap.values()) {
            resources.add(concatWithDot(schemaName, indexTableName));
        }

        // lock table group of primary table
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(primaryTableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    private List<DdlTask> genCheckerTasks() {
        List<DdlTask> result = new ArrayList<>();

        if (MapUtils.isEmpty(virtualColumnMap) || MapUtils.isEmpty(columnNewDef)) {
            return null;
        }

        String tableNameWithBacktick = String.format("`%s`", primaryTableName);
        virtualColumnMap.forEach((colName, virColName) -> {
            String addSqlFormatter =
                String.format("ALTER TABLE %%s ADD COLUMN `%s` %s GENERATED ALWAYS AS (ALTER_TYPE(`%s`)) VIRTUAL",
                    virColName, columnNewDef.get(colName), colName);
            String dropSqlFormatter = String.format("ALTER TABLE %%s DROP COLUMN `%s`", virColName);
            String addSql = String.format(addSqlFormatter, tableNameWithBacktick);
            String dropSql = String.format(dropSqlFormatter, tableNameWithBacktick);
            String addSqlTemplate = String.format(addSqlFormatter, "?");
            String dropSqlTemplate = String.format(dropSqlFormatter, "?");

            result.add(
                genAlterTablePhyTask(addSql, dropSql, addSqlTemplate, dropSqlTemplate, primaryTableName, "INPLACE"));
        });

        return result;
    }

    private DdlTask genAlterTablePhyTask(String sql, String reverseSql, String sqlTemplate, String reverseSqlTemplate,
                                         String tableName, String algorithm) {
        sql = sql + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSql)) {
            reverseSql = reverseSql + ", ALGORITHM=" + algorithm;
        }

        sqlTemplate = sqlTemplate + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSqlTemplate)) {
            reverseSqlTemplate = reverseSqlTemplate + ", ALGORITHM=" + algorithm;
        }

        PhysicalPlanData newPhysicalPlanData = oldPhysicalPlanData.clone();
        newPhysicalPlanData.setSqlTemplate(sqlTemplate);
        AlterTablePhyDdlTask task;
        task = new AlterTablePhyDdlTask(schemaName, tableName, newPhysicalPlanData);
        task.setSourceSql(sql);
        if (!StringUtils.isEmpty(reverseSql)) {
            task.setRollbackSql(reverseSql);
            task.setRollbackSqlTemplate(reverseSqlTemplate);
        }
        return task;
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    public void setNeedDropImplicitKey(boolean needDropImplicitKey) {
        this.needDropImplicitKey = needDropImplicitKey;
    }

    public void setAlterDefaultColumns(List<String> alterDefaultColumns) {
        this.alterDefaultColumns = alterDefaultColumns;
    }
}
