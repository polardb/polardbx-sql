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

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableRenamePartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupRenamePartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableRenamePartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.CleanupEmptyTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableRenamePartitionPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author luoyanxin
 */
public class AlterTableRenamePartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableRenamePartitionJobFactory(DDL ddl, AlterTableRenamePartitionPreparedData preparedData,
                                               ExecutionContext executionContext) {
        super(ddl, preparedData, null, null, null,
            null, null, null,
            null, executionContext);
    }

    @Override
    protected void validate() {
        TableValidator.validateTableWithCCI(preparedData.getSchemaName(), preparedData.getTableName(), executionContext,
            SqlKind.RENAME_PARTITION);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterTableGroupRenamePartitionPreparedData renamePartitionPreparedData =
            (AlterTableGroupRenamePartitionPreparedData) preparedData;
        if (renamePartitionPreparedData.isRenameNothing()) {
            return new TransientDdlJob();
        }
        if (preparedData.isRemainInOriginalTableGroup()) {
            return renameInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return renameAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return renameInNewTableGroup();
        } else if (org.apache.commons.lang.StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            return withImplicitTableGroup(executionContext);
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    protected ExecutableDdlJob renameAndMoveToExistTableGroup() {
        boolean enablePreemptiveMdl =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        Map<String, Long> tablesVersion = getTablesVersion();

        AlterTableRenamePartitionPreparedData alterTableRenamePartitionPreparedData =
            (AlterTableRenamePartitionPreparedData) preparedData;
        DdlTask changeMetaTask = new AlterTableRenamePartitionChangeMetaTask(preparedData.getSchemaName(),
            preparedData.getTargetTableGroup(), preparedData.getTableName(),
            alterTableRenamePartitionPreparedData.getChangePartitionsPair(),
            alterTableRenamePartitionPreparedData.isSubPartitionRename());
        DdlTask syncTask = new TableSyncTask(preparedData.getSchemaName(), tablesVersion.keySet().iterator().next(),
            enablePreemptiveMdl, initWait, interval,
            TimeUnit.MILLISECONDS);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        String schemaName = preparedData.getSchemaName();
        String sourceTableGroup = preparedData.getTableGroupName();
        String targetTableGroup = preparedData.getTargetTableGroup();
        DdlTask emptyTask = new EmptyTask(schemaName);
        DdlTask validateSourceTableGroup =
            new AlterTableGroupValidateTask(schemaName,
                sourceTableGroup, tablesVersion, false,
                /*todo*/null, false);
        DdlTask validateTargetTableGroup =
            new AlterTableGroupValidateTask(schemaName,
                targetTableGroup, preparedData.getFirstTableVersionInTargetTableGroup(), false,
                preparedData.getTargetPhysicalGroups(), false);

        executableDdlJob.addTask(emptyTask);
        executableDdlJob.addTask(validateSourceTableGroup);
        executableDdlJob.addTask(validateTargetTableGroup);
        executableDdlJob.addTask(changeMetaTask);

        CleanupEmptyTableGroupTask cleanupEmptyTableGroupTask =
            new CleanupEmptyTableGroupTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        BaseDdlTask synTargetTableGroup =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTargetTableGroup());
        BaseDdlTask synSourceTableGroup =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());

        executableDdlJob.addTaskRelationship(emptyTask, validateSourceTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateTargetTableGroup);
        executableDdlJob.addTaskRelationship(validateSourceTableGroup, changeMetaTask);
        executableDdlJob.addTaskRelationship(validateTargetTableGroup, changeMetaTask);

        DdlTask cdcAlterTableRenamePartitionMarkTask = buildCdcDdlMarkTask();

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            cdcAlterTableRenamePartitionMarkTask,
            syncTask,
            cleanupEmptyTableGroupTask,
            synTargetTableGroup,
            synSourceTableGroup
        ));
        executableDdlJob.addTaskRelationship(changeMetaTask, cdcAlterTableRenamePartitionMarkTask);

        return executableDdlJob;
    }

    protected ExecutableDdlJob renameInNewTableGroup() {
        executionContext.getDdlContext().setDdlType(DdlType.ALTER_TABLE_RENAME_PARTITION);
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                preparedData.getTableGroupName(), tablesVersion, false,
                preparedData.getTargetPhysicalGroups(), false);

        SubJobTask subJobMoveTableToNewGroup = new SubJobTask(schemaName,
            String.format(AlterTableGroupBaseJobFactory.SET_NEW_TABLE_GROUP, preparedData.getTableName()), null);
        SubJobTask subJobSplitTable = new SubJobTask(schemaName, preparedData.getSourceSql(), null);
        subJobMoveTableToNewGroup.setParentAcquireResource(true);
        subJobSplitTable.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            subJobMoveTableToNewGroup,
            subJobSplitTable
        ));
        return executableDdlJob;
    }

    protected ExecutableDdlJob renameInOriginTableGroup() {
        boolean enablePreemptiveMdl =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        List<String> logicalTableNames = new ArrayList<>();
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        for (String tableName : tableGroupConfig.getAllTables()) {
            String primaryTableName;
            TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(tableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTableName);
            } else {
                primaryTableName = tableName;
            }
            logicalTableNames.add(primaryTableName);
            tablesVersion.put(primaryTableName, tableMeta.getVersion());
        }

        AlterTableRenamePartitionPreparedData alterTableRenamePartitionPreparedData =
            (AlterTableRenamePartitionPreparedData) preparedData;
        DdlTask changeMetaTask = new AlterTableGroupRenamePartitionChangeMetaTask(preparedData.getSchemaName(),
            preparedData.getTableGroupName(), alterTableRenamePartitionPreparedData.getChangePartitionsPair(),
            alterTableRenamePartitionPreparedData.isSubPartitionRename());
        DdlTask syncTask =
            new TablesSyncTask(preparedData.getSchemaName(), logicalTableNames, enablePreemptiveMdl, initWait, interval,
                TimeUnit.MILLISECONDS);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getTableGroupName(),
                tablesVersion,
                true, null, false);

        DdlTask reloadTableGroup =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());

        DdlTask cdcAlterTableRenamePartitionMarkTask = buildCdcDdlMarkTask();

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            changeMetaTask,
            cdcAlterTableRenamePartitionMarkTask,
            syncTask,
            reloadTableGroup
        ));

        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableRenamePartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterTableRenamePartitionJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()));

        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        if (preparedData.isMoveToExistTableGroup() && StringUtils.isNotBlank(preparedData.getTargetTableGroup())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetTableGroup()));
        }
        if (StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetImplicitTableGroupName()));
        }
        for (String relatedPart : preparedData.getRelatedPartitions()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                relatedPart));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected Map<String, Long> getTablesVersion() {
        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String logicalTable = preparedData.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(logicalTable);
        if (tableMeta.isGsi()) {
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            logicalTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(logicalTable);
        }
        tablesVersion.put(logicalTable, tableMeta.getVersion());
        return tablesVersion;
    }

    private DdlTask buildCdcDdlMarkTask() {
        boolean placeHolder;

        if (executionContext.getDdlContext().isSubJob()) {
            if (isFromSetTableGroup(executionContext) || isFromAlterTableGroup(executionContext)) {
                placeHolder = true;
            } else if (isFromRenamePartition(executionContext)) {
                placeHolder = false;
            } else {
                DdlType parentDdlType = getRootParentDdlContext(executionContext.getDdlContext()).getDdlType();
                throw new RuntimeException("unexpected parent ddl job , " + parentDdlType);
            }
        } else {
            placeHolder = false;
        }

        String logicalTable = preparedData.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(logicalTable);

        return new CdcAlterTableRenamePartitionMarkTask(preparedData.getSchemaName(),
            preparedData.getTableName(), placeHolder, tableMeta.isColumnar(), preparedData.getDdlVersionId());
    }

    private boolean isFromSetTableGroup(ExecutionContext executionContext) {
        DdlType parentDdlType = getRootParentDdlContext(executionContext.getDdlContext()).getDdlType();
        return parentDdlType == DdlType.ALTER_TABLE_SET_TABLEGROUP;
    }

    private boolean isFromAlterTableGroup(ExecutionContext executionContext) {
        DdlType parentDdlType = getRootParentDdlContext(executionContext.getDdlContext()).getDdlType();
        return parentDdlType == DdlType.ALTER_TABLEGROUP;
    }

    private boolean isFromRenamePartition(ExecutionContext executionContext) {
        DdlType parentDdlType = getRootParentDdlContext(executionContext.getDdlContext()).getDdlType();
        return parentDdlType == DdlType.ALTER_TABLE_RENAME_PARTITION;
    }

    private DdlContext getRootParentDdlContext(DdlContext ddlContext) {
        if (ddlContext.getParentDdlContext() != null) {
            return getRootParentDdlContext(ddlContext.getParentDdlContext());
        } else {
            return ddlContext;
        }
    }
}
