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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMergeTableGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.CleanupEmptyTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.JoinGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.MergeTableGroupChangeTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MergeTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MergeTableGroupJobFactory extends DdlJobFactory {

    protected final MergeTableGroupPreparedData preparedData;
    protected final ExecutionContext executionContext;
    private static String MOVE_PARTITION_SQL = "alter tablegroup %s move partitions %s";
    private static String MOVE_PARTITIONS = "(%s) to %s";

    public MergeTableGroupJobFactory(MergeTableGroupPreparedData preparedData,
                                     ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        String schemaName = preparedData.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to execute alter joingroup for non-partitioning databases");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        return toDdlJob();
    }

    public static ExecutableDdlJob create(MergeTableGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new MergeTableGroupJobFactory(preparedData, executionContext).create();
    }

    private SubJobTask generateMovePartitionJob(TableGroupConfig sourceTableGroupConfig,
                                                Map<String, String> targetLocations) {
        String tableGroupName = sourceTableGroupConfig.getTableGroupRecord().getTg_name();
        sourceTableGroupConfig.getPartitionGroupRecords();
        Map<String, Set<String>> moveActions = new TreeMap<>(String::compareToIgnoreCase);

        boolean needMove = false;
        for (PartitionGroupRecord record : sourceTableGroupConfig.getPartitionGroupRecords()) {
            String targetDb = targetLocations.get(record.partition_name);
            String targetInst = preparedData.getDbInstMap().get(targetDb);
            if (StringUtils.isEmpty(targetInst)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the storage inst for [] " + targetDb);
            }
            if (!record.getPhy_db().equalsIgnoreCase(targetDb)) {
                needMove = true;
                moveActions.computeIfAbsent(targetInst, o -> new TreeSet<>(String::compareToIgnoreCase))
                    .add(record.getPartition_name());
            }
        }
        if (!needMove) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, Set<String>> entry : moveActions.entrySet()) {
            if (i > 0) {
                sb.append(", ");
            }
            if (GeneralUtil.isEmpty(entry.getValue())) {
                continue;
            }
            String dn = "'" + entry.getKey() + "'";
            sb.append(String.format(MOVE_PARTITIONS, String.join(",", entry.getValue()), dn));
            i++;
        }
        String sql = String.format(MOVE_PARTITION_SQL,
            tableGroupName,
            sb.toString());
        SubJobTask subJobTask = new SubJobTask(preparedData.getSchemaName(), sql, null);

        return subJobTask;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetTableGroupName()));
        for (String tableGroup : preparedData.getSourceTableGroups()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), tableGroup));
        }
        for (Map.Entry<String, Map<String, Long>> entry : preparedData.getTablesVersion().entrySet()) {
            for (Map.Entry<String, Long> tableEntry : entry.getValue().entrySet()) {
                resources.add(concatWithDot(preparedData.getSchemaName(), tableEntry.getKey()));
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public ExecutableDdlJob toDdlJob() {
        String targetTableGroup = preparedData.getTargetTableGroupName();
        TableGroupConfig targetTableGroupConfig = preparedData.getTableGroupConfigMap().get(targetTableGroup);
        Map<String, String> targetLocations = new TreeMap<>(String::compareToIgnoreCase);
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        for (PartitionGroupRecord record : targetTableGroupConfig.getPartitionGroupRecords()) {
            targetLocations.put(record.partition_name, record.phy_db);
        }

        ExecutableDdlJob job = new ExecutableDdlJob();
        job.setMaxParallelism(executionContext.getParamManager().getInt(ConnectionParams.REBALANCE_TASK_PARALISM));
        EmptyTask headTask = new EmptyTask(preparedData.getSchemaName());
        EmptyTask midTask = new EmptyTask(preparedData.getSchemaName());
        EmptyTask tailTask = new EmptyTask(preparedData.getSchemaName());
        job.addTask(headTask);
        job.addTask(midTask);
        job.addTask(tailTask);
        AlterTableGroupValidateTask targetTableGroupValidateTask = new AlterTableGroupValidateTask(
            preparedData.getSchemaName(), targetTableGroup, preparedData.getTablesVersion().get(targetTableGroup),
            false, preparedData.getPhysicalGroups(), false);
        job.addTask(targetTableGroupValidateTask);
        job.addTaskRelationship(headTask, targetTableGroupValidateTask);
        job.addTaskRelationship(targetTableGroupValidateTask, midTask);

        List<String> tableGroups = new ArrayList<>();
        tableGroups.add(targetTableGroup);
        tableGroups.addAll(preparedData.getSourceTableGroups());
        JoinGroupValidateTask joinGroupValidateTask =
            new JoinGroupValidateTask(preparedData.getSchemaName(), tableGroups, null, true);
        job.addTask(joinGroupValidateTask);
        job.addTaskRelationship(headTask, joinGroupValidateTask);
        job.addTaskRelationship(joinGroupValidateTask, midTask);

        for (String sourceTableGroup : preparedData.getSourceTableGroups()) {
            AlterTableGroupValidateTask validateTask =
                new AlterTableGroupValidateTask(preparedData.getSchemaName(), sourceTableGroup,
                    preparedData.getTablesVersion()
                        .get(sourceTableGroup), true, null, false);
            job.addTask(validateTask);
            job.addTaskRelationship(headTask, validateTask);
            job.addTaskRelationship(validateTask, midTask);
        }

        Map<String, Pair<DdlTask, Set<String>>> tableGroupTaskInfo = new TreeMap<>(String::compareToIgnoreCase);
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
        Set<String> primaryTables = new TreeSet<>(String::compareToIgnoreCase);

        MergeTableGroupChangeTablesMetaTask changeTablesMetaTask =
            new MergeTableGroupChangeTablesMetaTask(preparedData.getSchemaName(), targetTableGroup,
                preparedData.getSourceTableGroups(), primaryTables);

        job.addTask(changeTablesMetaTask);
        for (String tableGroup : preparedData.getSourceTableGroups()) {
            TableGroupConfig tableGroupConfig = preparedData.getTableGroupConfigMap().get(tableGroup);
            SubJobTask subJobTask = generateMovePartitionJob(tableGroupConfig, targetLocations);
            if (subJobTask != null) {
                subJobTask.setParentAcquireResource(true);
            }
            EmptyTask emptyTask = new EmptyTask(preparedData.getSchemaName());
            DdlTask subJobOrEmptyTask = subJobTask == null ? emptyTask : subJobTask;
            job.addTask(subJobOrEmptyTask);
            job.addTaskRelationship(midTask, subJobOrEmptyTask);
            job.addTaskRelationship(subJobOrEmptyTask, changeTablesMetaTask);
            for (Map.Entry<String, Pair<DdlTask, Set<String>>> entry : tableGroupTaskInfo.entrySet()) {
                Set<String> relatedGroups = entry.getValue().getValue();
                if (relatedGroups.contains(tableGroup)) {
                    job.addTaskRelationship(subJobOrEmptyTask, entry.getValue().getKey());
                    if (!job.isValid()) {
                        job.removeTaskRelationship(subJobOrEmptyTask, entry.getValue().getKey());
                    }
                }
            }
            if (!tableGroupTaskInfo.containsKey(tableGroup)) {
                Set<String> relatedTableGroup = getRelatedTableGroupNames(tableGroup, tableGroupInfoManager);
                tableGroupTaskInfo.put(tableGroup, Pair.of(subJobOrEmptyTask, relatedTableGroup));
            }
            primaryTables.addAll(preparedData.getTablesVersion().get(tableGroup).keySet());
        }

        TablesSyncTask tablesSyncTask = new TablesSyncTask(preparedData.getSchemaName(), primaryTables.stream().collect(
            Collectors.toList()), true, initWait, interval, TimeUnit.MILLISECONDS);
        job.addTask(tablesSyncTask);
        for (String tableGroup : preparedData.getSourceTableGroups()) {
            CleanupEmptyTableGroupTask cleanupEmptyTableGroupTask =
                new CleanupEmptyTableGroupTask(preparedData.getSchemaName(), tableGroup);
            job.addTask(cleanupEmptyTableGroupTask);
            job.addTaskRelationship(changeTablesMetaTask, cleanupEmptyTableGroupTask);
            job.addTaskRelationship(cleanupEmptyTableGroupTask, tablesSyncTask);
        }

        CdcMergeTableGroupMarkTask cdcMergeTableGroupMarkTask = new CdcMergeTableGroupMarkTask(
            preparedData.getSchemaName(), targetTableGroup);

        List<DdlTask> tableGroupSyncTasks = new ArrayList<>();
        TableGroupSyncTask targetTableGroupSyncTask =
            new TableGroupSyncTask(preparedData.getSchemaName(), targetTableGroup);
        tableGroupSyncTasks.add(targetTableGroupSyncTask);
        for (String tableGroup : preparedData.getSourceTableGroups()) {
            TableGroupSyncTask tableGroupSyncTask =
                new TableGroupSyncTask(preparedData.getSchemaName(), tableGroup);
            tableGroupSyncTasks.add(tableGroupSyncTask);
        }
        for (DdlTask tableGroupSyncTask : tableGroupSyncTasks) {
            job.addTask(tableGroupSyncTask);
            job.addTaskRelationship(tablesSyncTask, tableGroupSyncTask);
            job.addTaskRelationship(tableGroupSyncTask, cdcMergeTableGroupMarkTask);
        }

        job.labelAsHead(headTask);
        job.labelAsTail(tailTask);
        return job;
    }

    private Set<String> getRelatedTableGroupNames(String tableGroup, TableGroupInfoManager tableGroupInfoManager) {
        Set<String> tableGroups = new TreeSet<>(String::compareToIgnoreCase);
        tableGroups.add(tableGroup);
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
        if (tableGroupConfig != null) {
            for (String tableName : GeneralUtil.emptyIfNull(tableGroupConfig.getAllTables())) {
                TableMeta tableMeta =
                    executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(tableName);
                if (tableMeta.isGsi()) {
                    String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    tableMeta = OptimizerContext.getContext(preparedData.getSchemaName()).getLatestSchemaManager()
                        .getTable(primaryTableName);
                    TableGroupConfig curTableConfig =
                        tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());
                    if (curTableConfig != null) {
                        tableGroups.add(curTableConfig.getTableGroupRecord().getTg_name());
                    }
                }
            }
        }
        return tableGroups;
    }
}
