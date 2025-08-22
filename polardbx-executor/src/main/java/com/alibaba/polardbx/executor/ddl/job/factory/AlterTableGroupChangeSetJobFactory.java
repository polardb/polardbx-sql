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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.AlterTableGroupBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CloneTableDataFileTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.AlterTableGroupMovePartitionsCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;
import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genChangeSetCatchUpTasks;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genTargetTableLocations;

public class AlterTableGroupChangeSetJobFactory extends AlterTableGroupSubTaskJobFactory {
    private ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask;

    private ChangeSetApplyFinishTask changeSetApplyFinishTask;

    final List<PhyDdlTableOperation> discardTableSpaceOperations;
    final Map<String, org.apache.calcite.util.Pair<String, String>> ptbGroupMap;
    protected boolean usePhysicalBackfill = false;
    protected final AlterTableGroupBasePreparedData parentPrepareData;
    protected List<DdlTask> backfillTaskEdgeNodes = new ArrayList<>(2);
    //item: index[0]:clone task; index[1]:PhysicalBackfilltask; index[2~end]: importtask
    protected List<List<DdlTask>> physicalyTaskPipeLine = new ArrayList<>();

    final Map<String, String> sourceAndTarDnMap;
    final Map<String, Pair<String, String>> storageInstAndUserInfos;

    public AlterTableGroupChangeSetJobFactory(DDL ddl, AlterTableGroupBasePreparedData parentPrepareData,
                                              AlterTableGroupItemPreparedData preparedData,
                                              List<PhyDdlTableOperation> phyDdlTableOperations,
                                              TreeMap<String, List<List<String>>> tableTopology,
                                              Map<String, Set<String>> targetTableTopology,
                                              Map<String, Set<String>> sourceTableTopology,
                                              //List<Pair<String, String>> orderedTargetTableLocations,
                                              Map<String, Pair<String, String>> orderedTargetTableLocations,
                                              String targetPartition, boolean skipBackfill,
                                              ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                              ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                              ComplexTaskMetaManager.ComplexTaskType taskType,
                                              ExecutionContext executionContext) {
        super(ddl,
            parentPrepareData,
            preparedData,
            phyDdlTableOperations,
            tableTopology,
            targetTableTopology,
            sourceTableTopology,
            orderedTargetTableLocations,
            targetPartition,
            skipBackfill,
            taskType,
            executionContext);
        this.parentPrepareData = parentPrepareData;
        this.changeSetApplyExecutorInitTask = changeSetApplyExecutorInitTask;
        this.changeSetApplyFinishTask = changeSetApplyFinishTask;
        this.discardTableSpaceOperations = null;
        this.ptbGroupMap = null;
        this.sourceAndTarDnMap = null;
        this.storageInstAndUserInfos = null;
    }

    public AlterTableGroupChangeSetJobFactory(DDL ddl, AlterTableGroupBasePreparedData parentPrepareData,
                                              AlterTableGroupItemPreparedData preparedData,
                                              List<PhyDdlTableOperation> phyDdlTableOperations,
                                              List<PhyDdlTableOperation> discardTableSpaceOperations,
                                              Map<String, org.apache.calcite.util.Pair<String, String>> ptbGroupMap,
                                              Map<String, String> sourceAndTarDnMap,
                                              Map<String, Pair<String, String>> storageInstAndUserInfos,
                                              TreeMap<String, List<List<String>>> tableTopology,
                                              Map<String, Set<String>> targetTableTopology,
                                              Map<String, Set<String>> sourceTableTopology,
                                              Map<String, Pair<String, String>> orderedTargetTableLocations,
                                              String targetPartition,
                                              boolean skipBackfill,
                                              ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                              ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                              ComplexTaskMetaManager.ComplexTaskType taskType,
                                              ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, taskType, executionContext);
        this.parentPrepareData = parentPrepareData;
        this.changeSetApplyExecutorInitTask = changeSetApplyExecutorInitTask;
        this.changeSetApplyFinishTask = changeSetApplyFinishTask;
        this.discardTableSpaceOperations = discardTableSpaceOperations;
        this.ptbGroupMap = ptbGroupMap;
        this.sourceAndTarDnMap = sourceAndTarDnMap;
        this.storageInstAndUserInfos = storageInstAndUserInfos;
        this.usePhysicalBackfill = parentPrepareData.isUsePhysicalBackfill();
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (skipBackfill) {
            return super.doCreate();
        }

        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        String tableGroupName = preparedData.getTableGroupName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
            PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        //DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName());
        DdlTask addMetaTask =
            new AlterTableGroupAddSubTaskMetaTask(schemaName, tableName,
                tableGroupConfig.getTableGroupRecord().getTg_name(),
                tableGroupConfig.getTableGroupRecord().getId(), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0, logTableRec, partRecList,
                subPartRecInfos);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        //taskList.add(validateTask);
        if (changeSetApplyExecutorInitTask != null) {
            taskList.add(changeSetApplyExecutorInitTask);
        }

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        phyDdlTableOperations.forEach(o -> o.setPartitionInfo(newPartitionInfo));
        List<DdlTask> discardTableSpaceTasks = null;
        if (!tableTopology.isEmpty()) {
            PhysicalPlanData physicalPlanData =
                DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations, executionContext);
            DdlTask phyDdlTask =
                new CreatePhyTableWithRollbackCheckTask(schemaName, physicalPlanData.getLogicalTableName(),
                    physicalPlanData, sourceTableTopology);
            taskList.add(phyDdlTask);
            if (usePhysicalBackfill) {
                discardTableSpaceTasks = ScaleOutUtils.generateDiscardTableSpaceDdlTask(schemaName, tableTopology,
                    discardTableSpaceOperations, null, false, executionContext);
            }
        }

        List<String> relatedTables = new ArrayList<>();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            relatedTables.add(tableName);
        }

        Map<String, String> targetTableLocations = genTargetTableLocations(orderedTargetTableLocations);
        Long changeSetId = ChangeSetManager.getChangeSetId();

        ChangeSetStartTask changeSetStartTask =
            new ChangeSetStartTask(schemaName, tableName, sourceTableTopology, taskType, changeSetId);

        Map<String, ChangeSetCatchUpTask> catchUpTasks = genChangeSetCatchUpTasks(
            schemaName,
            tableName,
            null,
            sourceTableTopology,
            targetTableLocations,
            null,
            taskType,
            changeSetId
        );

        final boolean useApplyOpt = changeSetApplyFinishTask != null
            && executionContext.getParamManager().getBoolean(CHANGE_SET_APPLY_OPTIMIZATION);

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }
        List<DdlTask> movePartitionTasks;
        backfillTaskEdgeNodes.clear();
        physicalyTaskPipeLine.clear();

        final boolean waitLsn = executionContext.getParamManager()
            .getBoolean(ConnectionParams.PHYSICAL_BACKFILL_WAIT_LSN_WHEN_ROLLBACK);

        boolean healthyCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);
        List<String> physicalPartitions = preparedData.getOldPartitionNames();
        long totalDataSize = 0;

        boolean fallbackToLogicalBackfill = true;
        Long tableSizeThreshold = executionContext.getParamManager()
            .getLong(ConnectionParams.TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL);
        Map<String, Long> phyTableSize = new HashMap<>();

        if (usePhysicalBackfill) {
            for (Map.Entry<String, org.apache.calcite.util.Pair<String, String>> entry : ptbGroupMap.entrySet()) {
                String phyTb = entry.getKey();
                org.apache.calcite.util.Pair<String, String> srcTarGroup = entry.getValue();

                Pair<String, String> srcDbAndGroup = Pair.of(
                    GroupInfoUtil.buildPhysicalDbNameFromGroupName(srcTarGroup.getKey()).toLowerCase(),
                    srcTarGroup.getKey());

                long dataSize = PhysicalBackfillUtils.fetchPhysicalTableSize(schemaName,
                    srcDbAndGroup.getValue(),
                    srcDbAndGroup.getKey(),
                    phyTb, sourceAndTarDnMap, storageInstAndUserInfos);

                phyTableSize.put(phyTb.toLowerCase(), dataSize);
                totalDataSize += dataSize;
            }
            fallbackToLogicalBackfill = (totalDataSize / Math.max(ptbGroupMap.size(), 1)) < tableSizeThreshold;
        }
        if (!fallbackToLogicalBackfill) {
            Set<String> sourceStorageInsts = new HashSet<>();
            Set<String> targetStorageInsts = new HashSet<>();
            for (Map.Entry<String, org.apache.calcite.util.Pair<String, String>> entry : ptbGroupMap.entrySet()) {
                String phyTb = entry.getKey();
                org.apache.calcite.util.Pair<String, String> srcTarGroup = entry.getValue();
                String sourceStorageId = sourceAndTarDnMap.computeIfAbsent(srcTarGroup.getKey(),
                    key -> DbTopologyManager.getStorageInstIdByGroupName(schemaName, srcTarGroup.getKey()));
                String targetStorageId = sourceAndTarDnMap.computeIfAbsent(srcTarGroup.getValue(),
                    key -> DbTopologyManager.getStorageInstIdByGroupName(schemaName, srcTarGroup.getValue()));

                Pair<String, String> srcDbAndGroup = Pair.of(
                    GroupInfoUtil.buildPhysicalDbNameFromGroupName(srcTarGroup.getKey()).toLowerCase(),
                    srcTarGroup.getKey());
                Pair<String, String> tarDbAndGroup = Pair.of(
                    GroupInfoUtil.buildPhysicalDbNameFromGroupName(srcTarGroup.getValue()).toLowerCase(),
                    srcTarGroup.getValue());
                Pair<String, Integer> sourceHostIpAndPort =
                    PhysicalBackfillUtils.getSrcMySQLHostForCloneTask(sourceStorageId, executionContext);
                List<Pair<String, Integer>> targetHostsIpAndPort =
                    PhysicalBackfillUtils.getMySQLServerNodeIpAndPorts(targetStorageId, healthyCheck);
                final long batchSize =
                    executionContext.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_BATCH_SIZE);
                final long minUpdateBatch =
                    executionContext.getParamManager()
                        .getLong(ConnectionParams.PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE);
                final long parallelism =
                    executionContext.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_PARALLELISM);
                final long ioAdvise =
                    executionContext.getParamManager()
                        .getLong(ConnectionParams.PHYSICAL_BACKFILL_IMPORT_TABLESPACE_IO_ADVISE);

                List<String> phyPartNames =
                    PhysicalBackfillUtils.getPhysicalPartitionNames(schemaName, srcDbAndGroup.getValue(),
                        srcDbAndGroup.getKey(),
                        phyTb);

                Pair<String, String> userAndPasswd = storageInstAndUserInfos.computeIfAbsent(targetStorageId,
                    key -> PhysicalBackfillUtils.getUserPasswd(targetStorageId));
                sourceStorageInsts.add(sourceStorageId);
                targetStorageInsts.add(targetStorageId);

                boolean hasNoPhyPart = GeneralUtil.isEmpty(phyPartNames);
                List<String> temPhyPartNames = new ArrayList<>();
                if (hasNoPhyPart) {
                    temPhyPartNames.add("");
                } else {
                    temPhyPartNames.addAll(phyPartNames);
                }
                Long dataSize = phyTableSize.get(phyTb.toLowerCase());
                CloneTableDataFileTask cloneTableDataFileTask =
                    new CloneTableDataFileTask(schemaName, tableName, srcDbAndGroup, tarDbAndGroup, phyTb,
                        phyPartNames, sourceStorageId, sourceHostIpAndPort, targetHostsIpAndPort, batchSize,
                        dataSize, tableMeta.isEncryption());
                cloneTableDataFileTask.setTaskId(ID_GENERATOR.nextId());

                List<DdlTask> importTableSpaceTasks = new ArrayList<>();

                PhysicalBackfillTask physicalBackfillTask =
                    new PhysicalBackfillTask(schemaName, cloneTableDataFileTask.getTaskId(), tableName,
                        phyTb.toLowerCase(),
                        phyPartNames,
                        Pair.of(srcTarGroup.left, srcTarGroup.right),
                        Pair.of(sourceStorageId, targetStorageId),
                        storageInstAndUserInfos,
                        batchSize,
                        dataSize,
                        parallelism,
                        minUpdateBatch,
                        waitLsn,
                        tableMeta.isEncryption());

                for (Pair<String, Integer> hostIpAndPort : targetHostsIpAndPort) {
                    ImportTableSpaceDdlTask importTableSpaceDdlTask =
                        new ImportTableSpaceDdlTask(schemaName, tableName, tarDbAndGroup.getKey(), phyTb, hostIpAndPort,
                            userAndPasswd, targetStorageId, dataSize, ioAdvise);
                    importTableSpaceTasks.add(importTableSpaceDdlTask);
                }
                List<DdlTask> tasks = new ArrayList<>(importTableSpaceTasks.size() + 2);
                tasks.add(cloneTableDataFileTask);
                tasks.add(physicalBackfillTask);
                tasks.addAll(importTableSpaceTasks);
                physicalyTaskPipeLine.add(tasks);
            }
            Map<String, String> targetStorageIds = new HashMap<>();
            for (GroupDetailInfoExRecord groupDetailInfoExRecord : preparedData.getGroupDetailInfoExRecords()) {
                targetStorageIds.putIfAbsent(groupDetailInfoExRecord.getGroupName(),
                    groupDetailInfoExRecord.storageInstId);
            }

            AlterTableGroupMovePartitionsCheckTask changeSetCheckTask =
                new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName,
                    ptbGroupMap,
                    sourceTableTopology,
                    targetTableTopology,
                    useApplyOpt, relatedTables, sourceStorageInsts, targetStorageInsts, totalDataSize, tableGroupName,
                    physicalPartitions, null);
            AlterTableGroupMovePartitionsCheckTask changeSetCheckTwiceTask =
                new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName,
                    ptbGroupMap,
                    sourceTableTopology,
                    targetTableTopology,
                    false, relatedTables, sourceStorageInsts, targetStorageInsts, totalDataSize, tableGroupName,
                    physicalPartitions, null);

            movePartitionTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
                schemaName, tableName,
                relatedTables,
                finalStatus,
                changeSetStartTask,
                catchUpTasks,
                null,
                changeSetCheckTask,
                changeSetCheckTwiceTask,
                changeSetApplyFinishTask,
                backfillTaskEdgeNodes,
                executionContext);
        } else {
            AlterTableGroupMovePartitionsCheckTask changeSetCheckTask =
                new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName,
                    ptbGroupMap,
                    sourceTableTopology,
                    targetTableTopology,
                    useApplyOpt, relatedTables, null, null, totalDataSize, tableGroupName, physicalPartitions, null);
            AlterTableGroupMovePartitionsCheckTask changeSetCheckTwiceTask =
                new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName,
                    ptbGroupMap,
                    sourceTableTopology,
                    targetTableTopology,
                    false, relatedTables, null, null, totalDataSize, tableGroupName, physicalPartitions, null);

            AlterTableGroupBackFillTask alterTableGroupBackFillTask =
                new AlterTableGroupBackFillTask(schemaName, tableName, ptbGroupMap, sourceTableTopology,
                    targetTableTopology,
                    isBroadcast(),
                    ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION == taskType, true, false);
            movePartitionTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
                schemaName, tableName,
                relatedTables,
                finalStatus,
                changeSetStartTask,
                catchUpTasks,
                alterTableGroupBackFillTask,
                changeSetCheckTask,
                changeSetCheckTwiceTask,
                changeSetApplyFinishTask,
                backfillTaskEdgeNodes,
                executionContext);
        }
        DdlTask CreateTablePhyDdlTask = taskList.get(taskList.size() - 1);
        taskList.addAll(movePartitionTasks);
        executableDdlJob.addSequentialTasks(taskList);

        if (!fallbackToLogicalBackfill) {
            executableDdlJob.removeTaskRelationship(CreateTablePhyDdlTask, movePartitionTasks.get(0));
            for (DdlTask discardTask : discardTableSpaceTasks) {
                executableDdlJob.addTaskRelationship(CreateTablePhyDdlTask, discardTask);
                executableDdlJob.addTaskRelationship(discardTask, movePartitionTasks.get(0));
            }
        }

        //cdc ddl mark task
        SqlKind sqlKind = ddl.kind();
        DdlContext dc = executionContext.getDdlContext();

        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();
        if (stayAtPublic) {
            cdcTableGroupDdlMarkTask = new CdcTableGroupDdlMarkTask(tableGroupName, schemaName, tableName,
                sqlKind, newTopology, dc.getDdlStmt(),
                sqlKind == SqlKind.ALTER_TABLEGROUP ? CdcDdlMarkVisibility.Private : CdcDdlMarkVisibility.Protected,
                false);
        }

        if (changeSetApplyExecutorInitTask != null) {
            executableDdlJob.labelAsHead(changeSetApplyExecutorInitTask);
        } else {
            executableDdlJob.labelAsHead(addMetaTask);
        }
        executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));
        return executableDdlJob;
    }

    @Override
    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }

    @Override
    public List<DdlTask> getBackfillTaskEdgeNodes() {
        return backfillTaskEdgeNodes;
    }

    @Override
    public List<List<DdlTask>> getPhysicalyTaskPipeLine() {
        return physicalyTaskPipeLine;
    }
}
