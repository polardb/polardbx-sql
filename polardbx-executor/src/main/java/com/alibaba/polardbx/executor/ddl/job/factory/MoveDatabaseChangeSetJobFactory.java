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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.MoveTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CloneTableDataFileTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DiscardTableSpaceDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.MoveTableCheckTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;
import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genChangeSetCatchUpTasks;

public class MoveDatabaseChangeSetJobFactory extends MoveDatabaseSubTaskJobFactory {
    private ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask;
    private ChangeSetApplyFinishTask changeSetApplyFinishTask;

    protected final List<PhyDdlTableOperation> discardTableSpaceOperations;
    protected final Map<String, String> tarGroupAndStorageIds;
    protected final boolean usePhysicalBackfill;
    protected List<DdlTask> backfillTaskEdgeNodes = new ArrayList<>(2);
    protected List<List<DdlTask>> physicalyTaskPipeLine = new ArrayList<>();
    protected final Map<String, String> sourceAndTarDnMap;
    protected final Map<String, Pair<String, String>> storageInstAndUserInfos;

    public MoveDatabaseChangeSetJobFactory(DDL ddl, MoveDatabaseItemPreparedData preparedData,
                                           List<PhyDdlTableOperation> phyDdlTableOperations,
                                           List<PhyDdlTableOperation> discardTableSpaceOperations,
                                           Map<String, String> sourceAndTarDnMap,
                                           Map<String, Pair<String, String>> storageInstAndUserInfos,
                                           Map<String, List<List<String>>> tableTopology,
                                           Map<String, Set<String>> targetTableTopology,
                                           Map<String, Set<String>> sourceTableTopology,
                                           ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                           ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                           Map<String, String> tarGroupAndStorageIds,
                                           boolean usePhysicalBackfill,
                                           ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            executionContext);
        this.changeSetApplyExecutorInitTask = changeSetApplyExecutorInitTask;
        this.changeSetApplyFinishTask = changeSetApplyFinishTask;
        this.discardTableSpaceOperations = discardTableSpaceOperations;
        this.tarGroupAndStorageIds = tarGroupAndStorageIds;
        this.storageInstAndUserInfos = storageInstAndUserInfos;
        this.sourceAndTarDnMap = sourceAndTarDnMap;
        this.usePhysicalBackfill = usePhysicalBackfill;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        final boolean shareStorageMode =
            executionContext.getParamManager().getBoolean(ConnectionParams.SHARE_STORAGE_MODE);
        DdlTask addMetaTask =
            new MoveDatabaseAddMetaTask(schemaName, ImmutableList.of(tableName), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(),
                ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE.getValue(),
                1);

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
        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations, executionContext);
        DdlTask phyDdlTask =
            new CreatePhyTableWithRollbackCheckTask(schemaName, physicalPlanData.getLogicalTableName(),
                physicalPlanData, sourceTableTopology);
        taskList.add(phyDdlTask);
        MoveTableBackFillTask moveTableBackFillTask = null;
        List<DdlTask> discardTableSpaceTasks = null;
        if (usePhysicalBackfill) {
            discardTableSpaceTasks = ScaleOutUtils.generateDiscardTableSpaceDdlTask(schemaName, tableTopology,
                discardTableSpaceOperations, executionContext);
        } else {
            moveTableBackFillTask =
                new MoveTableBackFillTask(schemaName, tableName, sourceTableTopology, targetTableTopology,
                    preparedData.getSourceTargetGroupMap(), false, true);
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

        Long changeSetId = ChangeSetManager.getChangeSetId();

        ChangeSetStartTask changeSetStartTask = new ChangeSetStartTask(
            schemaName,
            tableName,
            sourceTableTopology,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE,
            changeSetId
        );

        Map<String, ChangeSetCatchUpTask> catchUpTasks = genChangeSetCatchUpTasks(
            schemaName,
            tableName,
            null,
            sourceTableTopology,
            preparedData.getSourceTargetGroupMap(),
            ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE,
            changeSetId
        );

        final boolean useApplyOpt = changeSetApplyFinishTask != null
            && executionContext.getParamManager().getBoolean(CHANGE_SET_APPLY_OPTIMIZATION);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG);

        List<DdlTask> moveDatabaseTasks;
        backfillTaskEdgeNodes.clear();
        physicalyTaskPipeLine.clear();
        Map<String, String> groupAndDbMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final boolean waitLsn = executionContext.getParamManager()
            .getBoolean(ConnectionParams.PHYSICAL_BACKFILL_WAIT_LSN_WHEN_ROLLBACK);

        boolean healthyCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);

        long totalDataSize = 0l;
        if (usePhysicalBackfill) {
            for (Map.Entry<String, Set<String>> entry : sourceTableTopology.entrySet()) {
                String srcGroupName = entry.getKey();
                String tarGroupName = GroupInfoUtil.buildScaleOutGroupName(srcGroupName);
                Pair<String, String> srcTarGroup = Pair.of(srcGroupName, tarGroupName);
                String sourceStorageId = sourceAndTarDnMap.computeIfAbsent(srcTarGroup.getKey(),
                    key -> DbTopologyManager.getStorageInstIdByGroupName(schemaName, srcTarGroup.getKey()));
                String targetStorageId = tarGroupAndStorageIds.get(srcTarGroup.getValue());

                String srcDbName = groupAndDbMap.computeIfAbsent(srcTarGroup.getKey(),
                    key -> DbTopologyManager.getPhysicalDbNameByGroupKeyFromMetaDb(schemaName, srcTarGroup.getKey()));

                Pair<String, String> srcDbAndGroup = Pair.of(srcDbName.toLowerCase(), srcTarGroup.getKey());
                String tarDbName;
                if (shareStorageMode) {
                    tarDbName = groupAndDbMap.computeIfAbsent(srcTarGroup.getValue(),
                        key -> GroupInfoUtil.buildScaleOutPhyDbName(schemaName, srcTarGroup.getKey()));
                } else {
                    tarDbName = groupAndDbMap.computeIfAbsent(srcTarGroup.getValue(), key -> srcDbName);
                }
                Pair<String, String> tarDbAndGroup = Pair.of(tarDbName.toLowerCase(),
                    srcTarGroup.getValue());
                Pair<String, Integer> sourceHostIpAndPort =
                    PhysicalBackfillUtils.getMySQLOneFollowerIpAndPort(sourceStorageId);
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

                for (String phyTb : entry.getValue()) {
                    List<String> phyPartNames =
                        PhysicalBackfillUtils.getPhysicalPartitionNames(schemaName, srcDbAndGroup.getValue(),
                            srcDbAndGroup.getKey(),
                            phyTb);

                    Pair<String, String> srcDnUserAndPasswd = storageInstAndUserInfos.computeIfAbsent(sourceStorageId,
                        key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageId));

                    Pair<String, String> userAndPasswd = storageInstAndUserInfos.computeIfAbsent(targetStorageId,
                        key -> PhysicalBackfillUtils.getUserPasswd(targetStorageId));

                    boolean hasNoPhyPart = GeneralUtil.isEmpty(phyPartNames);
                    List<String> temPhyPartNames = new ArrayList<>();
                    if (hasNoPhyPart) {
                        temPhyPartNames.add("");
                    } else {
                        temPhyPartNames.addAll(phyPartNames);
                    }
                    PolarxPhysicalBackfill.GetFileInfoOperator fileInfoOperator =
                            PhysicalBackfillUtils.checkFileExistence(srcDnUserAndPasswd, srcDbAndGroup.getKey(),
                                    phyTb.toLowerCase(),
                                    temPhyPartNames,
                                    true, PhysicalBackfillUtils.getMySQLLeaderIpAndPort(sourceStorageId));
                    long dataSize = 0l;
                    for (PolarxPhysicalBackfill.FileInfo fileInfo : fileInfoOperator.getTableInfo().getFileInfoList()) {
                        dataSize += fileInfo.getDataSize();
                    }

                    CloneTableDataFileTask cloneTableDataFileTask =
                        new CloneTableDataFileTask(schemaName, tableName, srcDbAndGroup, tarDbAndGroup, phyTb,
                            phyPartNames, sourceStorageId, sourceHostIpAndPort, targetHostsIpAndPort, batchSize,
                            dataSize, tableMeta.isEncryption());
                    cloneTableDataFileTask.setTaskId(ID_GENERATOR.nextId());

                    List<DdlTask> importTableSpaceTasks = new ArrayList<>();

                    PhysicalBackfillTask physicalBackfillTask =
                        new PhysicalBackfillTask(schemaName, cloneTableDataFileTask.getTaskId(), tableName, phyTb,
                            phyPartNames,
                            srcTarGroup,
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
                            new ImportTableSpaceDdlTask(schemaName, tableName, tarDbAndGroup.getKey(), phyTb,
                                hostIpAndPort,
                                userAndPasswd,
                                targetStorageId,
                                dataSize,
                                ioAdvise);
                        importTableSpaceTasks.add(importTableSpaceDdlTask);
                    }
                    List<DdlTask> tasks = new ArrayList<>(importTableSpaceTasks.size() + 2);
                    tasks.add(cloneTableDataFileTask);
                    tasks.add(physicalBackfillTask);
                    tasks.addAll(importTableSpaceTasks);
                    physicalyTaskPipeLine.add(tasks);
                    totalDataSize += dataSize;
                }
            }

            MoveTableCheckTask moveTableCheckTask =
                new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(),
                    sourceTableTopology,
                    targetTableTopology, useApplyOpt, relatedTables, totalDataSize);
            MoveTableCheckTask moveTableCheckTwiceTask =
                new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(),
                    sourceTableTopology,
                    targetTableTopology, false, relatedTables, totalDataSize);
            moveDatabaseTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
                schemaName, tableName,
                relatedTables,
                finalStatus,
                changeSetStartTask,
                catchUpTasks,
                null,
                moveTableCheckTask,
                moveTableCheckTwiceTask,
                changeSetApplyFinishTask,
                backfillTaskEdgeNodes,
                executionContext);
        } else {
            MoveTableCheckTask moveTableCheckTask =
                new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(),
                    sourceTableTopology,
                    targetTableTopology, useApplyOpt, relatedTables, totalDataSize);
            MoveTableCheckTask moveTableCheckTwiceTask =
                new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(),
                    sourceTableTopology,
                    targetTableTopology, false, relatedTables, totalDataSize);
            moveDatabaseTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
                schemaName, tableName,
                relatedTables,
                finalStatus,
                changeSetStartTask,
                catchUpTasks,
                moveTableBackFillTask,
                moveTableCheckTask,
                moveTableCheckTwiceTask,
                changeSetApplyFinishTask,
                backfillTaskEdgeNodes,
                executionContext);
        }

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        taskList.addAll(moveDatabaseTasks);
        executableDdlJob.addSequentialTasks(taskList);

        if (usePhysicalBackfill) {
            executableDdlJob.removeTaskRelationship(phyDdlTask, moveDatabaseTasks.get(0));
            for (DdlTask discardTask : discardTableSpaceTasks) {
                executableDdlJob.addTaskRelationship(phyDdlTask, discardTask);
                executableDdlJob.addTaskRelationship(discardTask, moveDatabaseTasks.get(0));
            }
        }

        //todo(ziyang) cdc ddl mark task
        if (changeSetApplyExecutorInitTask != null) {
            executableDdlJob.labelAsHead(changeSetApplyExecutorInitTask);
        } else {
            executableDdlJob.labelAsHead(addMetaTask);
        }

        executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));

        return executableDdlJob;
    }

    public List<DdlTask> getBackfillTaskEdgeNodes() {
        return backfillTaskEdgeNodes;
    }

    public List<List<DdlTask>> getPhysicalyTaskPipeLine() {
        return physicalyTaskPipeLine;
    }
}
