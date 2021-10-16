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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableGroupSubTaskJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupItemPreparedData preparedData;
    private final List<PhyDdlTableOperation> phyDdlTableOperations;
    private final Map<String, List<List<String>>> tableTopology;
    private final Map<String, Set<String>> targetTableTopology;
    private final Map<String, Set<String>> sourceTableTopology;
    protected final List<Pair<String, String>> orderedTargetTableLocations;
    private final boolean skipBackfill;
    protected final ExecutionContext executionContext;
    private final String targetPartition;

    public AlterTableGroupSubTaskJobFactory(DDL ddl, AlterTableGroupItemPreparedData preparedData,
                                            List<PhyDdlTableOperation> phyDdlTableOperations,
                                            Map<String, List<List<String>>> tableTopology,
                                            Map<String, Set<String>> targetTableTopology,
                                            Map<String, Set<String>> sourceTableTopology,
                                            List<Pair<String, String>> orderedTargetTableLocations,
                                            String targetPartition,
                                            boolean skipBackfill,
                                            ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.phyDdlTableOperations = phyDdlTableOperations;
        this.ddl = ddl;
        this.tableTopology = tableTopology;
        this.targetTableTopology = targetTableTopology;
        this.sourceTableTopology = sourceTableTopology;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.skipBackfill = skipBackfill;
        this.executionContext = executionContext;
        this.targetPartition = targetPartition;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
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

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        phyDdlTableOperations.forEach(o -> o.setPartitionInfo(newPartitionInfo));
        if (!tableTopology.isEmpty()) {
            PhysicalPlanData physicalPlanData =
                DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations);
            DdlTask phyDdlTask =
                new CreateTablePhyDdlTask(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData);
            taskList.add(phyDdlTask);
        }
        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology,
                skipBackfill || tableTopology.isEmpty(), executionContext);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        //cdc ddl mark task
        SqlKind sqlKind = ddl.kind();
        Map<String, Set<String>> newTopology =
            newPartitionInfo.getPhysicalPartitionTopology(null).entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey,
                    v -> v.getValue().stream().map(PhysicalPartitionInfo::getPhyTable).collect(Collectors.toSet())));
        DdlTask cdcDdlMarkTask = new CdcTableGroupDdlMarkTask(schemaName, tableName, sqlKind, newTopology);
        taskList.add(cdcDdlMarkTask);

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(addMetaTask);
        executableDdlJob.labelAsTail(bringUpNewPartitions.get(bringUpNewPartitions.size() - 1));
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String partName : preparedData.getOldPartitionNames()) {
            resources.add(concatWithDot(
                concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()), partName),
                preparedData.getTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        String tableGroupName = preparedData.getTableGroupName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        List<PartitionGroupRecord> inVisiblePartitionGroupRecords = preparedData.getInvisiblePartitionGroups();

        SqlNode sqlAlterTableGroupSpecNode = ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);

        PartitionInfo newPartInfo =
            AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(curPartitionInfo, inVisiblePartitionGroupRecords, sqlAlterTableGroupSpecNode,
                    tableGroupName,
                    targetPartition,
                    orderedTargetTableLocations,
                    preparedData.getNewPartitionNames(),
                    executionContext);

        return newPartInfo;
    }

}
