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

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class AlterTableSetTableGroupJobFactory extends DdlJobFactory {

    @Deprecated
    private final DDL ddl;
    private final AlterTableSetTableGroupPreparedData preparedData;
    private final PhysicalPlanData physicalPlanData;
    private final Map<String, Set<String>> sourceTableTopology;
    private final Map<String, Set<String>> targetTableTopology;
    private final List<PartitionGroupRecord> newPartitionRecords;
    protected final ExecutionContext executionContext;

    public AlterTableSetTableGroupJobFactory(DDL ddl, AlterTableSetTableGroupPreparedData preparedData,
                                             PhysicalPlanData physicalPlanData,
                                             Map<String, Set<String>> sourceTableTopology,
                                             Map<String, Set<String>> targetTableTopology,
                                             List<PartitionGroupRecord> newPartitionRecords,
                                             ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.physicalPlanData = physicalPlanData;
        this.sourceTableTopology = sourceTableTopology;
        this.targetTableTopology = targetTableTopology;
        this.newPartitionRecords = newPartitionRecords;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {

        String schemaName = preparedData.getSchemaName();
        String targetTableGroupName = preparedData.getTableGroupName();
        String tableName = preparedData.getTableName();

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, targetTableGroupName);
        TableGroupConfig curTableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigById(partitionInfo.getTableGroupId());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        // the old and new partition name is identical, so here use newPartitionRecords
        for (PartitionGroupRecord newRecord : newPartitionRecords) {
            for (PartitionGroupRecord record : curTableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(newRecord.getPartition_name())) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        for (PartitionGroupRecord newRecord : newPartitionRecords) {
            targetDbList.add(newRecord.getPhy_db());
            newPartitions.add(newRecord.getPartition_name());
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            targetTableGroupName,
            curTableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));
        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, targetTableGroupName, tableName,
                ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP, executionContext);

        executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);

        constructSubTasks(schemaName, partitionInfo, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()));

    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected void constructSubTasks(String schemaName, PartitionInfo curPartitionInfo,
                                     ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                     List<DdlTask> bringUpAlterTableGroupTasks) {
        String tableName = preparedData.getTableName();

        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
            PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        TableGroupConfig curTableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        //DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName());
        DdlTask addMetaTask =
            new AlterTableGroupAddSubTaskMetaTask(schemaName, tableName,
                curTableGroupConfig.getTableGroupRecord().tg_name, curPartitionInfo.getTableGroupId(), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0, logTableRec, partRecList,
                subPartRecInfos);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        //taskList.add(validateTask);

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        DdlTask phyDdlTask =
            new CreateTablePhyDdlTask(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData);
        taskList.add(phyDdlTask);
        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology, false,
                executionContext);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        final ExecutableDdlJob subTask = new ExecutableDdlJob();
        subTask.addSequentialTasks(taskList);
        subTask.labelAsHead(addMetaTask);
        subTask.labelAsTail(bringUpNewPartitions.get(bringUpNewPartitions.size() - 1));

        executableDdlJob.combineTasks(subTask);
        executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());
        executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
        DdlTask dropUselessTableTask = ComplexTaskFactory
            .CreateDropUselessPhyTableTask(schemaName, tableName, sourceTableTopology, executionContext);
        executableDdlJob.addTask(dropUselessTableTask);
        executableDdlJob.addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
            dropUselessTableTask);
        executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
    }

    private PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSetTableGroup = ddl.getSqlNode();

        return AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForSetTableGroup(curPartitionInfo, sqlAlterTableSetTableGroup);
    }

}