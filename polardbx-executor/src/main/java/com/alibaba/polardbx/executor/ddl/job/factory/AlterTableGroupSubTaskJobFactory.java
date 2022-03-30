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
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private DdlTask cdcTableGroupDdlMarkTask;

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

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }
        final boolean stayAtCreating =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.CREATING.name(), finalStatus);
        final boolean stayAtDeleteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY.name(), finalStatus);
        final boolean stayAtWriteReOrg =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(), finalStatus);

        DdlTask mayBeTailTask = taskList.get(taskList.size() - 1);
        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology,
                stayAtCreating, stayAtDeleteOnly, stayAtWriteOnly, stayAtWriteReOrg,
                skipBackfill || tableTopology.isEmpty(), executionContext);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        //cdc ddl mark task
        SqlKind sqlKind = ddl.kind();
        DdlContext dc = executionContext.getDdlContext();

        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();
        DdlTask cdcDdlMarkTask =
            new CdcTableGroupDdlMarkTask(schemaName, tableName, sqlKind, newTopology, dc.getDdlStmt());
        if (stayAtPublic) {
            cdcTableGroupDdlMarkTask = cdcDdlMarkTask;
        }

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(addMetaTask);
        if (!stayAtCreating) {
            executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));
        } else {
            executableDdlJob.labelAsTail(mayBeTailTask);
        }
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String phyTableName : preparedData.getNewPhyTables()) {
            resources.add(
                concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()), phyTableName));
        }
        //todo luoyanxin when
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()));
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

    protected void checkPartitionCount(PartitionInfo newPartInfo) {
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (executionContext != null) {
            maxPhysicalPartitions =
                executionContext.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        if (newPartInfo.getPartitionBy().getPartitions().size() > maxPhysicalPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String
                    .format("Too many partitions [%s] (including subpartitions) after alter tablegroup",
                        newPartInfo.getPartitionBy().getPartitions().size()));
        }
    }

    public DdlTask getCdcTableGroupDdlMarkTask() {
        return cdcTableGroupDdlMarkTask;
    }
}
