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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupItemBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddLogicalForeignKeyTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropLogicalForeignKeyTask;
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
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

public class AlterTableGroupSubTaskJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupItemPreparedData preparedData;
    private final AlterTableGroupBasePreparedData parentPrepareData;
    protected final List<PhyDdlTableOperation> phyDdlTableOperations;
    protected final Map<String, List<List<String>>> tableTopology;
    protected final Map<String, Set<String>> targetTableTopology;
    protected final Map<String, Set<String>> sourceTableTopology;
    protected final Map<String, Pair<String, String>> orderedTargetTableLocations;
    protected final boolean skipBackfill;
    protected final ComplexTaskMetaManager.ComplexTaskType taskType;
    protected final ExecutionContext executionContext;
    protected final String targetPartition;
    protected DdlTask cdcTableGroupDdlMarkTask;

    public AlterTableGroupSubTaskJobFactory(DDL ddl,
                                            AlterTableGroupBasePreparedData parentPrepareData,
                                            AlterTableGroupItemPreparedData preparedData,
                                            List<PhyDdlTableOperation> phyDdlTableOperations,
                                            Map<String, List<List<String>>> tableTopology,
                                            Map<String, Set<String>> targetTableTopology,
                                            Map<String, Set<String>> sourceTableTopology,
                                            Map<String, Pair<String, String>> orderedTargetTableLocations,
                                            String targetPartition,
                                            boolean skipBackfill,
                                            ComplexTaskMetaManager.ComplexTaskType taskType,
                                            ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.parentPrepareData = parentPrepareData;
        this.phyDdlTableOperations = phyDdlTableOperations;
        this.ddl = ddl;
        this.tableTopology = tableTopology;
        this.targetTableTopology = targetTableTopology;
        this.sourceTableTopology = sourceTableTopology;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.skipBackfill = skipBackfill;
        this.executionContext = executionContext;
        this.targetPartition = targetPartition;
        this.taskType = taskType;
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

        //1. add logical foreign key
        DdlTask addLogicalForeignKeyTask = getPushDownForeignKeysTask(schemaName, tableName, true);
        taskList.add(addLogicalForeignKeyTask);

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        if (!preparedData.isColumnarIndex()) {
            phyDdlTableOperations.forEach(o -> o.setPartitionInfo(newPartitionInfo));
            if (!tableTopology.isEmpty()) {
                PhysicalPlanData physicalPlanData =
                    DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations,
                        executionContext);
                DdlTask phyDdlTask =
                    new CreatePhyTableWithRollbackCheckTask(schemaName, physicalPlanData.getLogicalTableName(),
                        physicalPlanData, sourceTableTopology);
                taskList.add(phyDdlTask);
            }
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
        boolean skipBackFill = skipBackfill || tableTopology.isEmpty() || preparedData.isColumnarIndex();
        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology,
                stayAtCreating, stayAtDeleteOnly, stayAtWriteOnly, stayAtWriteReOrg,
                skipBackFill, executionContext, isBroadcast(), taskType);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (!tableTopology.isEmpty() && tableMeta.isGsi() && !tableMeta.isClustered() && !tableMeta.isHasPrimaryKey()
            && !tableMeta.getGsiTableMetaBean().gsiMetaBean.nonUnique) {

            String sql =
                "drop index " + TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME + " on " + surroundWithBacktick(tableName);

            DropPartLocalIndexBuilder builder =
                DropPartLocalIndexBuilder.createBuilder(schemaName, tableName, TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME,
                    sql, tableTopology, newPartitionInfo, executionContext);
            List<PhyDdlTableOperation> phyDdlTableOperations = builder.build().getPhysicalPlans();

            PhysicalPlanData physicalPlanData =
                DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations, executionContext);
            DdlTask phyDdlTask =
                new DropIndexPhyDdlTask(schemaName, physicalPlanData);
            taskList.add(phyDdlTask);
        }

        //cdc ddl mark task
        SqlKind sqlKind = ddl.kind();
        DdlContext dc = executionContext.getDdlContext();

        // drop logical foreign key
        DdlTask dropLogicalForeignKeyTask = getPushDownForeignKeysTask(schemaName, tableName, false);
        taskList.add(dropLogicalForeignKeyTask);

        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();
        DdlTask cdcDdlMarkTask =
            new CdcTableGroupDdlMarkTask(tableGroupName, schemaName, tableName, sqlKind, newTopology,
                dc.getDdlStmt(),
                sqlKind == SqlKind.ALTER_TABLEGROUP ? CdcDdlMarkVisibility.Private : CdcDdlMarkVisibility.Protected,
                preparedData.isColumnarIndex());
        if (stayAtPublic) {
            cdcTableGroupDdlMarkTask = cdcDdlMarkTask;
        }

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(addLogicalForeignKeyTask);
        if (!stayAtCreating) {
            executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));
        } else {
            executableDdlJob.labelAsTail(mayBeTailTask);
        }
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String phyTableName : GeneralUtil.emptyIfNull(preparedData.getNewPhyTables())) {
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
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(preparedData.getTableName());

        List<PartitionGroupRecord> invisiblePartitionGroupRecords = preparedData.getInvisiblePartitionGroups();

        SqlNode sqlAlterTableGroupSpecNode = ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        String tableGroupName = preparedData.getTableGroupName();

        PartitionInfo newPartInfo =
            AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    getParentPrepareData(),
                    curPartitionInfo,
                    true,
                    sqlAlterTableGroupSpecNode,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    tableGroupName,
                    targetPartition,
                    invisiblePartitionGroupRecords,
                    orderedTargetTableLocations,
                    executionContext);

        return newPartInfo;
    }

    protected void checkPartitionCount(PartitionInfo newPartInfo) {
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (executionContext != null) {
            maxPhysicalPartitions =
                executionContext.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        int phyPartCntOfNewPartInfo = newPartInfo.getPartitionBy().getPhysicalPartitions().size();
        if (phyPartCntOfNewPartInfo > maxPhysicalPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String
                    .format(
                        "Too many partitions [%s] (including subpartitions) after altering tablegroup is not allowed",
                        phyPartCntOfNewPartInfo));
        }
    }

    public DdlTask getCdcTableGroupDdlMarkTask() {
        return cdcTableGroupDdlMarkTask;
    }

    public boolean isBroadcast() {
        return false;
    }

    protected boolean schemaChange(PartitionInfo curPartInfo, PartitionInfo newPartInfo) {
        if (curPartInfo.getPartitionBy().getPartitions().size() != newPartInfo.getPartitionBy().getPartitions()
            .size()) {
            return true;
        }
        for (int i = 0; i < curPartInfo.getPartitionBy().getPartitions().size(); i++) {
            PartitionSpec curPartSpec = curPartInfo.getPartitionBy().getPartitions().get(i);
            PartitionSpec newPartSpec = newPartInfo.getPartitionBy().getPartitions().get(i);
            if (!curPartSpec.getStrategy().isList() && curPartSpec.getBoundSpaceComparator()
                .compare(curPartSpec.getBoundSpec().getSingleDatum(), newPartSpec.getBoundSpec().getSingleDatum())
                != 0) {
                return true;
            }
            if (curPartSpec.getStrategy().isList() && !curPartSpec.getBoundSpec().equals(newPartSpec.getBoundSpec())) {
                return true;
            }
            if (GeneralUtil.emptyIfNull(curPartSpec.getSubPartitions()).size() != GeneralUtil.emptyIfNull(
                newPartSpec.getSubPartitions()).size()) {
                return true;
            }
            if (GeneralUtil.isNotEmpty(curPartSpec.getSubPartitions())) {
                for (int j = 0; j < curPartSpec.getSubPartitions().size(); j++) {
                    PartitionSpec curSubPartSpec = curPartSpec.getSubPartitions().get(j);
                    PartitionSpec newSubPartSpec = newPartSpec.getSubPartitions().get(j);
                    if (curSubPartSpec.getBoundSpaceComparator()
                        .compare(curSubPartSpec.getBoundSpec().getSingleDatum(),
                            newSubPartSpec.getBoundSpec().getSingleDatum())
                        != 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected void updateNewPartitionInfoByTargetGroup(AlterTableGroupBasePreparedData parentPrepareData,
                                                       PartitionInfo newPartInfo) {
        assert parentPrepareData.isMoveToExistTableGroup();
        String schemaName = parentPrepareData.getSchemaName();
        String targetTableGroup = parentPrepareData.getTargetTableGroup();
        TableGroupConfig targetTableGroupInfo =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(targetTableGroup);
        List<PartitionGroupRecord> partitionGroupRecords = targetTableGroupInfo.getPartitionGroupRecords();
        //List<PartitionSpec> partitionSpecs = newPartInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> partitionSpecs = newPartInfo.getPartitionBy().getPhysicalPartitions();
        assert partitionGroupRecords.size() == partitionSpecs.size();
        newPartInfo.setTableGroupId(targetTableGroupInfo.getTableGroupRecord().id);
        for (int i = 0; i < partitionSpecs.size(); i++) {
            final String partitionName = partitionSpecs.get(i).getName();
            Optional<PartitionGroupRecord> partitionGroupRecord = partitionGroupRecords.stream()
                .filter(o -> o.partition_name.equalsIgnoreCase(partitionName)).findFirst();
            if (!partitionGroupRecord.isPresent()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    "the partition:" + partitionSpecs.get(i).getName() + " is not exists in table group:"
                        + parentPrepareData.getTargetTableGroup());
            }
            partitionSpecs.get(i).getLocation().setPartitionGroupId(partitionGroupRecord.get().id);
        }
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }

    DdlTask getPushDownForeignKeysTask(String schemaName, String tableName, boolean add) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        List<ForeignKeyData> pushDownForeignKeys = new ArrayList<>(tableMeta.getForeignKeys().values());

        if (add) {
            return new AddLogicalForeignKeyTask(schemaName, tableName, pushDownForeignKeys);
        } else {
            return new DropLogicalForeignKeyTask(schemaName, tableName, pushDownForeignKeys);
        }
    }

    public List<DdlTask> getBackfillTaskEdgeNodes() {
        return null;
    }

    public List<List<DdlTask>> getPhysicalyTaskPipeLine() {
        return null;
    }
}
