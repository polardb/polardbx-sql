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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddPartitionMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class AlterTableAddPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableAddPartitionPreparedData parentPrepareData;

    public AlterTableAddPartitionSubTaskJobFactory(DDL ddl,
                                                   AlterTableAddPartitionPreparedData parentPrepareData,
                                                   AlterTableGroupItemPreparedData preparedData,
                                                   List<PhyDdlTableOperation> phyDdlTableOperations,
                                                   TreeMap<String, List<List<String>>> tableTopology,
                                                   Map<String, Set<String>> targetTableTopology,
                                                   Map<String, Set<String>> sourceTableTopology,
                                                   Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                   String targetPartition,
                                                   boolean skipBackfill,
                                                   ComplexTaskMetaManager.ComplexTaskType taskType,
                                                   ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
                sourceTableTopology, orderedTargetTableLocations, targetPartition, skipBackfill, taskType,
                executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        PartitionInfo curPartitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(preparedData.getTableName());

        final TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        String targetTableGroupName = null;
        if (StringUtils.isNotEmpty(parentPrepareData.getTargetTableGroup())) {
            targetTableGroupName = parentPrepareData.getTargetTableGroup();

        } else if (StringUtils.isNotEmpty(parentPrepareData.getTargetImplicitTableGroupName())) {
            targetTableGroupName = parentPrepareData.getTargetImplicitTableGroupName();
        }
        if (StringUtils.isNotEmpty(targetTableGroupName)) {
            TableGroupConfig tableGroupConfig =
                    tableGroupInfoManager.getTableGroupConfigByName(targetTableGroupName);
            if (tableGroupConfig != null) {
                newPartitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().id);
            }
        }
        List<DdlTask> taskList = new ArrayList<>();

        if (!preparedData.isColumnarIndex()) {
            phyDdlTableOperations.forEach(o -> o.setPartitionInfo(newPartitionInfo));
            if (!tableTopology.isEmpty()) {
                PhysicalPlanData physicalPlanData =
                        DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations,
                                executionContext);
                DdlTask phyDdlTask =
                        new CreatePhyTableWithRollbackCheckTask(preparedData.getSchemaName(), physicalPlanData.getLogicalTableName(),
                                physicalPlanData, sourceTableTopology);
                taskList.add(phyDdlTask);
            }
        }

        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
                PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                        newPartitionInfo.getPartitionBy().getPartitions());

        AlterTableGroupAddPartitionMetaTask addSubTaskMetaTask =
                new AlterTableGroupAddPartitionMetaTask(preparedData.getSchemaName(), StringUtils.isNotEmpty(targetTableGroupName) ? targetTableGroupName : preparedData.getTableGroupName(),
                        preparedData.getTableName(),
                        logTableRec, partRecList, subPartRecInfos, getParentPrepareData().getSourceSql());
        taskList.add(addSubTaskMetaTask);
        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(taskList.get(0));
        executableDdlJob.labelAsTail(addSubTaskMetaTask);

        SqlKind sqlKind = ddl.kind();
        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();

        DdlContext dc = executionContext.getDdlContext();

        DdlTask cdcDdlMarkTask =
                new CdcTableGroupDdlMarkTask(preparedData.getTableGroupName(), preparedData.getSchemaName(),
                        preparedData.getTableName(), sqlKind, newTopology,
                        dc.getDdlStmt(),
                        sqlKind == SqlKind.ALTER_TABLEGROUP ? CdcDdlMarkVisibility.Private : CdcDdlMarkVisibility.Protected,
                        preparedData.isColumnarIndex());
        boolean stayAtPublic = true;
        final String finalStatus =
                executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                    StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }
        if (stayAtPublic) {
            cdcTableGroupDdlMarkTask = cdcDdlMarkTask;
        }
        return executableDdlJob;
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        PartitionInfo curPartitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(preparedData.getTableName());

        SqlNode sqlNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                        parentPrepareData,
                        curPartitionInfo,
                        false,
                        sqlNode,
                        preparedData.getOldPartitionNames(),
                        preparedData.getNewPartitionNames(),
                        parentPrepareData.getTableGroupName(),
                        null,
                        preparedData.getInvisiblePartitionGroups(),
                        orderedTargetTableLocations,
                        executionContext);

        if (parentPrepareData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPrepareData, newPartInfo);
        }

        //checkPartitionCount(newPartInfo);

        return newPartInfo;
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }

}
