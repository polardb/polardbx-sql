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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupDropPartitionAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils.updatePartitionSpecRelationship;
import static com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils.updateSubPartitionTemplate;

public class AlterTableDropPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableGroupDropPartitionPreparedData parentPrepareData;
    protected List<DdlTask> allTaskList;

    public AlterTableDropPartitionSubTaskJobFactory(DDL ddl,
                                                    AlterTableGroupDropPartitionPreparedData parentPrepareData,
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
    protected ExecutableDdlJob doCreate() {
        PartitionInfo curPartitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(preparedData.getTableName());
        SqlNode sqlNode;
        if (ddl.getSqlNode() instanceof SqlAlterTable) {
            sqlNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);
        } else {
            sqlNode = ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        }
        final TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        PartitionInfo newPartitionInfo = AlterTableGroupSnapShotUtils
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
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
                PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                        newPartitionInfo.getPartitionBy().getPartitions());

        AlterTableGroupDropPartitionAddSubTaskMetaTask addSubTaskMetaTask =
                new AlterTableGroupDropPartitionAddSubTaskMetaTask(preparedData.getSchemaName(),
                        preparedData.getTableName(),
                        logTableRec, partRecList, subPartRecInfos);
        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(addSubTaskMetaTask);
        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        taskList.addAll(generateSyncTask(preparedData.getSchemaName(), preparedData.getTableName(), executionContext));
        executableDdlJob.addSequentialTasks(taskList);

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
        allTaskList = taskList;
        return executableDdlJob;
    }

    public List<DdlTask> generateSyncTask(String schemaName,
                                          String tableName,
                                          ExecutionContext executionContext) {

        List<String> logicalTableNames = new ArrayList<>();
        // not include GSI tables
        Set<String> primaryLogicalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // for alter table set tableGroup, only need to care about the table in "alter table" only
        String logicalTable = tableName;
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTable);
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            logicalTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        if (!primaryLogicalTables.contains(logicalTable)) {
            logicalTableNames.add(logicalTable);
            primaryLogicalTables.add(logicalTable);
        }


        List<DdlTask> ddlTasks = new ArrayList<>(2);
        DdlTask updateTablesVersionTask = new UpdateTablesVersionTask(schemaName, logicalTableNames);
        //not use preemptive sync to interrupt dml， just wait for the sync task to finish
        DdlTask tablesSyncTask =
                new TablesSyncTask(schemaName, logicalTableNames, true);
        ddlTasks.add(updateTablesVersionTask);
        ddlTasks.add(tablesSyncTask);
        return ddlTasks;
    }

    public List<DdlTask> getAllTaskList() {
        return allTaskList;
    }
}
