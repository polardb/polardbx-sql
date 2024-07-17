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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Getter
@TaskName(name = "AlterTableRenamePartitionChangeMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableRenamePartitionChangeMetaTask extends BaseDdlTask {

    protected String tableName;
    protected String targetTableGroup;
    protected List<Pair<String, String>> changePartitionsPair;
    protected boolean subPartitionRename;

    @JSONCreator
    public AlterTableRenamePartitionChangeMetaTask(String schemaName, String targetTableGroup, String tableName,
                                                   List<Pair<String, String>> changePartitionsPair,
                                                   boolean subPartitionRename) {
        super(schemaName);
        this.targetTableGroup = targetTableGroup;
        this.tableName = tableName;
        this.changePartitionsPair = changePartitionsPair;
        this.subPartitionRename = subPartitionRename;

    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        final TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        final TableGroupRecord targetTableGroupRecord = tableGroupConfig.getTableGroupRecord();
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        final PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();

        boolean useSubPartTemplate = false;
        boolean hasSubPart = false;
        if (subPartBy != null) {
            useSubPartTemplate = subPartBy.isUseSubPartTemplate();
            hasSubPart = true;
        }

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        Map<String, String> changePartitionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Pair<String, String> pair : changePartitionsPair) {
            changePartitionsMap.put(pair.getKey(), pair.getValue());
        }
        List<TablePartitionRecord> newTablePartitionRecords = new ArrayList();
        if (!hasSubPart) {
            processRenameFirstLevelPhysicalPartition(partitionInfo, changePartitionsMap, targetTableGroupRecord,
                partitionGroupRecords, newTablePartitionRecords, metaDbConnection);
        } else if (!subPartitionRename) {
            processRenameFirstLevelLogicalPartition(partitionInfo, changePartitionsMap, targetTableGroupRecord,
                useSubPartTemplate, partitionGroupRecords, newTablePartitionRecords, metaDbConnection);
        } else {
            processRenameSubPartition(partitionInfo, changePartitionsMap, targetTableGroupRecord,
                useSubPartTemplate, partitionGroupRecords, newTablePartitionRecords, metaDbConnection);
        }
        if (GeneralUtil.isNotEmpty(newTablePartitionRecords)) {
            tablePartitionAccessor.addNewTablePartitionsWithId(newTablePartitionRecords);
        }
        String primaryTableName = tableMeta.getTableName();
        try {
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            TableInfoManager.updateTableVersion(schemaName, primaryTableName, metaDbConnection);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while update table version, schemaName:%s, tableName:%s",
                schemaName,
                primaryTableName));
            throw GeneralUtil.nestedException(t);
        }
        updateSupportedCommands(true, false, null);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private void processRenameSubPartition(PartitionInfo partitionInfo,
                                           Map<String, String> changePartitionsMap,
                                           TableGroupRecord targetGroupRecord,
                                           boolean useTemplateSubPartition,
                                           List<PartitionGroupRecord> partitionGroupRecords,
                                           List<TablePartitionRecord> newTablePartitionRecords,
                                           Connection conn) {
        boolean firstPart = true;
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(conn);
        List<TablePartitionRecord> subPartRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameTbNameLevel(schemaName,
                partitionInfo.getTableName(), TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION, false);
        Map<Long, TablePartitionRecord> subPartRecordsMap = new HashMap<>();
        for (TablePartitionRecord subPartRecord : subPartRecords) {
            subPartRecordsMap.put(subPartRecord.getId(), subPartRecord);
        }
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                TablePartitionRecord tablePartitionRecord = subPartRecordsMap.get(subPartSpec.getId());
                if (useTemplateSubPartition && changePartitionsMap.containsKey(subPartSpec.getTemplateName())
                    || changePartitionsMap.containsKey(subPartSpec.getName())) {
                    if (useTemplateSubPartition) {
                        tablePartitionRecord.partTempName = changePartitionsMap.get(subPartSpec.getTemplateName());
                        tablePartitionRecord.partName =
                            partitionSpec.getName() + tablePartitionRecord.partTempName;

                    } else {
                        tablePartitionRecord.partName = changePartitionsMap.get(subPartSpec.getTemplateName());
                    }
                }
                Optional<PartitionGroupRecord> partGroupRecord = partitionGroupRecords.stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(tablePartitionRecord.getPartName())).findFirst();
                if (!partGroupRecord.isPresent()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                        String.format(
                            "the partition group [%s.%s] is not exists",
                            targetGroupRecord.tg_name, tablePartitionRecord.partName));
                }
                tablePartitionRecord.groupId = partGroupRecord.get().getId();
                tablePartitionAccessor.deleteTablePartitionsById(tablePartitionRecord.id);
                newTablePartitionRecords.add(tablePartitionRecord);
            }
            if (firstPart) {
                firstPart = false;
                tablePartitionAccessor.updateGroupIdById(targetGroupRecord.getId(),
                    partitionSpec.getParentId());
            }
        }
    }

    private void processRenameFirstLevelLogicalPartition(PartitionInfo partitionInfo,
                                                         Map<String, String> changePartitionsMap,
                                                         TableGroupRecord targetGroupRecord,
                                                         boolean useTemplateSubPartition,
                                                         List<PartitionGroupRecord> partitionGroupRecords,
                                                         List<TablePartitionRecord> newTablePartitionRecords,
                                                         Connection conn) {
        boolean firstPart = true;
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(conn);
        List<TablePartitionRecord> subPartRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameTbNameLevel(schemaName,
                partitionInfo.getTableName(), TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION, false);
        Map<Long, TablePartitionRecord> subPartRecordsMap = new HashMap<>();
        for (TablePartitionRecord subPartRecord : subPartRecords) {
            subPartRecordsMap.put(subPartRecord.getId(), subPartRecord);
        }
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            final String partitionName;
            boolean updateName = false;
            if (changePartitionsMap.containsKey(partitionSpec.getName())) {
                partitionName = changePartitionsMap.get(partitionSpec.getName());
                updateName = true;
            } else {
                partitionName = partitionSpec.getName();
            }
            if (updateName) {
                List<TablePartitionRecord> tpRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameId(schemaName, partitionSpec.getId());
                assert tpRecords.size() == 1;
                tpRecords.get(0).partName = partitionName;
                newTablePartitionRecords.add(tpRecords.get(0));
                tablePartitionAccessor.deleteTablePartitionsById(partitionSpec.getId());
            }
            for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                TablePartitionRecord subTablePartition = subPartRecordsMap.get(subPartSpec.getId());
                if (updateName && useTemplateSubPartition) {
                    subTablePartition.partName = partitionName + subTablePartition.partTempName;
                }
                Optional<PartitionGroupRecord> partitionGroupRecord =
                    partitionGroupRecords.stream()
                        .filter(o -> o.partition_name.equalsIgnoreCase(subTablePartition.partName))
                        .findFirst();
                if (!partitionGroupRecord.isPresent()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                        String.format(
                            "the partition group [%s.%s] is not exists",
                            targetGroupRecord.tg_name, subTablePartition.partName));
                }
                subTablePartition.groupId = partitionGroupRecord.get().id;
                tablePartitionAccessor.deleteTablePartitionsById(subTablePartition.getId());
                newTablePartitionRecords.add(subTablePartition);
            }
            if (firstPart) {
                firstPart = false;
                tablePartitionAccessor.updateGroupIdById(targetGroupRecord.getId(),
                    partitionSpec.getParentId());
            }
        }

    }

    private void processRenameFirstLevelPhysicalPartition(PartitionInfo partitionInfo,
                                                          Map<String, String> changePartitionsMap,
                                                          TableGroupRecord targetGroupRecord,
                                                          List<PartitionGroupRecord> partitionGroupRecords,
                                                          List<TablePartitionRecord> newTablePartitionRecords,
                                                          Connection conn) {
        boolean firstPart = true;
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(conn);
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            final String partitionName;
            boolean updateName = false;
            if (changePartitionsMap.containsKey(partitionSpec.getName())) {
                partitionName = changePartitionsMap.get(partitionSpec.getName());
                updateName = true;
            } else {
                partitionName = partitionSpec.getName();
            }
            Optional<PartitionGroupRecord> partitionGroupRecord =
                partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partitionName))
                    .findFirst();
            if (!partitionGroupRecord.isPresent()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    String.format(
                        "the tableGroup[%s].[%s] is not exists",
                        targetGroupRecord.tg_name, partitionName));
            }
            if (updateName) {
                List<TablePartitionRecord> tpRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameId(schemaName, partitionSpec.getId());

                assert tpRecords.size() == 1;
                tpRecords.get(0).groupId = partitionGroupRecord.get().id;
                tpRecords.get(0).partName = partitionName;
                newTablePartitionRecords.add(tpRecords.get(0));

                tablePartitionAccessor.deleteTablePartitionsById(partitionSpec.getId());
            } else {
                tablePartitionAccessor.updateGroupIdById(partitionGroupRecord.get().id, partitionSpec.getId());
            }
            if (firstPart) {
                firstPart = false;
                tablePartitionAccessor.updateGroupIdById(targetGroupRecord.getId(),
                    partitionSpec.getParentId());
            }
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

}
