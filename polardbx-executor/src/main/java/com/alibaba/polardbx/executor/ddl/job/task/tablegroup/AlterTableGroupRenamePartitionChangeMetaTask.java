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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Getter
@TaskName(name = "AlterTableGroupRenamePartitionChangeMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupRenamePartitionChangeMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected List<Pair<String, String>> changePartitionsPair;
    protected boolean subPartitionRename;

    @JSONCreator
    public AlterTableGroupRenamePartitionChangeMetaTask(String schemaName, String tableGroupName,
                                                        List<Pair<String, String>> changePartitionsPair,
                                                        boolean subPartitionRename) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.changePartitionsPair = changePartitionsPair;
        this.subPartitionRename = subPartitionRename;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        final TableGroupConfig tgCofig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        final TableGroupDetailConfig tableGroupConfig =
            TableGroupUtils.getTableGroupDetailInfoByGroupId(null, tgCofig.getTableGroupRecord().id);
        final List<TablePartRecordInfoContext> tablePartitionInfoRecords =
            tableGroupConfig.getTablesPartRecordInfoContext();
        String firstTableInTg = tablePartitionInfoRecords.get(0).getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTableInTg);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();

        boolean hasSubPart = false;
        if (subPartBy != null) {
            hasSubPart = true;
        }

        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        List<TablePartitionRecord> newTablePartitionRecords = new ArrayList<>();
        List<PartitionGroupRecord> newPartitionGroupRecords = new ArrayList<>();

        if (subPartitionRename) {
            processRenameSubPartition(partitionInfo, partitionGroupRecords, newTablePartitionRecords,
                newPartitionGroupRecords, metaDbConnection);
        } else if (hasSubPart) {
            processRenameFirstLevelLogicalPartition(tableGroupConfig, partitionInfo, partitionGroupRecords,
                newTablePartitionRecords,
                newPartitionGroupRecords, metaDbConnection);
        } else {
            processRenameFirstLevelPhysicalPartition(partitionGroupRecords, newTablePartitionRecords,
                newPartitionGroupRecords, metaDbConnection);
        }

        for (PartitionGroupRecord prd : newPartitionGroupRecords) {
            partitionGroupAccessor.addNewPartitionGroupWithId(prd);
        }

        tablePartitionAccessor.addNewTablePartitionsWithId(newTablePartitionRecords);

        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        if (!GeneralUtil.isEmpty(tablePartitionInfoRecords)) {
            for (TablePartRecordInfoContext recordInfoContext : tablePartitionInfoRecords) {
                try {
                    String tableName = recordInfoContext.getLogTbRec().tableName;
                    tableMeta = schemaManager.getTable(tableName);
                    if (tableMeta.isGsi()) {
                        //all the gsi table version change will be behavior by primary table
                        assert
                            tableMeta.getGsiTableMetaBean() != null
                                && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                        tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    }
                    TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConnection);
                } catch (Throwable t) {
                    LOGGER.error(String.format(
                        "error occurs while update table version, schemaName:%s, tableName:%s",
                        recordInfoContext.getLogTbRec().tableSchema,
                        recordInfoContext.getLogTbRec().tableName));
                    throw GeneralUtil.nestedException(t);
                }
            }
        }
        updateSupportedCommands(true, false, null);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private void processRenameSubPartition(
        PartitionInfo partitionInfo,
        List<PartitionGroupRecord> partitionGroupRecords,
        List<TablePartitionRecord> newTablePartitionRecords,
        List<PartitionGroupRecord> newpartitionGroupRecords,
        Connection conn) {
        PartitionByDefinition partBy = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartBy = partBy.getSubPartitionBy();
        boolean useSubPartTemplate = subPartBy.isUseSubPartTemplate();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(conn);
        tablePartitionAccessor.setConnection(conn);

        for (PartitionSpec partitionSpec : partBy.getPartitions()) {
            for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                for (Pair<String, String> pair : changePartitionsPair) {
                    String partitionGroupName;
                    if (useSubPartTemplate) {
                        if (pair.getKey().equalsIgnoreCase(subPartitionSpec.getTemplateName())) {
                            partitionGroupName = subPartitionSpec.getName();
                        } else {
                            continue;
                        }
                    } else {
                        if (pair.getKey().equalsIgnoreCase(subPartitionSpec.getName())) {
                            partitionGroupName = pair.getKey();
                        } else {
                            continue;
                        }

                    }
                    PartitionGroupRecord partitionGroupRecord =
                        partitionGroupRecords.stream()
                            .filter(o -> o.partition_name.equalsIgnoreCase(partitionGroupName))
                            .findFirst().orElse(null);
                    partitionGroupAccessor.deletePartitionGroupById(partitionGroupRecord.id);
                    List<TablePartitionRecord> tbps =
                        tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName,
                            partitionGroupRecord.id);
                    if (GeneralUtil.isNotEmpty(tbps)) {
                        for (TablePartitionRecord tb : tbps) {
                            if (useSubPartTemplate) {
                                tb.partTempName = pair.getValue();
                                tb.partName = partitionSpec.getName() + tb.partTempName;
                            } else {
                                tb.partName = pair.getValue();
                            }
                        }
                        newTablePartitionRecords.addAll(tbps);
                    }
                    PartitionGroupRecord newPartitionGroupRecord = partitionGroupRecord.copy();
                    newPartitionGroupRecord.partition_name =
                        useSubPartTemplate ? partitionSpec.getName() + pair.getValue() : pair.getValue();
                    newpartitionGroupRecords.add(newPartitionGroupRecord);
                    tablePartitionAccessor.deleteTablePartitions(schemaName, partitionGroupRecord.id);
                }
            }
        }
    }

    private void processRenameFirstLevelLogicalPartition(TableGroupDetailConfig tableGroupConfig,
                                                         PartitionInfo partitionInfo,
                                                         List<PartitionGroupRecord> partitionGroupRecords,
                                                         List<TablePartitionRecord> newTablePartitionRecords,
                                                         List<PartitionGroupRecord> newPartitionGroupRecords,
                                                         Connection conn) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(conn);
        tablePartitionAccessor.setConnection(conn);

        PartitionByDefinition partBy = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartBy = partBy.getSubPartitionBy();
        boolean useSubPartTemplate = subPartBy.isUseSubPartTemplate();

        Map<String, List<TablePartitionRecord>> firstLevelPartitionRecords = new TreeMap<>(String::compareToIgnoreCase);
        for (TablePartRecordInfoContext tablePartInfo : tableGroupConfig.getTablesPartRecordInfoContext()) {
            tablePartInfo.getPartitionRecList().stream().forEach(
                o -> firstLevelPartitionRecords.computeIfAbsent(o.partName, k -> new ArrayList<>()).add(o.copy()));
        }

        for (Pair<String, String> pair : changePartitionsPair) {
            if (firstLevelPartitionRecords.containsKey(pair.getKey())) {
                List<TablePartitionRecord> tpRecords = firstLevelPartitionRecords.get(pair.getKey());
                if (useSubPartTemplate) {
                    PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                        .filter(o -> o.getName().equalsIgnoreCase(pair.getKey())).findFirst().orElse(null);
                    for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                        String partitionGroupName = subPartSpec.getName();
                        PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                            .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionGroupName)).findFirst()
                            .orElse(null);
                        List<TablePartitionRecord> tbps =
                            tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName,
                                partitionGroupRecord.id);
                        for (TablePartitionRecord tbp : tbps) {
                            tbp.partName = pair.getValue() + tbp.getPartTempName();
                            newTablePartitionRecords.add(tbp);
                        }
                        PartitionGroupRecord newPartitionGroupRecord = partitionGroupRecord.copy();
                        newPartitionGroupRecord.partition_name = tbps.get(0).partName;
                        newPartitionGroupRecords.add(newPartitionGroupRecord);

                        tablePartitionAccessor.deleteTablePartitions(schemaName, partitionGroupRecord.id);
                        partitionGroupAccessor.deletePartitionGroupById(partitionGroupRecord.id);
                    }

                }
                for (TablePartitionRecord tpRecord : tpRecords) {
                    tpRecord.partName = pair.getValue();
                    newTablePartitionRecords.add(tpRecord);
                    tablePartitionAccessor.deleteTablePartitionsById(tpRecord.id);
                }
            }
        }

    }

    private void processRenameFirstLevelPhysicalPartition(List<PartitionGroupRecord> partitionGroupRecords,
                                                          List<TablePartitionRecord> newTablePartitionRecords,
                                                          List<PartitionGroupRecord> newPartitionGroupRecords,
                                                          Connection conn) {

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(conn);
        tablePartitionAccessor.setConnection(conn);

        for (Pair<String, String> pair : changePartitionsPair) {
            String partitionGroupName = pair.getKey();
            PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionGroupName)).findFirst()
                .orElse(null);
            List<TablePartitionRecord> tbps =
                tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName,
                    partitionGroupRecord.id);
            for (TablePartitionRecord tbp : tbps) {
                tbp.partName = pair.getValue();
                newTablePartitionRecords.add(tbp);
            }
            PartitionGroupRecord newPartitionGroupRecord = partitionGroupRecord.copy();
            newPartitionGroupRecord.partition_name = tbps.get(0).partName;
            newPartitionGroupRecords.add(newPartitionGroupRecord);

            tablePartitionAccessor.deleteTablePartitions(schemaName, partitionGroupRecord.id);
            partitionGroupAccessor.deletePartitionGroupById(partitionGroupRecord.id);
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
