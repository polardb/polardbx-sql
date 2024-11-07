package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AlterTableGroupDropPartitionRefreshMetaTask")
public class AlterTableGroupDropPartitionRefreshMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected String targetTableGroupName;
    protected String tableName;
    protected boolean dropPartitionGroup;
    protected String sourceSql;
    protected Set<String> oldPartitions;
    protected boolean dropSubPartition;

    @JSONCreator
    public AlterTableGroupDropPartitionRefreshMetaTask(String schemaName,
                                                       String tableGroupName,
                                                       String targetTableGroupName,
                                                       String tableName,
                                                       boolean dropPartitionGroup,
                                                       String sourceSql,
                                                       Set<String> oldPartitions,
                                                       boolean dropSubPartition) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.targetTableGroupName = targetTableGroupName;
        this.tableName = tableName;
        this.dropPartitionGroup = dropPartitionGroup;
        this.sourceSql = sourceSql;
        this.oldPartitions = oldPartitions;
        this.dropSubPartition = dropSubPartition;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        removePartitionsTobeDrop(metaDbConnection, executionContext);
        refreshTableGroupMeta(metaDbConnection, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void removePartitionsTobeDrop(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tbAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();

        tbAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);
        if (GeneralUtil.isEmpty(tableGroupConfig.getTables())) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                tableGroupName + " is empty");
        }
        boolean onlyOneTable = tableGroupConfig.getTables().size() == 1;
        List<String> tableNames = dropPartitionGroup ? tableGroupConfig.getTables() : ImmutableList.of(tableName);
        String firstTb = tableNames.get(0);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTb);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                "PartitionInfo is not exists for table:" + firstTb);
        }

        List<TablePartitionRecord> inValidTablePartitions;
        if (partitionInfo.containSubPartitions()) {
            inValidTablePartitions = tbAccessor.getInValidTablePartitionsByDbNameTbNameLevel(schemaName, firstTb,
                TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION);
        } else {
            inValidTablePartitions = tbAccessor.getInValidTablePartitionsByDbNameTbNameLevel(schemaName, firstTb,
                TablePartitionRecord.PARTITION_LEVEL_PARTITION);
        }

        if (GeneralUtil.isEmpty(inValidTablePartitions)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                "invalidTablePartitionRecord is not exists for table:" + firstTb);
        } else {
            if (onlyOneTable || dropPartitionGroup) {
                for (TablePartitionRecord partitionRecord : inValidTablePartitions) {
                    partitionGroupAccessor.deletePartitionGroupById(partitionRecord.groupId);
                }
            }
        }

        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        for (String partition : oldPartitions) {
            if (dropSubPartition) {
                if (partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate()) {
                    for (String tableName : tableNames) {
                        tbAccessor.deletePartitionBySchTempPartL2(schemaName, tableName, partition);
                    }
                } else {
                    Optional<PartitionGroupRecord> partitionGroupRecordOpt =
                        partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partition))
                            .findFirst();
                    if (!partitionGroupRecordOpt.isPresent()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                            "partition group:" + partition + " is not exists");
                    }
                    if (dropPartitionGroup) {
                        tbAccessor.deletePartitionBySchGidL2(schemaName, partitionGroupRecordOpt.get().id);
                    } else {
                        tbAccessor.deletePartitionBySchTbGidL2(schemaName, tableName, partitionGroupRecordOpt.get().id);
                    }
                }
            } else {
                for (String tableName : tableNames) {
                    tbAccessor.deletePartitionBySchPartL1(schemaName, tableName, partition);
                }
                if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                    PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitionByPartName(partition);
                    for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                        Optional<PartitionGroupRecord> partitionGroupRecordOpt =
                            partitionGroupRecords.stream()
                                .filter(o -> o.partition_name.equalsIgnoreCase(subPartSpec.getName()))
                                .findFirst();
                        if (!partitionGroupRecordOpt.isPresent()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                                "partition group:" + subPartSpec.getName() + " is not exists");
                        }
                        if (dropPartitionGroup) {
                            tbAccessor.deletePartitionBySchGidL2(schemaName, partitionGroupRecordOpt.get().id);
                        } else {
                            tbAccessor.deletePartitionBySchTbGidL2(schemaName, tableName,
                                partitionGroupRecordOpt.get().id);
                        }
                    }
                }
            }
        }

        if (dropSubPartition && !partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate()) {
            boolean noLogicalPartDelete = true;
            for (String tableName : tableNames) {
                tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
                partitionInfo = tableMeta.getPartitionInfo();

                if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                    for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                        boolean deleteLogicalPart = true;
                        if (partitionSpec.getSubPartitions().size() <= oldPartitions.size()) {
                            for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                                if (!oldPartitions.contains(subPartSpec.getName())) {
                                    deleteLogicalPart = false;
                                    break;
                                }
                            }
                            if (deleteLogicalPart) {
                                noLogicalPartDelete = false;
                                tbAccessor.deleteTablePartitionsById(partitionSpec.getId());
                            }
                        }
                    }
                }
                if (noLogicalPartDelete) {
                    break;
                }
            }
        }

    }

    public void refreshTableGroupMeta(Connection metaDbConnection, ExecutionContext executionContext) {

        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);

        List<String> tableNames = dropPartitionGroup ? tableGroupConfig.getTables() : ImmutableList.of(tableName);

        String firstTb = tableNames.get(0);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTb);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        boolean isUpsert = true;
        for (String tableName : tableNames) {
            if (!StringUtils.isEmpty(targetTableGroupName) && !tableGroupName.equalsIgnoreCase(targetTableGroupName)) {
                TableGroupConfig newTableGroupConfig =
                    OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                        .getTableGroupConfigByName(targetTableGroupName);

                long newTableGroupId = newTableGroupConfig.getTableGroupRecord().id;

                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                        TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                assert tablePartitionRecords.size() == 1;

                // 1.1、update table's groupid
                tablePartitionAccessor.updateGroupIdById(newTableGroupId, tablePartitionRecords.get(0).id);

                List<TablePartitionRecord> phyTablePartitionRecords;
                if (partitionInfo.containSubPartitions()) {
                    phyTablePartitionRecords =
                        tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                            TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION);
                } else {
                    phyTablePartitionRecords =
                        tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                            TablePartitionRecord.PARTITION_LEVEL_PARTITION);
                }
                List<PartitionGroupRecord> partitionGroupRecords = newTableGroupConfig.getPartitionGroupRecords();

                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
                    TablePartitionRecord tablePartitionRecord =
                        phyTablePartitionRecords.stream()
                            .filter(tp -> tp.getPartName().equalsIgnoreCase(partitionGroupRecord.getPartition_name()))
                            .findFirst().orElse(null);

                    if (tablePartitionRecord == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "can't find the partition:" + partitionGroupRecord.getPartition_name());
                    }

                    // 1.2、update partition's groupid
                    tablePartitionAccessor.updateGroupIdById(partitionGroupRecord.id, tablePartitionRecord.getId());
                }
            }
            tablePartitionAccessor.deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
            List<TablePartitionRecord> logicalTableRecords =
                tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                    TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
            assert logicalTableRecords.size() == 1;

            List<TablePartitionRecord> tablePartitionRecords =
                tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                    TablePartitionRecord.PARTITION_LEVEL_PARTITION);

            List<TablePartitionRecord> tableSubPartitionRecords =
                tablePartitionAccessor.getValidTablePartitionsByDbNameTbNameLevel(schemaName, tableName,
                    TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION);

            Map<String, List<TablePartitionRecord>> subPartRecordInfos =
                prepareRecordForAllSubpartitions(partitionInfo, tablePartitionRecords, tableSubPartitionRecords);

            tablePartitionAccessor.addNewTablePartitionConfigs(logicalTableRecords.get(0),
                tablePartitionRecords,
                subPartRecordInfos,
                isUpsert, false);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    private Map<String, List<TablePartitionRecord>> prepareRecordForAllSubpartitions(
        PartitionInfo partitionInfo,
        List<TablePartitionRecord> parentRecords,
        List<TablePartitionRecord> subPartRecords) {

        Map<String, List<TablePartitionRecord>> subPartRecordInfos = new HashMap<>();
        if (partitionInfo.getPartitionBy().getSubPartitionBy() == null) {
            return subPartRecordInfos;
        }

        for (int k = 0; k < parentRecords.size(); k++) {
            TablePartitionRecord parentRecord = parentRecords.get(k);
            Map<Long, List<TablePartitionRecord>> subPartMap = subPartRecords.stream()
                .collect(Collectors.groupingBy(TablePartitionRecord::getParentId));
            List<TablePartitionRecord> subPartRecList = subPartMap.get(parentRecord.id);
            if (subPartRecList != null) {
                subPartRecordInfos.put(parentRecord.getPartName(), subPartRecList);
            }
        }
        return subPartRecordInfos;
    }

}
