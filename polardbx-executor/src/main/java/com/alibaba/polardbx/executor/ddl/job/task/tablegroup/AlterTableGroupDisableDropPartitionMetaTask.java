package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Getter
@TaskName(name = "AlterTableGroupDisableDropPartitionMetaTask")
public class AlterTableGroupDisableDropPartitionMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected Long tableGroupId;
    protected String tableName;
    protected boolean dropPartitionGroup;
    protected String sourceSql;
    protected Set<String> oldPartitions;
    protected boolean dropSubPartition;

    @JSONCreator
    public AlterTableGroupDisableDropPartitionMetaTask(String schemaName,
                                                       String tableGroupName,
                                                       Long tableGroupId,
                                                       String tableName,
                                                       boolean dropPartitionGroup,
                                                       String sourceSql,
                                                       Set<String> oldPartitions,
                                                       boolean dropSubPartition) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.tableGroupId = tableGroupId;
        this.tableName = tableName;
        this.dropPartitionGroup = dropPartitionGroup;
        this.sourceSql = sourceSql;
        this.oldPartitions = oldPartitions;
        this.dropSubPartition = dropSubPartition;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        disablePartitionsTobeDrop(metaDbConnection, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void disablePartitionsTobeDrop(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tbAccessor = new TablePartitionAccessor();
        tbAccessor.setConnection(metaDbConnection);
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        List<String> tableNames = dropPartitionGroup ? tableGroupConfig.getTables() : ImmutableList.of(tableName);
        if (GeneralUtil.isEmpty(tableNames)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                tableGroupName + " is empty");
        }
        String firstTb = tableGroupConfig.getTables().get(0);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTb);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                "PartitionInfo is not exists for table:" + firstTb);
        }
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        for (String partition : oldPartitions) {
            if (dropSubPartition) {
                if (partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate()) {
                    for (String tableName : tableNames) {
                        tbAccessor.disableStatusBySchTempPartL2(schemaName, tableName, partition);
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
                        tbAccessor.disableStatusBySchGidL2(schemaName, partitionGroupRecordOpt.get().id);
                    } else {
                        tbAccessor.disableStatusBySchTbGidL2(schemaName, tableName, partitionGroupRecordOpt.get().id);
                    }
                }
            } else {
                for (String tableName : tableNames) {
                    tbAccessor.disableStatusBySchPartL1(schemaName, tableName, partition);
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
                            tbAccessor.disableStatusBySchGidL2(schemaName, partitionGroupRecordOpt.get().id);
                        } else {
                            tbAccessor.disableStatusBySchTbGidL2(schemaName, tableName,
                                partitionGroupRecordOpt.get().id);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //do not support rollback
        updateSupportedCommands(true, false, metaDbConnection);
        executeImpl(metaDbConnection, executionContext);
    }

}
