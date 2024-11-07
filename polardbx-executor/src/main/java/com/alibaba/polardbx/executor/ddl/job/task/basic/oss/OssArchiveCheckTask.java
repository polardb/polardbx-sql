package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillValidator;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "OssArchiveCheckTask")
public class OssArchiveCheckTask extends BaseDdlTask {
    protected final String ossTableName;
    protected final String tmpTableSchema;
    protected final String tmpTableName;
    protected final List<String> partitionNames;
    protected final Long archiveTaskId;

    @JSONCreator
    public OssArchiveCheckTask(String schemaName, String ossTableName,
                               String tmpTableSchema, String tmpTableName,
                               List<String> partitionNames, Long archiveTaskId) {
        super(schemaName);
        this.ossTableName = ossTableName;
        this.tmpTableSchema = tmpTableSchema;
        this.tmpTableName = tmpTableName;
        this.partitionNames = partitionNames;
        this.archiveTaskId = archiveTaskId;

        // Pause if check failed.
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // 0. check task state and skip this task if allowed.
        if (allowSkip(metaDbConnection)) {
            return;
        }

        // 1. find mapping: <partition, local_partition> - files - physical_table, construct validators.
        final String sourceLogicalSchema = tmpTableSchema;
        final String sourceLogicalTable = tmpTableName;
        TableMeta sourceTableMeta =
            executionContext.getSchemaManager(sourceLogicalSchema).getTable(sourceLogicalTable);
        final String targetLogicalSchema = schemaName;
        final String targetLogicalTable = ossTableName;

        // handle single table.
        Pair<String, String> singleTopology =
            OSSTaskUtils.getSingleTopology(sourceLogicalSchema, sourceLogicalTable, sourceTableMeta);

        PartitionInfo sourceTablePartitionInfo =
            OSSTaskUtils.getSourcePartitionInfo(executionContext, sourceLogicalSchema, sourceLogicalTable);
        final boolean isSingle = sourceTablePartitionInfo.isSingleTable();
        final boolean isBroadcast = sourceTablePartitionInfo.isBroadcastTable();

        OSSBackFillValidator.ValidatorBound boundVal = findBoundVal(sourceLogicalTable);

        // construct back-fill validators.
        FilesAccessor filesAccessor = new FilesAccessor();
        filesAccessor.setConnection(metaDbConnection);
        List<OSSBackFillValidator> validatorList = new ArrayList<>();
        for (PhysicalPartitionInfo targetPhysicalPartitionInfo :
            getFlattenedPartitionInfo(targetLogicalSchema, targetLogicalTable, partitionNames)) {
            final String partName = targetPhysicalPartitionInfo.getPartName();
            Pair<String, String> sourcePhySchemaAndTable = Optional
                .ofNullable(singleTopology)
                .orElseGet(() -> OSSTaskUtils.getSourcePhyTable(sourceTablePartitionInfo, partName));

            String targetPhySchema = targetPhysicalPartitionInfo.getGroupKey();
            String targetPhyTable = targetPhysicalPartitionInfo.getPhyTable();
            String sourcePhySchema = sourcePhySchemaAndTable.getKey();
            String sourcePhyTable = sourcePhySchemaAndTable.getValue();

            List<FilesRecord> filesRecords = filesAccessor.queryByPhyTableName(
                targetLogicalSchema, targetLogicalTable, targetPhySchema, targetPhyTable, archiveTaskId);

            OSSBackFillValidator validator = new OSSBackFillValidator(
                isSingle, isBroadcast, targetLogicalSchema, targetLogicalTable, targetPhySchema, targetPhyTable,
                filesRecords,
                sourceLogicalSchema, sourceLogicalTable, sourcePhySchema, sourcePhyTable,
                partName, boundVal, getTaskId()
            );
            validatorList.add(validator);
        }

        // 2. invoke all validators
        List<OSSBackFillValidator.ValidationResult> validationResults = validatorList.stream()
            .map(validator -> {
                ExecutionContext xaEc = null;
                try {
                    ExecutionContext.CopyOption copyOption = new ExecutionContext.CopyOption()
                        .setMemoryPoolHolder(new QueryMemoryPoolHolder())
                        .setParameters(executionContext.cloneParamsOrNull());
                    xaEc = executionContext.copy(copyOption);
                    xaEc.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    return GsiUtils.wrapWithTransaction(
                        ExecutorContext.getContext(schemaName).getTransactionManager(),
                        ITransactionPolicy.XA,
                        xaEc,
                        validator::validate);
                } finally {
                    if (xaEc != null) {
                        xaEc.clearAllMemoryPool();
                    }
                }
            }).collect(Collectors.toList());

        // 3. pause ddl if check failed.
        List<OSSBackFillValidator.ValidationResult> failedResults =
            validationResults.stream().filter(r -> !r.isCheckSuccess()).collect(Collectors.toList());
        if (!failedResults.isEmpty()) {
            throw GeneralUtil.nestedException(
                "Data validation failed! Check the source table modification during migration. "
                    + "Checksum info: " + validationResults.stream().map(
                    OSSBackFillValidator.ValidationResult::toString).collect(Collectors.toList()));
        }
    }

    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table,
                                                                    List<String> partitionNames) {
        return OSSTaskUtils.getFlattenedPartitionInfo(schema, table, partitionNames);
    }

    protected boolean allowSkip(Connection metaDbConnection) {
        DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        ddlEngineTaskAccessor.setConnection(metaDbConnection);
        DdlEngineTaskRecord ddlEngineTaskRecord = ddlEngineTaskAccessor.query(getJobId(), getTaskId());
        return ddlEngineTaskRecord.state != null
            && DdlTaskState.valueOf(ddlEngineTaskRecord.state) == DdlTaskState.SUCCESS;
    }

    protected OSSBackFillValidator.ValidatorBound findBoundVal(String sourceLogicalTable) {
        return new OSSBackFillValidator.ValidatorBound(sourceLogicalTable, null, null, "MAX_VALUE");
    }
}
