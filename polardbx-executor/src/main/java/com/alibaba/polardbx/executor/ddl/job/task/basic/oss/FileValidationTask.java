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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillValidator;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "FileValidationTask")
public class FileValidationTask extends BaseGmsTask {
    protected final String loadTableSchema;
    protected final String loadTableName;
    protected final String localPartitionName;

    @JSONCreator
    public FileValidationTask(String schemaName, String logicalTableName,
                              String loadTableSchema, String loadTableName,
                              String localPartitionName) {
        super(schemaName, logicalTableName);
        this.loadTableSchema = loadTableSchema;
        this.loadTableName = loadTableName;
        this.localPartitionName = localPartitionName;

        // Pause if check failed.
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // 0. check task state and skip this task if allowed.
        if (allowSkip(metaDbConnection)) {
            return;
        }

        // 1. find mapping: <partition, local_partition> - files - physical_table, construct validators.
        final String sourceLogicalSchema = loadTableSchema;
        final String sourceLogicalTable = loadTableName;
        TableMeta sourceTableMeta =
            executionContext.getSchemaManager(sourceLogicalSchema).getTable(sourceLogicalTable);
        final String targetLogicalSchema = schemaName;
        final String targetLogicalTable = logicalTableName;

        // find local partition lower bound && upper bound
        OSSBackFillValidator.ValidatorBound boundVal =
            findBoundVal(executionContext, sourceLogicalSchema, sourceLogicalTable);

        // handle single table.
        Pair<String, String> singleTopology =
            OSSTaskUtils.getSingleTopology(sourceLogicalSchema, sourceLogicalTable, sourceTableMeta);

        PartitionInfo sourceTablePartitionInfo =
            OSSTaskUtils.getSourcePartitionInfo(executionContext, sourceLogicalSchema, sourceLogicalTable);
        final boolean isSingle = sourceTablePartitionInfo.isSingleTable();
        final boolean isBroadcast = sourceTablePartitionInfo.isBroadcastTable();

        // construct back-fill validators.
        FilesAccessor filesAccessor = new FilesAccessor();
        filesAccessor.setConnection(metaDbConnection);
        List<OSSBackFillValidator> validatorList = new ArrayList<>();
        for (PhysicalPartitionInfo targetPhysicalPartitionInfo :
            getFlattenedPartitionInfo(targetLogicalSchema, targetLogicalTable)) {
            final String partName = targetPhysicalPartitionInfo.getPartName();
            Pair<String, String> sourcePhySchemaAndTable = Optional
                .ofNullable(singleTopology)
                .orElseGet(() -> OSSTaskUtils.getSourcePhyTable(sourceTablePartitionInfo, partName));

            String targetPhySchema = targetPhysicalPartitionInfo.getGroupKey();
            String targetPhyTable = targetPhysicalPartitionInfo.getPhyTable();
            String sourcePhySchema = sourcePhySchemaAndTable.getKey();
            String sourcePhyTable = sourcePhySchemaAndTable.getValue();

            List<FilesRecord> filesRecords = filesAccessor.queryByLocalPartition(
                targetLogicalSchema, targetLogicalTable, targetPhySchema, targetPhyTable, localPartitionName);

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
                        ExecutorContext.getContext(loadTableSchema).getTransactionManager(),
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

    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table) {
        return OSSTaskUtils.getFlattenedPartitionInfo(schema, table);
    }

    protected boolean allowSkip(Connection metaDbConnection) {
        DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        ddlEngineTaskAccessor.setConnection(metaDbConnection);
        DdlEngineTaskRecord ddlEngineTaskRecord = ddlEngineTaskAccessor.query(getJobId(), getTaskId());
        return ddlEngineTaskRecord.state != null
            && DdlTaskState.valueOf(ddlEngineTaskRecord.state) == DdlTaskState.SUCCESS;
    }

    protected OSSBackFillValidator.ValidatorBound findBoundVal(ExecutionContext executionContext,
                                                               String sourceLogicalSchema, String sourceLogicalTable) {
        IRepository repository =
            ExecutorContext.getContext(sourceLogicalSchema)
                .getTopologyHandler()
                .getRepositoryHolder()
                .get(Group.GroupType.MYSQL_JDBC.toString());
        List<TableDescription> primaryTableDesc =
            LocalPartitionManager.getLocalPartitionInfoList(
                (MyRepository) repository,
                sourceLogicalSchema,
                sourceLogicalTable,
                true
            );
        TableDescription expect = primaryTableDesc.get(0);
        List<LocalPartitionDescription> localPartitionDescriptions = expect.getPartitions();

        SortedMap<Long, LocalPartitionDescription> localPartitionMap = new TreeMap<>();
        long designatedOrdinal = 0;
        String localPartitionCol = null;
        for (LocalPartitionDescription rs : localPartitionDescriptions) {
            if (!CanAccessTable.verifyPrivileges(sourceLogicalSchema, sourceLogicalTable, executionContext)) {
                continue;
            }
            localPartitionCol = rs.getPartitionExpression();
            long ordinal = rs.getPartitionOrdinalPosition();
            localPartitionMap.put(ordinal, rs);

            if (localPartitionName.equalsIgnoreCase(rs.getPartitionName())) {
                designatedOrdinal = ordinal;
            }
        }

        // lower bound
        SortedMap<Long, LocalPartitionDescription> subMap = localPartitionMap.subMap(0L, designatedOrdinal);
        long lowerBoundOrdinal = subMap.isEmpty() ? -1L : subMap.lastKey();
        String lowerBoundInclusive =
            lowerBoundOrdinal == -1L ? null : localPartitionMap.get(lowerBoundOrdinal).getPartitionDescription();
        // upper bound
        long upperBoundOrdinal = designatedOrdinal;
        String upperBoundExclusive = localPartitionMap.get(upperBoundOrdinal).getPartitionDescription();
        return new OSSBackFillValidator.ValidatorBound(sourceLogicalTable, localPartitionCol, lowerBoundInclusive,
            upperBoundExclusive);
    }
}
