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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterPartitionTableTruncatePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RepartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

public class LogicalAlterTableHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalAlterTable logicalAlterTable = (LogicalAlterTable) logicalDdlPlan;

        if (logicalAlterTable.needRewriteToGsi(false)) {
            logicalAlterTable.needRewriteToGsi(true);
        }

        if (logicalAlterTable.isRepartition()
            || logicalAlterTable.isCreateGsi()
            || logicalAlterTable.isCreateClusteredIndex()) {
            initPrimaryTableDefinition(logicalAlterTable, executionContext);
        }
        logicalAlterTable.prepareData();
        if (logicalAlterTable.isRepartition()) {
            return buildRepartitionJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isCreateGsi()
            || logicalAlterTable.isCreateClusteredIndex()) {
            return buildCreateGsiJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isDropGsi()) {
            return buildDropGsiJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isAlterTableRenameGsi()) {
            // will not support
            // /*+TDDL:cmd_extra(FORCE_DDL_ON_LEGACY_ENGINE=true,ALLOW_ALTER_GSI_INDIRECTLY=true)*/alter table t rename index ...
            throw new TddlRuntimeException(
                ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY,
                logicalAlterTable.getTableName());
        } else if (logicalAlterTable.isTruncatePartition()) {
            return buildAlterTableTruncatePartitionJob(logicalAlterTable, executionContext);
        } else {
            return buildAlterTableJob(logicalAlterTable, executionContext);
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String logicalTableName = logicalDdlPlan.getTableName();

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        if (sqlAlterTable.getTableOptions() != null && sqlAlterTable.getTableOptions().getComment() != null) {
            TableValidator.validateTableComment(logicalTableName,
                sqlAlterTable.getTableOptions().getComment().toValue());
        }

        TableValidator.validateTableName(logicalTableName);

        ColumnValidator.validateColumnLimits(logicalDdlPlan.getSchemaName(), logicalTableName, sqlAlterTable);

        IndexValidator.validateIndexNameLengths(sqlAlterTable);

        ConstraintValidator.validateConstraintLimits(sqlAlterTable);

        return false;
    }

    private DdlJob buildAlterTableJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        ExecutableDdlJob alterTableJob =
            new AlterTableJobFactory(physicalPlanData, alterTablePreparedData, logicalAlterTable, executionContext)
                .create();

        // Apply local index modification to clustered-index table
        AlterTableWithGsiPreparedData gsiData = logicalAlterTable.getAlterTableWithGsiPreparedData();
        if (gsiData != null) {
            // add local index
            CreateIndexWithGsiPreparedData createIndexWithGsi = gsiData.getCreateIndexWithGsiPreparedData();
            if (createIndexWithGsi != null) {
                ExecutableDdlJob localIndexJob =
                    CreateIndexJobFactory.createLocalIndex(
                        logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
                        createIndexWithGsi.getLocalIndexPreparedDataList(),
                        executionContext);
                if (localIndexJob != null) {
                    alterTableJob.appendJob(localIndexJob);
                }
            }

            // drop local index
            DropIndexWithGsiPreparedData dropIndexWithGsi = gsiData.getDropIndexWithGsiPreparedData();
            if (dropIndexWithGsi != null) {
                ExecutableDdlJob localIndexJob = DropIndexJobFactory.createDropLocalIndex(
                    logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
                    dropIndexWithGsi.getLocalIndexPreparedDataList(),
                    false,
                    executionContext);
                if (localIndexJob != null) {
                    alterTableJob.appendJob(localIndexJob);
                }
            }

            // TODO it's not elegant to de-duplicate in this way, try to avoid duplication in prepareData()
            // Alters on clustered-index table and normal global index table
            Set<AlterTablePreparedData> alterOnGsi = new HashSet<>(gsiData.getClusteredIndexPrepareData());
            alterOnGsi.addAll(gsiData.getGlobalIndexPreparedData());
            alterOnGsi.removeIf(alter -> {
                if (dropIndexWithGsi != null &&
                    dropIndexWithGsi.getLocalIndexPreparedDataList().stream().anyMatch(x -> x.canEquals(alter))) {
                    return true;
                }
                if (createIndexWithGsi != null &&
                    createIndexWithGsi.getLocalIndexPreparedDataList().stream().anyMatch(x -> x.canEquals(alter))) {
                    return true;
                }
                return false;
            });

            for (AlterTablePreparedData clusteredTable : alterOnGsi) {
                DdlPhyPlanBuilder builder =
                    AlterTableBuilder.create(logicalAlterTable.relDdl, clusteredTable, executionContext).build();

                PhysicalPlanData clusterIndexPlan = builder.genPhysicalPlanData();
                AlterTableJobFactory jobFactory =
                    new AlterTableJobFactory(clusterIndexPlan, clusteredTable, logicalAlterTable, executionContext);
                jobFactory.validateExistence(false);
                jobFactory.withAlterGsi(true, alterTablePreparedData.getTableName());

                ExecutableDdlJob clusterIndexJob = jobFactory.create();
                alterTableJob.appendJob(clusterIndexJob);
            }
        }

        return alterTableJob;
    }

    private DdlJob buildRepartitionJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        initPrimaryTableDefinition(logicalAlterTable, executionContext);

        logicalAlterTable.prepareData();

        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();

        CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getCreateIndexWithGsiPreparedData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            createIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(
            logicalAlterTable.relDdl,
            globalIndexPreparedData,
            executionContext
        ).build();

        boolean isPartitionRuleUnchanged = RepartitionValidator.checkPartitionRuleUnchanged(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.isSingle(),
            globalIndexPreparedData.isBroadcast(),
            globalIndexPreparedData.getIndexTableRule()
        );
        //no need to repartition
        if (isPartitionRuleUnchanged) {
            return new TransientDdlJob();
        }
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        SqlAlterTablePartitionKey ast = (SqlAlterTablePartitionKey) logicalAlterTable.relDdl.sqlNode;

        //validate
        RepartitionValidator.validate(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getDbPartitionBy(),
            ast.isBroadcast(),
            ast.isSingle()
        );

        return new RepartitionJobFactory(
            ast,
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexTableName(),
            globalIndexPreparedData.isSingle(),
            globalIndexPreparedData.isBroadcast(),
            globalIndexPreparedData.getColumns(),
            globalIndexPreparedData.getCoverings(),
            physicalPlanData,
            executionContext
        ).create();
    }

    private DdlJob buildCreateGsiJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        CreateIndexWithGsiPreparedData indexPreparedData =
            alterTableWithGsiPreparedData.getCreateIndexWithGsiPreparedData();
        CreateGlobalIndexPreparedData globalIndexPreparedData = indexPreparedData.getGlobalIndexPreparedData();

        // global index
        ExecutableDdlJob gsiJob =
            CreatePartitionGsiJobFactory.create(logicalAlterTable.relDdl, globalIndexPreparedData, executionContext);

        // local index
        ExecutableDdlJob localIndexJob = CreateIndexJobFactory.createLocalIndex(
            logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
            indexPreparedData.getLocalIndexPreparedDataList(),
            executionContext
        );
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        return gsiJob;
    }

    private DdlJob buildDropGsiJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
            dropIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        ExecutableDdlJob baseJob = new ExecutableDdlJob();

        ExecutableDdlJob ddlJob = DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, true);
        if (ddlJob != null) {
            baseJob.appendJob(ddlJob);
        }

        ExecutableDdlJob lsiJob = DropIndexJobFactory.createDropLocalIndex(
            logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
            dropIndexWithGsiPreparedData.getLocalIndexPreparedDataList(),
            true,
            executionContext);
        if (lsiJob != null) {
            baseJob.appendJob(lsiJob);
        }
        return baseJob;
    }

    private DdlJob buildAlterTableTruncatePartitionJob(LogicalAlterTable logicalAlterTable,
                                                       ExecutionContext executionContext) {
        boolean isNewPartDb = DbInfoManager.getInstance()
            .isNewPartitionDb(logicalAlterTable.getAlterTablePreparedData().getSchemaName());
        if (!isNewPartDb) {
            throw new TddlNestableRuntimeException("Unsupported alter table truncate partition");
        } else {
            if (logicalAlterTable.isWithGsi()) {
                throw new TddlNestableRuntimeException("Unsupported alter table truncate partition which has GSI");
            }
        }
        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();

        DdlPhyPlanBuilder alterTableBuilder =
            new AlterPartitionTableTruncatePartitionBuilder(logicalAlterTable.relDdl, alterTablePreparedData,
                executionContext).build();

        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();
        physicalPlanData.setTruncatePartition(true);
        ExecutableDdlJob alterTableJob =
            new AlterTableJobFactory(physicalPlanData, alterTablePreparedData, logicalAlterTable, executionContext)
                .create();

        return alterTableJob;
    }

    /**
     * Get table definition from primary table and generate index table definition with it
     */
    private void initPrimaryTableDefinition(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);

        if (logicalDdlPlan.getNativeSqlNode() instanceof SqlAlterTablePartitionKey) {
            //generate GSI sqlNode for altering partition key DDL
            SqlAlterTablePartitionKey ast =
                (SqlAlterTablePartitionKey) logicalDdlPlan.getNativeSqlNode();

            String primaryTableName = ast.getOriginTableName().getLastName();
            String targetTableName =
                executionContext.getParamManager().getString(ConnectionParams.REPARTITION_FORCE_GSI_NAME);
            if (StringUtils.isEmpty(targetTableName)) {
                targetTableName = generateRandomGsiName(primaryTableName);
            }
            ast.setLogicalSecondaryTableName(targetTableName);

            List<SqlIndexDefinition> gsiList = new ArrayList<>();
            SqlIndexDefinition repartitionGsi =
                initIndexInfo(primaryTableInfo.getValue(), ast, primaryTableInfo.getKey());
            gsiList.add(repartitionGsi);
            List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
                new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
            ).collect(Collectors.toList());
            ast.getAlters().addAll(sqlAddIndexList);
        } else if (logicalDdlPlan.getNativeSqlNode() instanceof SqlAlterTable) {
            final SqlAddIndex addIndex =
                (SqlAddIndex) ((SqlAlterTable) logicalDdlPlan.getNativeSqlNode()).getAlters().get(0);
            addIndex.getIndexDef().setPrimaryTableDefinition(primaryTableInfo.getKey());
            addIndex.getIndexDef().setPrimaryTableNode(primaryTableInfo.getValue());

        }
    }

    private static String generateRandomGsiName(String logicalSourceTableName) {
        String randomSuffix =
            RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME).toLowerCase();
        String targetTableName = logicalSourceTableName + "_" + randomSuffix;
        return targetTableName;
    }

    /**
     * generate GSI according the primary table information
     */
    private static SqlIndexDefinition initIndexInfo(SqlCreateTable primaryTableNode,
                                                    SqlAlterTablePartitionKey alterTablePartitionKey,
                                                    String primaryTableDefinition) {
        if (StringUtils.isEmpty(alterTablePartitionKey.getLogicalSecondaryTableName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }
        Set<String> partitionColumnSet = new HashSet<>();
        if (!alterTablePartitionKey.isBroadcast() && !alterTablePartitionKey.isSingle()) {
            SqlNode[] dbPartitionColumns = ((SqlBasicCall) alterTablePartitionKey.getDbPartitionBy()).getOperands();
            partitionColumnSet.addAll(
                Stream.of(dbPartitionColumns)
                    .filter(e -> e instanceof SqlIdentifier)
                    .map(e -> RelUtils.stringValue(e))
                    .collect(Collectors.toSet()));
            if (alterTablePartitionKey.getTablePartitionBy() != null) {
                SqlNode[] tbPartitionColumns =
                    ((SqlBasicCall) alterTablePartitionKey.getTablePartitionBy()).getOperands();
                partitionColumnSet.addAll(
                    Stream.of(tbPartitionColumns)
                        .filter(e -> e instanceof SqlIdentifier)
                        .map(e -> RelUtils.stringValue(e))
                        .collect(Collectors.toSet()));
            }
        } else {
            final String schemaName = alterTablePartitionKey.getOriginTableName().getComponent(0).getLastName();
            final String sourceLogicalTable = alterTablePartitionKey.getOriginTableName().getComponent(1).getLastName();
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(sourceLogicalTable);
            List<String> primaryKeys =
                GlobalIndexMeta.getPrimaryKeys(tableMeta).stream().map(String::toLowerCase).collect(
                    Collectors.toList());
            partitionColumnSet.addAll(primaryKeys);
        }

        List<SqlIndexColumnName> indexColumns = partitionColumnSet.stream()
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null, null))
            .collect(Collectors.toList());

        List<SqlIndexColumnName> coveringColumns = primaryTableNode.getColDefs().stream()
            .filter(e -> !partitionColumnSet.contains(e.getKey().getLastName()))
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, e.getKey(), null, null))
            .collect(Collectors.toList());

        SqlIndexDefinition indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
            false,
            null,
            null,
            null,
            new SqlIdentifier(alterTablePartitionKey.getLogicalSecondaryTableName(), SqlParserPos.ZERO),
            (SqlIdentifier) primaryTableNode.getTargetTable(),
            indexColumns,
            coveringColumns,
            alterTablePartitionKey.getDbPartitionBy(),
            alterTablePartitionKey.getTablePartitionBy(),
            alterTablePartitionKey.getTbpartitions(),
            null,
            new LinkedList<>());
        indexDef.setBroadcast(alterTablePartitionKey.isBroadcast());
        indexDef.setSingle(alterTablePartitionKey.isSingle());
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

}
