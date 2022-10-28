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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterPartitionTableTruncatePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableOnlineModifyColumnJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RepartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTableAsOfTimeStampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTableDropOssFileJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTablePurgeBeforeTimeStampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.MoveOSSDataJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.StatisticSampleTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableAllocateLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableEngineHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableExpireLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableRemoveLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableRepartitionLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

        if (logicalAlterTable.isAllocateLocalPartition()) {
            return new LogicalAlterTableAllocateLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isExpireLocalPartition()) {
            return new LogicalAlterTableExpireLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isRepartitionLocalPartition()) {
            return new LogicalAlterTableRepartitionLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isRemoveLocalPartition()) {
            return new LogicalAlterTableRemoveLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.needRewriteToGsi(false)) {
            logicalAlterTable.needRewriteToGsi(true);
        }

        if (logicalAlterTable.isRepartition()
            || logicalAlterTable.isCreateGsi()
            || logicalAlterTable.isCreateClusteredIndex()) {
            initPrimaryTableDefinition(logicalAlterTable, executionContext);
        }
        logicalAlterTable.prepareData();
        if (logicalAlterTable.isDropFile()) {
            return buildDropFileJob(logicalDdlPlan, executionContext, logicalAlterTable);
        } else if (logicalAlterTable.isExchangePartition()) {
            return buildAlterTableExchangeJob(logicalDdlPlan, executionContext, logicalAlterTable);
        } else if (logicalAlterTable.isAlterEngine()) {
            return buildAlterTableEngineJob(logicalDdlPlan, executionContext, logicalAlterTable);
        } else if (logicalAlterTable.isAlterAsOfTimeStamp()) {
            return buildAlterTableAsOfTimeStamp(logicalDdlPlan, executionContext, logicalAlterTable);
        } else if (logicalAlterTable.isAlterPurgeBeforeTimeStamp()) {
            return buildAlterTablePurgeBeforeTimeStamp(logicalDdlPlan, executionContext, logicalAlterTable);
        } else if (logicalAlterTable.isRepartition()) {
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
            if (logicalAlterTable.getAlterTablePreparedData().isOnlineModifyColumn()
                || logicalAlterTable.getAlterTablePreparedData().isOnlineChangeColumn()) {
                return buildAlterTableOnlineModifyColumnJob(logicalAlterTable, executionContext);
            } else {
                return buildAlterTableJob(logicalAlterTable, executionContext);
            }
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

        // check collation in column definitions.
        if (sqlAlterTable.getAlters() != null) {
            for (SqlAlterSpecification spec : sqlAlterTable.getAlters()) {
                if (spec instanceof SqlModifyColumn) {
                    TableValidator.validateCollationImplemented((SqlModifyColumn) spec);
                }
            }
        }

        String schemaName = logicalDdlPlan.getSchemaName();

        TableValidator.validateTableName(logicalTableName);

        ColumnValidator.validateColumnLimits(logicalDdlPlan.getSchemaName(), logicalTableName, sqlAlterTable);

        IndexValidator.validateIndexNameLengths(sqlAlterTable);

        ConstraintValidator.validateConstraintLimits(sqlAlterTable);

        TableValidator.validateTruncatePartition(logicalDdlPlan.getSchemaName(), logicalTableName, sqlAlterTable);

        return super.validatePlan(logicalDdlPlan, executionContext);
    }

    private DdlJob buildAlterTableExchangeJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext,
                                              LogicalAlterTable logicalAlterTable) {
        SqlAlterTable sqlAlterTable = logicalAlterTable.getSqlAlterTable();
        List<SqlAlterSpecification> alters = sqlAlterTable.getAlters();
        if (alters.size() > 1) {
            throw GeneralUtil.nestedException("Unsupported operation: exchange more than one partition per time");
        }
        SqlAlterTableExchangePartition sqlExchange = (SqlAlterTableExchangePartition) alters.get(0);
        String schemaName = logicalDdlPlan.getSchemaName();
        String tableName = logicalDdlPlan.getTableName();
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        // source table info
        TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTableWithNull(tableName);
        TddlRuleManager tddlRuleManager = optimizerContext.getRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(tableName);
        TableGroupConfig tgConfig = optimizerContext.getTableGroupInfoManager()
            .getTableGroupConfigById(partInfo.getTableGroupId());
        // target table info
        String targetTableName = sqlExchange.getTableName().toString();
        String partName = sqlExchange.getPartitions().get(0).toString();
        boolean validation = sqlExchange.isValidation();
        TableMeta targetTableMeta = optimizerContext.getLatestSchemaManager().getTableWithNull(targetTableName);
        PartitionInfo targetPartInfo = partitionInfoManager.getPartitionInfo(targetTableName);
        TableGroupConfig targetTgConfig = optimizerContext.getTableGroupInfoManager()
            .getTableGroupConfigById(targetPartInfo.getTableGroupId());
        // 1. check partition
        PartitionGroupRecord partitionGroupRecord = tgConfig.getPartitionGroupRecords().stream()
            .filter(o -> partName.equalsIgnoreCase(o.partition_name)).findFirst().orElse(null);
        if (partitionGroupRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + partName + " is not exists this current table group");
        }
        PartitionGroupRecord targetPartitionGroupRecord = targetTgConfig.getPartitionGroupRecords().stream()
            .filter(o -> partName.equalsIgnoreCase(o.partition_name)).findFirst().orElse(null);
        boolean needAddPartition = targetPartitionGroupRecord == null;
        ExecutableDdlJob firstJob = null;
        // 2. add partition to target table
        if (needAddPartition) {
            String targetTableGroupName = targetTgConfig != null ? targetTgConfig.getTableGroupRecord().tg_name : "";
            PartitionSpec partitionSpec = partInfo.getPartitionBy().getPartitionByPartName(partName);
            String logicalSql =
                String.format("alter tablegroup %s add partition (%s)", targetTableGroupName, partitionSpec);
            RelNode plan = Planner.getInstance().plan(logicalSql, executionContext).getPlan();

            LogicalAlterTableGroupAddPartition logicalAlterTableGroupAddPartition =
                (LogicalAlterTableGroupAddPartition) plan;
            ExecutableDdlJob addPartitionJob = (ExecutableDdlJob) new LogicalAlterTableGroupAddPartitionHandler(repo)
                .buildDdlJob(logicalAlterTableGroupAddPartition, executionContext);
            firstJob = addPartitionJob;
        }
        // 3. validation
        if (validation) {
            // primary key range check
            // partition lower bound & upper bound check
        }
        // 4. move data to target table
        ExecutableDdlJob moveDataJob = new MoveOSSDataJobFactory(
            schemaName, tableName, schemaName, targetTableName,
            tableMeta.getEngine(), targetTableMeta.getEngine(), ImmutableList.of(partName)).create(true);
        if (firstJob != null) {
            firstJob.appendJob2(moveDataJob);
        } else {
            firstJob = moveDataJob;
        }
        return firstJob;
    }

    private DdlJob buildAlterTablePurgeBeforeTimeStamp(BaseDdlOperation logicalDdlPlan,
                                                       ExecutionContext executionContext,
                                                       LogicalAlterTable logicalAlterTable) {
        final TableMeta tableMeta =
            OptimizerContext
                .getContext(logicalDdlPlan.getSchemaName())
                .getLatestSchemaManager()
                .getTable(logicalDdlPlan.getTableName());
        if (tableMeta != null && !Engine.isFileStore(tableMeta.getEngine())) {
            throw GeneralUtil.nestedException(MessageFormat.format(
                "Only support alter table purge before timestamp for file-store table. The table engine of {0} is {1}",
                tableMeta.getTableName(), tableMeta.getEngine()));
        }
        return new AlterTablePurgeBeforeTimeStampJobFactory(
            logicalDdlPlan.getSchemaName(),
            logicalDdlPlan.getTableName(),
            logicalAlterTable.getAlterTablePreparedData(),
            executionContext
        ).create();
    }

    private DdlJob buildAlterTableAsOfTimeStamp(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext,
                                                LogicalAlterTable logicalAlterTable) {
        final TableMeta tableMeta =
            OptimizerContext
                .getContext(logicalDdlPlan.getSchemaName())
                .getLatestSchemaManager()
                .getTable(logicalDdlPlan.getTableName());
        if (tableMeta != null && !Engine.isFileStore(tableMeta.getEngine())) {
            throw GeneralUtil.nestedException(MessageFormat.format(
                "Only support alter table as of timestamp for file-store table. The table engine of {0} is {1}",
                tableMeta.getTableName(), tableMeta.getEngine()));
        }
        return new AlterTableAsOfTimeStampJobFactory(
            logicalDdlPlan.getSchemaName(),
            logicalDdlPlan.getTableName(),
            logicalAlterTable.getAlterTablePreparedData(),
            executionContext
        ).create();
    }

    private DdlJob buildAlterTableEngineJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext,
                                            LogicalAlterTable logicalAlterTable) {
        SqlIdentifier sqlIdentifier = logicalAlterTable.getSqlAlterTable().getTableOptions().getEngine();
        Engine targetEngine = Engine.of(sqlIdentifier.toString());
        final TableMeta tableMeta =
            OptimizerContext
                .getContext(logicalDdlPlan.getSchemaName())
                .getLatestSchemaManager()
                .getTable(logicalDdlPlan.getTableName());
        Engine sourceEngine = tableMeta.getEngine();
        switch (sourceEngine) {
        case INNODB: {
            switch (targetEngine) {
            case S3:
            case OSS:
            case LOCAL_DISK:
                // innodb -> file store
                return new LogicalAlterTableEngineHandler(repo, sourceEngine, targetEngine)
                    .buildDdlJob(logicalDdlPlan, executionContext);
            default:
                // default
                return buildAlterTableJob(logicalAlterTable, executionContext);
            }
        }
        case S3:
        case OSS:
        case LOCAL_DISK: {
            switch (targetEngine) {
            case INNODB:
                // file store -> innodb
                return new LogicalAlterTableEngineHandler(repo, sourceEngine, targetEngine)
                    .buildDdlJob(logicalDdlPlan, executionContext);
            default:
                // file store -> file store
                if (targetEngine == sourceEngine) {
                    throw GeneralUtil.nestedException(MessageFormat.format(
                        "The Table {0} is already {1}.",
                        logicalDdlPlan.getTableName(), targetEngine));
                }
                return new LogicalAlterTableEngineHandler(repo, sourceEngine, targetEngine)
                    .buildDdlJob(logicalDdlPlan, executionContext);
            }
        }
        default:
            return buildAlterTableJob(logicalAlterTable, executionContext);
        }
    }

    private DdlJob buildDropFileJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext,
                                    LogicalAlterTable logicalAlterTable) {
        // for alter table drop file ...
        final TableMeta tableMeta =
            OptimizerContext
                .getContext(logicalDdlPlan.getSchemaName())
                .getLatestSchemaManager()
                .getTable(logicalDdlPlan.getTableName());
        if (tableMeta != null && !Engine.isFileStore(tableMeta.getEngine())) {
            throw GeneralUtil.nestedException(MessageFormat.format(
                "Only support drop file from file-store table. The table engine of {0} is {1}",
                tableMeta.getTableName(), tableMeta.getEngine()));
        }
        return new AlterTableDropOssFileJobFactory(
            logicalDdlPlan.getSchemaName(),
            logicalDdlPlan.getTableName(),
            logicalAlterTable.getAlterTablePreparedData(),
            executionContext
        ).create();
    }

    private DdlJob buildAlterTableOnlineModifyColumnJob(LogicalAlterTable logicalAlterTable,
                                                        ExecutionContext executionContext) {
        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();

        ExecutorContext executorContext = ExecutorContext.getContext(alterTablePreparedData.getSchemaName());
        if (!executorContext.getStorageInfoManager().supportsAlterType()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "DN do not support online modify column");
        }

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        if (!alterTablePreparedData.isNewColumnNullable() && (executionContext.getSqlMode() == null
            || !TStringUtil.containsIgnoreCase(executionContext.getSqlMode(), "STRICT_TRANS_TABLES"))) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Do not support modify/change to not nullable column in non strict mode");
        }

        // Get old column type from show create table, wish we could rename column directly in DN someday
        if (alterTablePreparedData.isOnlineModifyColumn() || alterTablePreparedData.isOnlineChangeColumn()) {
            String targetTableName = alterTablePreparedData.getTableName();
            LogicalShowCreateTableHandler logicalShowCreateTablesHandler = new LogicalShowCreateTableHandler(repo);

            SqlShowCreateTable sqlShowCreateTable =
                SqlShowCreateTable.create(SqlParserPos.ZERO, new SqlIdentifier(targetTableName, SqlParserPos.ZERO));
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
            ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
            LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();

            Cursor showCreateTableCursor =
                logicalShowCreateTablesHandler.handle(logicalShowCreateTable, executionContext);

            String createTableSql = null;

            Row showCreateResult = showCreateTableCursor.next();
            if (showCreateResult != null && showCreateResult.getString(1) != null) {
                createTableSql = showCreateResult.getString(1);
            } else {
                GeneralUtil.nestedException("Get reference table architecture failed.");
            }

            SQLCreateTableStatement createTableStmt =
                (SQLCreateTableStatement) FastsqlUtils.parseSql(createTableSql).get(0);

            // Do not support if table contains fulltext index on any columns
            if (createTableStmt.existFullTextIndex()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Do not support online modify column on table with fulltext index");
            }

            SQLColumnDefinition colDef = createTableStmt.getColumn(alterTablePreparedData.getModifyColumnName());

            final boolean forceTypeConversion =
                executionContext.getParamManager().getBoolean(ConnectionParams.OMC_FORCE_TYPE_CONVERSION);
            if (!forceTypeConversion && TableColumnUtils.isUnsupportedType(colDef.getDataType().getName())) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Converting from %s is not supported", colDef.getDataType().getName()));
            }

            alterTablePreparedData.setModifyColumnType(
                TableColumnUtils.getDataDefFromColumnDef(alterTablePreparedData.getModifyColumnName(),
                    colDef.toString()));
        }

        List<PhysicalPlanData> gsiPhysicalPlanData = new ArrayList<>();
        if (alterTableWithGsiPreparedData != null
            && alterTableWithGsiPreparedData.getGlobalIndexPreparedData() != null) {
            for (AlterTablePreparedData gsiTablePreparedData : alterTableWithGsiPreparedData.getGlobalIndexPreparedData()) {
                DdlPhyPlanBuilder builder =
                    AlterTableBuilder.create(logicalAlterTable.relDdl, gsiTablePreparedData, executionContext).build();
                PhysicalPlanData phyPlanData = builder.genPhysicalPlanData();
                phyPlanData.setSequence(null);
                gsiPhysicalPlanData.add(phyPlanData);
            }
        }

        if (alterTableWithGsiPreparedData != null
            && alterTableWithGsiPreparedData.getClusteredIndexPrepareData() != null) {
            for (AlterTablePreparedData gsiTablePreparedData : alterTableWithGsiPreparedData.getClusteredIndexPrepareData()) {
                DdlPhyPlanBuilder builder =
                    AlterTableBuilder.create(logicalAlterTable.relDdl, gsiTablePreparedData, executionContext).build();
                PhysicalPlanData phyPlanData = builder.genPhysicalPlanData();
                phyPlanData.setSequence(null);
                gsiPhysicalPlanData.add(phyPlanData);
            }
        }

        ExecutableDdlJob alterTableJob =
            new AlterTableOnlineModifyColumnJobFactory(physicalPlanData, gsiPhysicalPlanData, alterTablePreparedData,
                alterTableWithGsiPreparedData, logicalAlterTable, executionContext).create();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(alterTablePreparedData.getTableName(),
            alterTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(alterTablePreparedData.getSchemaName(), tableVersions);

        alterTableJob.addTask(validateTableVersionTask);
        alterTableJob.addTaskRelationship(validateTableVersionTask, alterTableJob.getHead());

        return alterTableJob;
    }

    private DdlJob buildAlterTableJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();
        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTable);
        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        AlterTableJobFactory alterTableJobFactory =
            new AlterTableJobFactory(physicalPlanData, alterTablePreparedData, logicalAlterTable, executionContext);

        TableMeta tableMeta =
            OptimizerContext.getContext(alterTablePreparedData.getSchemaName()).getLatestSchemaManager()
                .getTable(alterTablePreparedData.getTableName());
        if (tableMeta.isGsi()) {
            alterTableJobFactory.withAlterGsi(true, tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        }

        ExecutableDdlJob alterTableJob = alterTableJobFactory.create();

        Map<String, Long> tableVersions = new HashMap<>();

        // Apply local index modification to clustered-index table
        AlterTableWithGsiPreparedData gsiData = logicalAlterTable.getAlterTableWithGsiPreparedData();
        if (gsiData != null && !logicalAlterTable.getAlterTablePreparedData().isOnlineModifyColumnIndexTask()) {
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
                clusteredTable.setColumnAfterAnother(new ArrayList<>());
                clusteredTable.setIsGsi(true);

                DdlPhyPlanBuilder builder =
                    AlterTableBuilder.create(logicalAlterTable.relDdl, clusteredTable, executionContext).build();

                PhysicalPlanData clusterIndexPlan = builder.genPhysicalPlanData();
                clusterIndexPlan.setSequence(null);

                AlterTableJobFactory jobFactory =
                    new AlterTableJobFactory(clusterIndexPlan, clusteredTable, logicalAlterTable, executionContext);
                jobFactory.validateExistence(false);
                jobFactory.withAlterGsi(true, alterTablePreparedData.getTableName());

                ExecutableDdlJob clusterIndexJob = jobFactory.create();
                alterTableJob.appendJob(clusterIndexJob);
            }
        }

        tableVersions.put(alterTablePreparedData.getTableName(),
            alterTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(alterTablePreparedData.getSchemaName(), tableVersions);

        alterTableJob.addTask(validateTableVersionTask);
        alterTableJob.addTaskRelationship(validateTableVersionTask, alterTableJob.getHead());

        return alterTableJob;
    }

    private DdlJob buildRepartitionJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        initPrimaryTableDefinition(logicalAlterTable, executionContext);

        SqlAlterTablePartitionKey ast = (SqlAlterTablePartitionKey) logicalAlterTable.relDdl.sqlNode;

        //validate
        RepartitionValidator.validate(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getDbPartitionBy(),
            ast.isBroadcast(),
            ast.isSingle(),
            false
        );

        logicalAlterTable.prepareData();

        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTable.getTableName(), logicalAlterTable.getSchemaName());

        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();

        CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getCreateIndexWithGsiPreparedData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            createIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        logicalAlterTable.prepareLocalIndexData();
        RepartitionPrepareData repartitionPrepareData = logicalAlterTable.getRepartitionPrepareData();
        globalIndexPreparedData.setRepartitionPrepareData(repartitionPrepareData);

        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(
            logicalAlterTable.relDdl,
            globalIndexPreparedData,
            executionContext
        ).build();

        // GSI table prepareData
        logicalAlterTable.prepareRepartitionData(globalIndexPreparedData.getIndexTableRule());

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

        return new RepartitionJobFactory(
            globalIndexPreparedData,
            repartitionPrepareData,
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

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(globalIndexPreparedData.getSchemaName(), tableVersions);
        // global index
        ExecutableDdlJob gsiJob =
            CreatePartitionGsiJobFactory.create(logicalAlterTable.relDdl, globalIndexPreparedData, executionContext);

        if (globalIndexPreparedData.isNeedToGetTableGroupLock()) {
            return gsiJob;
        }
        gsiJob.addTask(validateTableVersionTask);
        gsiJob.addTaskRelationship(validateTableVersionTask, gsiJob.getHead());

        // local index
        ExecutableDdlJob localIndexJob = CreateIndexJobFactory.createLocalIndex(
            logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
            indexPreparedData.getLocalIndexPreparedDataList(),
            executionContext
        );
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        gsiJob.addSequentialTasksAfter(gsiJob.getTail(), Lists.newArrayList(new StatisticSampleTask(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getIndexTableName()
        )));
        return gsiJob;
    }

    private DdlJob buildDropGsiJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
            dropIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(alterTableWithGsiPreparedData.getTableName(),
            alterTableWithGsiPreparedData.getTableVersion());
        tableVersions.put(dropGlobalIndexPreparedData.getIndexTableName(),
            dropGlobalIndexPreparedData.getTableVersion());

        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(dropGlobalIndexPreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob baseJob = new ExecutableDdlJob();

        ExecutableDdlJob ddlJob = DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, true);
        if (ddlJob != null) {
            ddlJob.addTask(validateTableVersionTask);
            ddlJob.addTaskRelationship(validateTableVersionTask, ddlJob.getHead());
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

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(alterTablePreparedData.getTableName(),
            alterTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(alterTablePreparedData.getSchemaName(), tableVersions);

        alterTableJob.addTask(validateTableVersionTask);
        alterTableJob.addTaskRelationship(validateTableVersionTask, alterTableJob.getHead());

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
                targetTableName = GsiUtils.generateRandomGsiName(primaryTableName);
            }
            ast.setLogicalSecondaryTableName(targetTableName);

            List<SqlIndexDefinition> gsiList = new ArrayList<>();
            SqlIndexDefinition repartitionGsi =
                AlterRepartitionUtils.initIndexInfo(
                    primaryTableInfo.getValue(),
                    ast,
                    primaryTableInfo.getKey()
                );
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

}
