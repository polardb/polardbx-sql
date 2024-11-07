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
import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterPartitionTableTruncatePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableAddLogicalForeignKeyJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGeneratedColumnJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableOnlineModifyColumnJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableWithFileStoreJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.AlterGsiVisibilityJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RebuildTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RenameGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RepartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.CreateColumnarIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.DropColumnarIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTableAsOfTimeStampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTableDropOssFileJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTablePurgeBeforeTimeStampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.MoveOSSDataJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiStatisticsInfoSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.StatisticSampleTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ForeignKeyValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableAllocateLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableCleanupExpiredDataHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableEngineHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableExpireLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableModifyTtlHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableRemoveLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableRemoveTtlHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterTableRepartitionLocalPartitionHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RebuildTablePrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.RenameGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_KEY_NAME;
import static com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils.generateSqlPartitionKey;
import static com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils.getShardColumnsFromPartitionBy;

public class LogicalAlterTableHandler extends LogicalCommonDdlHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterTableHandler.class);

    public LogicalAlterTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final Long versionId = DdlUtils.generateVersionId(executionContext);
        return doBuildDdlJob(logicalDdlPlan, versionId, executionContext);
    }

    protected DdlJob doBuildDdlJob(BaseDdlOperation logicalDdlPlan,
                                   Long ddlVersionId,
                                   ExecutionContext executionContext) {
        LogicalAlterTable logicalAlterTable = (LogicalAlterTable) logicalDdlPlan;

        if (logicalAlterTable.isAllocateLocalPartition()) {
            return new LogicalAlterTableAllocateLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isExpireLocalPartition()) {
            return new LogicalAlterTableExpireLocalPartitionHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isCleanupExpiredData()) {
            return new LogicalAlterTableCleanupExpiredDataHandler(repo)
                .buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isModifyTtlOptions()) {
            return new LogicalAlterTableModifyTtlHandler(repo).buildDdlJob(logicalDdlPlan, executionContext);
        }

        if (logicalAlterTable.isRemoveTtlOptions()) {
            return new LogicalAlterTableRemoveTtlHandler(repo).buildDdlJob(logicalDdlPlan, executionContext);
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
            || logicalAlterTable.isCreateClusteredIndex()
            || logicalAlterTable.isCreateCci()) {
            initPrimaryTableDefinition(logicalAlterTable, executionContext);
        }

        logicalAlterTable.validateColumnar();

        if (logicalAlterTable.validateOnlineModify(executionContext, false)
            || logicalAlterTable.autoConvertToOmc(executionContext)) {
            return buildRebuildTableJob(logicalAlterTable, true, executionContext, ddlVersionId);
        }

        logicalAlterTable = rewriteExpressionIndex(logicalAlterTable, executionContext);
        logicalAlterTable.prepareData();
        logicalAlterTable.setDdlVersionId(ddlVersionId);

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
        } else if (logicalAlterTable.isCreateCci()) {
            return buildCreateCciJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isCreateGsi()
            || logicalAlterTable.isCreateClusteredIndex()) {
            return buildCreateGsiJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isDropCci()) {
            return buildDropCciJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isDropGsi()) {
            return buildDropGsiJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isAlterTableRenameGsi()) {
            return buildRenameGsiJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isTruncatePartition()) {
            return buildAlterTableTruncatePartitionJob(logicalAlterTable, executionContext);
        } else if (logicalAlterTable.isAlterIndexVisibility()) {
            return buildAlterIndexVisibilityJob(logicalAlterTable, executionContext);
        } else {
            if (logicalAlterTable.getAlterTablePreparedData().isNeedRepartition()) {
                // for drop primary key, add primary
                return buildRebuildTableJob(logicalAlterTable, false, executionContext, ddlVersionId);
            } else {
                return buildAlterTableJob(logicalAlterTable, executionContext);
            }
        }
    }

    private LogicalAlterTable rewriteExpressionIndex(LogicalAlterTable logicalAlterTable,
                                                     ExecutionContext executionContext) {
        // We rewrite expression in executor because in parser we do not know other column names in the table
        if (logicalAlterTable.getSqlAlterTable().getOriginalSql() == null || InstanceVersion.isMYSQL80()) {
            // Could be alter table after rewrite
            return logicalAlterTable;
        }

        String schemaName = logicalAlterTable.getSchemaName();
        String tableName = logicalAlterTable.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        Set<String> tableColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableColumns.addAll(
            tableMeta.getPhysicalColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        SQLAlterTableStatement sqlAlterTableStatement;
        try {
            sqlAlterTableStatement =
                (SQLAlterTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                    logicalAlterTable.getSqlAlterTable().getOriginalSql(),
                    JdbcConstants.MYSQL).get(0);
        } catch (Throwable e) {
            logger.error("fail to parse sql " + logicalAlterTable.getSqlAlterTable().getOriginalSql(), e);
            return logicalAlterTable;
        }

        Set<String> exprIndexNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // check if we build unique index on virtual/stored generated column
        for (SQLAlterTableItem item : sqlAlterTableStatement.getItems()) {
            if (item instanceof SQLAlterTableAddIndex) {
                SQLIndexDefinition indexDefinition = ((SQLAlterTableAddIndex) item).getIndexDefinition();
                if (indexDefinition.isUnique() && indexDefinition.getColumns().stream()
                    .anyMatch(c -> GeneratedColumnUtil.isGeneratedColumn(c, tableColumns, tableMeta))) {
                    if (indexDefinition.isUnique() && !executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "create unique index on VIRTUAL/STORED generated column is not enabled");
                    }
                }
            } else if (item instanceof SQLAlterTableAddConstraint
                && ((SQLAlterTableAddConstraint) item).getConstraint() instanceof MySqlUnique) {
                SQLIndexDefinition indexDefinition =
                    ((MySqlUnique) ((SQLAlterTableAddConstraint) item).getConstraint()).getIndexDefinition();
                if (indexDefinition.getColumns().stream()
                    .anyMatch(c -> GeneratedColumnUtil.isGeneratedColumn(c, tableColumns, tableMeta))) {
                    if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                        if (!executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "create unique index on VIRTUAL/STORED generated column is not enabled");
                        }
                    }
                }
            }
        }

        // rewrite expression index
        //如果是alter table add column, add index 情况，table columns 需包含所有列，而不是已存在的列
        //主要判断是否是函数索引，排除 column(20) 这类索引情况，这种索引是普通索引
        Set<String> dstTableColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        dstTableColumns.addAll(tableColumns);
        for (SQLAlterTableItem item : sqlAlterTableStatement.getItems()) {
            if (item instanceof SQLAlterTableAddColumn) {
                List<String> columnNames =
                    ((SQLAlterTableAddColumn) item).getColumns().stream()
                        .map(definition -> SQLUtils.normalizeNoTrim(definition.getColumnName()))
                        .collect(Collectors.toList());
                dstTableColumns.addAll(columnNames);
            } else if (item instanceof MySqlAlterTableChangeColumn) {
                String columnName = SQLUtils.normalizeNoTrim(
                    ((MySqlAlterTableChangeColumn) item).getNewColumnDefinition().getColumnName());
                dstTableColumns.add(columnName);
            }
        }
        for (SQLAlterTableItem item : sqlAlterTableStatement.getItems()) {
            if (item instanceof SQLAlterTableAddIndex) {
                SQLIndexDefinition indexDefinition = ((SQLAlterTableAddIndex) item).getIndexDefinition();
                if (indexDefinition.getColumns().stream()
                    .anyMatch(c -> GeneratedColumnUtil.isExpression(c, dstTableColumns))) {
                    if (indexDefinition.isUnique() && !executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "create unique expression index is not enabled");
                    }

                    exprIndexNames.add(SQLUtils.normalizeNoTrim(indexDefinition.getName().getSimpleName()));
                }
            } else if (item instanceof SQLAlterTableAddConstraint
                && ((SQLAlterTableAddConstraint) item).getConstraint() instanceof MySqlUnique) {
                SQLIndexDefinition indexDefinition =
                    ((MySqlUnique) ((SQLAlterTableAddConstraint) item).getConstraint()).getIndexDefinition();
                if (indexDefinition.getColumns().stream()
                    .anyMatch(c -> GeneratedColumnUtil.isExpression(c, dstTableColumns))) {
                    if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "create unique expression index is not enabled");
                    }
                    exprIndexNames.add(SQLUtils.normalizeNoTrim(indexDefinition.getName().getSimpleName()));
                }
            }
        }

        if (exprIndexNames.isEmpty()) {
            return logicalAlterTable;
        }

        if (!InstanceVersion.isMYSQL80() && !executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CREATE_EXPRESSION_INDEX)) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "create expression index is not enabled");
        }

        if (logicalAlterTable.isCreateGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "add global expression index");
        }

        List<SQLAlterTableItem> rewrittenAlterTableItems = new ArrayList<>();
        Iterator<SQLAlterTableItem> iterator = sqlAlterTableStatement.getItems().iterator();

        while (iterator.hasNext()) {
            Object item = iterator.next();
            if (item instanceof SQLAlterTableAddIndex) {
                SQLIndexDefinition indexDefinition = ((SQLAlterTableAddIndex) item).getIndexDefinition();
                String indexName = SQLUtils.normalizeNoTrim(indexDefinition.getName().getSimpleName());
                if (exprIndexNames.contains(indexName)) {
                    rewrittenAlterTableItems.addAll(
                        GeneratedColumnUtil.rewriteExprIndex(tableMeta, indexDefinition,
                            executionContext));
                }
                iterator.remove();
            } else if (item instanceof SQLAlterTableAddConstraint
                && ((SQLAlterTableAddConstraint) item).getConstraint() instanceof MySqlUnique) {
                SQLIndexDefinition indexDefinition =
                    ((MySqlUnique) ((SQLAlterTableAddConstraint) item).getConstraint()).getIndexDefinition();
                String indexName = SQLUtils.normalizeNoTrim(indexDefinition.getName().getSimpleName());
                if (exprIndexNames.contains(indexName)) {
                    rewrittenAlterTableItems.addAll(
                        GeneratedColumnUtil.rewriteExprIndex(tableMeta, indexDefinition,
                            executionContext));
                }
                iterator.remove();
            }
        }

        for (SQLAlterTableItem item : rewrittenAlterTableItems) {
            sqlAlterTableStatement.addItem(item);
        }

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), executionContext);
        sqlAlterTableStatement.accept(visitor);
        SqlAlterTable sqlAlterTable = (SqlAlterTable) visitor.getSqlNode();

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(schemaName);
        ExecutionPlan executionPlan = Planner.getInstance().getPlan(sqlAlterTable, plannerContext);
        LogicalAlterTable result = (LogicalAlterTable) executionPlan.getPlan();
        result.setRewrittenAlterSql(true);
        return result;
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
            case EXTERNAL_DISK:
            case NFS:
            case ABS:
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
        case LOCAL_DISK:
        case EXTERNAL_DISK:
        case ABS:
        case NFS: {
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

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String logicalTableName = logicalDdlPlan.getTableName();

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        if (sqlAlterTable.getTableOptions() != null && sqlAlterTable.getTableOptions().getComment() != null) {
            TableValidator.validateTableComment(logicalTableName,
                sqlAlterTable.getTableOptions().getComment().toValue());
        }

        final boolean checkForeignKey = executionContext.foreignKeyChecks();
        // check collation in column definitions.
        if (sqlAlterTable.getAlters() != null) {
            for (SqlAlterSpecification spec : sqlAlterTable.getAlters()) {
                if (spec instanceof SqlModifyColumn) {
                    TableValidator.validateCollationImplemented((SqlModifyColumn) spec);
                } else if (spec instanceof SqlAddForeignKey) {
                    String refTableName =
                        ((SqlAddForeignKey) spec).getReferenceDefinition().getTableName().getLastName();
                    boolean referencedTableExists =
                        TableValidator.checkIfTableExists(logicalDdlPlan.getSchemaName(), refTableName);
                    if (!referencedTableExists && checkForeignKey) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, logicalDdlPlan.getSchemaName(),
                            refTableName);
                    }
                }
            }
        }

        String schemaName = logicalDdlPlan.getSchemaName();

        TableValidator.validateTableName(logicalTableName);

        ColumnValidator.validateColumnLimits(logicalDdlPlan.getSchemaName(), logicalTableName, sqlAlterTable);

        IndexValidator.validateIndexNameLengths(sqlAlterTable);

        ConstraintValidator.validateConstraintLimits(sqlAlterTable);

        ForeignKeyValidator.validateFkConstraints(sqlAlterTable, schemaName, logicalTableName, executionContext);

        TableValidator.validateTruncatePartition(logicalDdlPlan.getSchemaName(), logicalTableName, sqlAlterTable,
            executionContext);

        return false;
    }

    private DdlJob buildAlterTableJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        // Need Refractor, actually we should not let it play in this way.
        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();
        AlterTableWithGsiPreparedData gsiData = logicalAlterTable.getAlterTableWithGsiPreparedData();

        ExecutableDdlJob ddlJob = null;
        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        if (executionContext.getDdlContext().getDdlStmt().contains(ForeignKeyUtils.PARTITION_FK_SUB_JOB)) {
            executionContext.getDdlContext().setFkRepartition(true);
        }

        if (logicalAlterTable.getAlterTableWithFileStorePreparedData() != null) {
            // 存在 oss 表
            AlterTablePreparedData alterTableFileStorePreparedData =
                logicalAlterTable.getAlterTableWithFileStorePreparedData();

            DdlPhyPlanBuilder alterTableFileStoreBuilder =
                AlterTableBuilder.create(logicalAlterTable.relDdl, alterTableFileStorePreparedData, executionContext)
                    .build();
            PhysicalPlanData physicalPlanFileStoreData = alterTableFileStoreBuilder.genPhysicalPlanData();
            AlterTableJobFactory alterTableJobFactory =
                new AlterTableWithFileStoreJobFactory(physicalPlanData, alterTablePreparedData,
                    physicalPlanFileStoreData, alterTableFileStorePreparedData,
                    logicalAlterTable, executionContext);

            ddlJob = alterTableJobFactory.create();
        } else if (logicalAlterTable.isAddGeneratedColumn() || logicalAlterTable.isDropGeneratedColumn()) {
            if (logicalAlterTable.isDropGeneratedColumn()) {
                List<String> colNames = new ArrayList<>();
                for (SqlAlterSpecification alter : logicalAlterTable.getSqlAlterTable().getAlters()) {
                    colNames.add(((SqlDropColumn) alter).getColName().getLastName());
                }

                // Get not nullable column, convert before drop
                String targetTableName = alterTablePreparedData.getTableName();
                LogicalShowCreateTableHandler logicalShowCreateTablesHandler =
                    new LogicalShowCreateTableHandler(repo);

                SqlShowCreateTable sqlShowCreateTable =
                    SqlShowCreateTable.create(SqlParserPos.ZERO,
                        new SqlIdentifier(targetTableName, SqlParserPos.ZERO));
                PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
                ExecutionPlan showCreateTablePlan =
                    Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
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

                Map<String, Pair<String, String>> notNullableGeneratedColumnDefs =
                    new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (String colName : colNames) {
                    SQLColumnDefinition colDef = createTableStmt.getColumn(colName);
                    if (colDef == null) {
                        for (SQLColumnDefinition columnDefinition : createTableStmt.getColumnDefinitions()) {
                            String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getColumnName());
                            if (columnName.equalsIgnoreCase(colName)) {
                                colDef = columnDefinition;
                                break;
                            }
                        }
                    }

                    // Remove generated expression from colDef
                    colDef.setGeneratedAlawsAs(null);
                    colDef.setLogical(false);

                    if (colDef.getConstraints().stream().anyMatch(c -> c instanceof SQLNotNullConstraint)) {
                        notNullableGeneratedColumnDefs.put(colName,
                            new Pair<>(TableColumnUtils.getDataDefFromColumnDefWithoutNullable(colDef),
                                TableColumnUtils.getDataDefFromColumnDef(colDef)));
                    }
                }
                alterTablePreparedData.setNotNullableGeneratedColumnDefs(notNullableGeneratedColumnDefs);
            } else {
                // Validate expression is legal
                for (Map.Entry<String, String> entry : alterTablePreparedData.getSpecialDefaultValues()
                    .entrySet()) {
                    if (alterTablePreparedData.getSpecialDefaultValueFlags().get(entry.getKey())
                        .equals(ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN)) {
                        GeneratedColumnUtil.getSqlCallAndValidateFromExprWithTableName(
                            alterTablePreparedData.getSchemaName(), alterTablePreparedData.getTableName(),
                            entry.getValue(),
                            executionContext);
                    }
                }
            }

            AlterTableGeneratedColumnJobFactory jobFactory =
                new AlterTableGeneratedColumnJobFactory(physicalPlanData, alterTablePreparedData, logicalAlterTable,
                    executionContext);
            ddlJob = jobFactory.create();
        } else if (logicalAlterTable.isAddLogicalForeignKeyOnly()) {
            AlterTableAddLogicalForeignKeyJobFactory alterTableAddLogicalForeignKeyJobFactory =
                new AlterTableAddLogicalForeignKeyJobFactory(physicalPlanData, alterTablePreparedData,
                    logicalAlterTable, executionContext);
            ddlJob = alterTableAddLogicalForeignKeyJobFactory.create();
        } else {
            // 物理执行 ddl 的 pausedPolicy 设置为 PAUSED，避免自动调度
            executionContext.getDdlContext().setPausedPolicy(DdlState.PAUSED);

            ParamManager paramManager = executionContext.getParamManager();
            boolean supportTwoPhaseDdl = paramManager.getBoolean(ConnectionParams.ENABLE_DRDS_MULTI_PHASE_DDL);
            String finalStatus = paramManager.getString(ConnectionParams.TWO_PHASE_DDL_FINAL_STATUS);
            AlterTableJobFactory alterTableJobFactory =
                new AlterTableJobFactory(physicalPlanData, alterTablePreparedData, logicalAlterTable, executionContext);
            alterTableJobFactory.setSupportTwoPhaseDdl(supportTwoPhaseDdl);
            alterTableJobFactory.setFinalStatus(finalStatus);

            TableMeta tableMeta =
                OptimizerContext.getContext(alterTablePreparedData.getSchemaName()).getLatestSchemaManager()
                    .getTable(alterTablePreparedData.getTableName());
            if (tableMeta.isGsi()) {
                alterTableJobFactory.withAlterGsi(true, tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
            }
            if (tableMeta.isColumnar() || tableMeta.withColumnar()) {
                alterTablePreparedData.setColumnar(true);
            }

            ddlJob = alterTableJobFactory.create();
        }

        Map<String, Long> tableVersions = new HashMap<>();
        // Apply local index modification to clustered-index table
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
                    ddlJob.appendJob(localIndexJob);
                }
            }

            // drop local index 级联删除clustered gsi上的local index
            DropIndexWithGsiPreparedData dropIndexWithGsi = gsiData.getDropIndexWithGsiPreparedData();
            if (dropIndexWithGsi != null) {
                ExecutableDdlJob localIndexJob = DropIndexJobFactory.createDropLocalIndex(
                    logicalAlterTable.relDdl, logicalAlterTable.getNativeSqlNode(),
                    dropIndexWithGsi.getLocalIndexPreparedDataList(),
                    false,
                    executionContext);
                if (localIndexJob != null) {
                    ddlJob.appendJob(localIndexJob);
                }
            }

            // drop local index ---> rename index  级联删除 clustered gsi 上的 local index 时转成 rename
            RenameLocalIndexPreparedData renameIndexPreparedData = gsiData.getRenameLocalIndexPreparedData();
            if (renameIndexPreparedData != null) {
                DdlTask ddlTask = LogicalDropIndexHandler.genRenameLocalIndexTask(renameIndexPreparedData);
                ddlJob.appendTask(ddlTask);
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
                if (alter.isColumnar()) {
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
                ddlJob.appendJob(clusterIndexJob);
            }
        }

        if (CollectionUtils.isNotEmpty(alterTablePreparedData.getAlterDefaultColumns())) {
            String schemaName = physicalPlanData.getSchemaName();
            String logicalTableName = physicalPlanData.getLogicalTableName();

            List<String> colNames = alterTablePreparedData.getAlterDefaultNewColumns().isEmpty() ?
                alterTablePreparedData.getAlterDefaultColumns() : alterTablePreparedData.getAlterDefaultNewColumns();
            DdlTask endAlterColumnDefault = new AlterColumnDefaultTask(schemaName, logicalTableName,
                colNames, false);
            DdlTask endAlterColumnDefaultSyncTask = new TableSyncTask(schemaName, logicalTableName);

            // Append to tail
            ddlJob.addTask(endAlterColumnDefault);
            ddlJob.addTask(endAlterColumnDefaultSyncTask);
            ddlJob.addTaskRelationship(endAlterColumnDefault, endAlterColumnDefaultSyncTask);
            ddlJob.addTaskRelationship(ddlJob.getTail(), endAlterColumnDefault);
            ddlJob.labelAsTail(endAlterColumnDefaultSyncTask);
        }

        // update primary table version
        if (!alterTablePreparedData.isNeedRepartition()) {
            String schemaName = physicalPlanData.getSchemaName();
            String logicalTableName = physicalPlanData.getLogicalTableName();

            UpdateTablesVersionTask updateTablesVersionTask =
                new UpdateTablesVersionTask(schemaName,
                    Collections.singletonList(logicalTableName));
            TableSyncTask finalSyncTask = new TableSyncTask(schemaName, logicalTableName);

            ddlJob.appendTask(updateTablesVersionTask);
            ddlJob.addTaskRelationship(updateTablesVersionTask, finalSyncTask);
        }

        tableVersions.put(alterTablePreparedData.getTableName(),
            alterTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(alterTablePreparedData.getSchemaName(), tableVersions);

        ddlJob.addTask(validateTableVersionTask);
        ddlJob.addTaskRelationship(validateTableVersionTask, ddlJob.getHead());

        addRefreshArcViewSubJobIfNeed(ddlJob, logicalAlterTable, executionContext);

        return ddlJob;
    }

    private void validateGenerateColumn(LogicalAlterTable logicalAlterTable, List<String> columns,
                                        ExecutionContext ec) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalAlterTable, ec);

        final List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(primaryTableInfo.getKey(), JdbcConstants.MYSQL);
        final MySqlCreateTableStatement createTableStmt = (MySqlCreateTableStatement) statementList.get(0);

        final boolean enableWithGenCol =
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_OMC_WITH_GEN_COL);

        // Check if table has any generated column
        for (SQLColumnDefinition columnDefinition : createTableStmt.getColumnDefinitions()) {
            if (columnDefinition.getGeneratedAlawsAs() != null) {
                if (!enableWithGenCol) {
                    // For now, we do not allow OMC on table with generated column, because on mysql we can not add or
                    // drop column before a generated column using inplace algorithm
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Can not modify column [%s] on table with generated column [%s].",
                            columns, columnDefinition.getColumnName()));
                }

                String expr = columnDefinition.getGeneratedAlawsAs().toString();
                Set<String> refCols = GeneratedColumnUtil.getReferencedColumns(expr);

                for (String column : columns) {
                    if (refCols.contains(column)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Can not modify column [%s] referenced by a generated column [%s].",
                                column, columnDefinition.getColumnName()));
                    }
                }
            }
        }
    }

    private DdlJob buildRebuildTableJob(LogicalAlterTable logicalAlterTable, boolean omc, ExecutionContext ec,
                                        long versionId) {
        if (omc) {
            logicalAlterTable.prepareOnlineModifyColumn();
        }

        ec.getParamManager().getProps()
            .put(ConnectionProperties.ONLY_MANUAL_TABLEGROUP_ALLOW, Boolean.FALSE.toString());

        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();
        AlterTableWithGsiPreparedData gsiData = logicalAlterTable.getAlterTableWithGsiPreparedData();

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, ec).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        List<String> changedColumns = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(alterTablePreparedData.getUpdatedColumns())) {
            changedColumns.addAll(alterTablePreparedData.getUpdatedColumns());
        }

        if (GeneralUtil.isNotEmpty(alterTablePreparedData.getChangedColumns())) {
            changedColumns.addAll(alterTablePreparedData.getChangedColumns().stream().map(Pair::getKey).collect(
                Collectors.toList()));
        }

        // validate generate column
        validateGenerateColumn(logicalAlterTable, changedColumns, ec);

        if (ec.getDdlContext().getDdlStmt().contains(ForeignKeyUtils.PARTITION_FK_SUB_JOB)) {
            ec.getDdlContext().setFkRepartition(true);
        }

        AtomicBoolean primaryKeyNotChanged = new AtomicBoolean(false);
        RebuildTablePrepareData rebuildTablePrepareData = new RebuildTablePrepareData();
        initPrimaryTableDefinition4RebuildTable(logicalAlterTable, ec, gsiData, primaryKeyNotChanged,
            rebuildTablePrepareData);
        rebuildTablePrepareData.setVersionId(versionId);

        if (primaryKeyNotChanged.get()) {
            // alter table drop add primary key, but primary key is not changed
            return new TransientDdlJob();
        }

        logicalAlterTable.prepareModifySk(alterTablePreparedData.getNewTableMeta());

        List<CreateGlobalIndexPreparedData> globalIndexesPreparedData =
            logicalAlterTable.getCreateGlobalIndexesPreparedData();

        Map<String, Boolean> relatedTableGroupInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        globalIndexesPreparedData.forEach(o -> relatedTableGroupInfo.putAll(o.getRelatedTableGroupInfo()));

        Map<String, CreateGlobalIndexPreparedData> indexTablePreparedDataMap = new LinkedHashMap<>();

        List<Pair<CreateGlobalIndexPreparedData, PhysicalPlanData>> globalIndexPrepareData = new ArrayList<>();
        String targetPrimaryTableName = logicalAlterTable.getSqlAlterTable().getLogicalSecondaryTableName();
        for (CreateGlobalIndexPreparedData createGsiPreparedData : globalIndexesPreparedData) {
            createGsiPreparedData.getRelatedTableGroupInfo().putAll(relatedTableGroupInfo);
            DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
                logicalAlterTable.relDdl,
                createGsiPreparedData,
                indexTablePreparedDataMap,
                ec).build();

            if (omc && StringUtils.equalsIgnoreCase(targetPrimaryTableName,
                createGsiPreparedData.getIndexTableName())) {
                createGsiPreparedData.setOmcRebuildPrimaryTable(true);
            }
            indexTablePreparedDataMap.put(createGsiPreparedData.getIndexTableName(), createGsiPreparedData);
            globalIndexPrepareData.add(new Pair<>(createGsiPreparedData, builder.genPhysicalPlanData()));
        }

        RebuildTableJobFactory jobFactory = new RebuildTableJobFactory(
            logicalAlterTable.getSchemaName(),
            logicalAlterTable.getTableName(),
            omc ? targetPrimaryTableName : logicalAlterTable.getTableName(),
            globalIndexPrepareData,
            rebuildTablePrepareData,
            physicalPlanData,
            ec
        );
        jobFactory.setAlterDefaultColumns(alterTablePreparedData.getAlterDefaultColumns());
        jobFactory.setNeedDropImplicitKey(alterTablePreparedData.isNeedDropImplicitKey());
        jobFactory.setChangedColumns(changedColumns);

        ExecutableDdlJob ddlJob = jobFactory.create();
        Optional<CreateGlobalIndexPreparedData> opt = globalIndexesPreparedData.stream().filter(
            CreateGlobalIndexPreparedData::isNeedToGetTableGroupLock).findAny();
        if (opt.isPresent()) {
            //create tablegroup firstly
            addRefreshArcViewSubJobIfNeed(ddlJob, logicalAlterTable, ec);
            return ddlJob;
        }
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(alterTablePreparedData.getTableName(),
            alterTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(alterTablePreparedData.getSchemaName(), tableVersions);

        ddlJob.addTask(validateTableVersionTask);
        ddlJob.addTaskRelationship(validateTableVersionTask, ddlJob.getHead());

        addRefreshArcViewSubJobIfNeed(ddlJob, logicalAlterTable, ec);

        return ddlJob;
    }

    private DdlJob buildRepartitionJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
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

        // get foreign keys
        String schemaName = globalIndexPreparedData.getSchemaName();
        String tableName = globalIndexPreparedData.getPrimaryTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        logicalAlterTable.prepareForeignKeyData(tableMeta, ast);

        return new RepartitionJobFactory(
            globalIndexPreparedData,
            repartitionPrepareData,
            physicalPlanData,
            executionContext,
            logicalAlterTable.getCluster()
        ).create();
    }

    private DdlJob buildCreateCciJob(LogicalAlterTable logicalAlterTable,
                                     ExecutionContext executionContext) {
        final CreateGlobalIndexPreparedData cciPreparedData = logicalAlterTable
            .getAlterTableWithGsiPreparedData()
            .getCreateIndexWithGsiPreparedData()
            .getGlobalIndexPreparedData();

        ExecutableDdlJob cciJob = CreateColumnarIndexJobFactory.create4CreateCci(
            logicalAlterTable.relDdl,
            cciPreparedData,
            executionContext);

        return cciJob;
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
            globalIndexPreparedData.getPrimaryTableName()
        )));
        gsiJob.appendTask(
            new GsiStatisticsInfoSyncTask(
                globalIndexPreparedData.getSchemaName(),
                globalIndexPreparedData.getPrimaryTableName(),
                globalIndexPreparedData.getIndexTableName(),
                GsiStatisticsSyncAction.INSERT_RECORD,
                null)
        );
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
        baseJob.appendTask(
            new GsiStatisticsInfoSyncTask(
                dropGlobalIndexPreparedData.getSchemaName(),
                dropGlobalIndexPreparedData.getPrimaryTableName(),
                dropGlobalIndexPreparedData.getIndexTableName(),
                GsiStatisticsSyncAction.DELETE_RECORD,
                null)
        );

        // drop local index ---> rename index  级联删除 clustered gsi 上的 local index 时转成 rename
        RenameLocalIndexPreparedData renameIndexPreparedData =
            alterTableWithGsiPreparedData.getRenameLocalIndexPreparedData();
        if (renameIndexPreparedData != null) {
            DdlTask ddlTask = LogicalDropIndexHandler.genRenameLocalIndexTask(renameIndexPreparedData);
            baseJob.appendTask(ddlTask);
        }
        return baseJob;
    }

    public DdlJob buildDropCciJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        final AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        final DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData();
        final DropGlobalIndexPreparedData preparedData = dropIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        final Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        final ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob cciJob = DropColumnarIndexJobFactory.create(preparedData, executionContext, false, true);
        cciJob.addTask(validateTableVersionTask);
        cciJob.addTaskRelationship(validateTableVersionTask, cciJob.getHead());

        return cciJob;
    }

    private DdlJob buildRenameGsiJob(LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        RenameGlobalIndexPreparedData renameGlobalIndexPreparedData =
            alterTableWithGsiPreparedData.getRenameGlobalIndexPreparedData();
        RenameTablePreparedData renameTablePreparedData =
            renameGlobalIndexPreparedData.getIndexTablePreparedData();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(alterTableWithGsiPreparedData.getTableName(),
            alterTableWithGsiPreparedData.getTableVersion());
        tableVersions.put(renameTablePreparedData.getTableName(),
            renameTablePreparedData.getTableVersion());

        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(renameTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob baseJob = new ExecutableDdlJob();

        ExecutableDdlJob ddlJob =
            RenameGsiJobFactory.create(renameTablePreparedData, executionContext);
        ddlJob.addTask(validateTableVersionTask);
        ddlJob.addTaskRelationship(validateTableVersionTask, ddlJob.getHead());
        baseJob.appendJob2(ddlJob);

        DdlTask renameLocalIndexTask = RenameGsiJobFactory.createRenameLocalIndex(
            renameTablePreparedData.getSchemaName(),
            (SqlAlterTable) logicalAlterTable.getNativeSqlNode(),
            renameGlobalIndexPreparedData.getRenameLocalIndexPreparedData());
        if (renameLocalIndexTask != null) {
            baseJob.appendTask(renameLocalIndexTask);
        }

        baseJob.appendTask(
            new GsiStatisticsInfoSyncTask(
                renameGlobalIndexPreparedData.getSchemaName(),
                logicalAlterTable.getTableName(),
                renameTablePreparedData.getTableName(),
                GsiStatisticsSyncAction.RENAME_RECORD,
                renameTablePreparedData.getNewTableName())
        );
        return baseJob;
    }

    private DdlJob buildAlterIndexVisibilityJob(LogicalAlterTable logicalAlterTable,
                                                ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData preparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
        return new AlterGsiVisibilityJobFactory(preparedData, executionContext).create();
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

    private List<PhysicalPlanData> getGsiPhysicalPlanData(LogicalAlterTable logicalAlterTable,
                                                          ExecutionContext executionContext) {
        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTable.getAlterTableWithGsiPreparedData();
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

        return gsiPhysicalPlanData;
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

    private boolean primaryKeyChanged(List<String> oldPrimaryKeys, List<String> newPrimaryKeys) {
        List<String> newPrimaryKeysLow =
            newPrimaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList());

        return oldPrimaryKeys.equals(newPrimaryKeysLow);
    }

    private void checkAutoIncrement4Cdc(String sqlCreateTable) {
        Set<String> keyColumns = new TreeSet<>(String::compareToIgnoreCase);
        String autoIncrementColumn = null;

        final List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(sqlCreateTable, JdbcConstants.MYSQL);
        final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();

        while (it.hasNext()) {
            SQLTableElement tableElement = it.next();
            SQLIndexDefinition indexDefinition = null;
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                if (columnDefinition.isAutoIncrement()) {
                    if (autoIncrementColumn != null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "There can be only one auto_increment column and it must be defined as a key");
                    }
                    autoIncrementColumn = SQLUtils.normalizeNoTrim(columnDefinition.getColumnName());
                }
            } else if (tableElement instanceof MySqlPrimaryKey) {
                indexDefinition = ((MySqlPrimaryKey) tableElement).getIndexDefinition();
            } else if (tableElement instanceof MySqlKey) {
                indexDefinition = ((MySqlKey) tableElement).getIndexDefinition();
            } else if (tableElement instanceof MySqlTableIndex) {
                indexDefinition = ((MySqlTableIndex) tableElement).getIndexDefinition();
            }
            if (indexDefinition != null) {
                keyColumns.add(SQLUtils.normalizeNoTrim(indexDefinition.getColumns().get(0).toString()));
            }
        }

        if (autoIncrementColumn != null && !keyColumns.contains(autoIncrementColumn)) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "There can be only one auto_increment column and it must be defined as a key");
        }
    }

    private void initPrimaryTableDefinition4RebuildTable(BaseDdlOperation logicalDdlPlan,
                                                         ExecutionContext executionContext,
                                                         AlterTableWithGsiPreparedData gsiData,
                                                         AtomicBoolean primaryKeyNotChanged,
                                                         RebuildTablePrepareData rebuildTablePrepareData) {
        String schemaName = logicalDdlPlan.getSchemaName();
        String tableName = logicalDdlPlan.getTableName();
        List<String> oldPrimaryKeys = new ArrayList<>();
        AlterTablePreparedData alterTablePreparedData =
            ((LogicalAlterTable) logicalDdlPlan).getAlterTablePreparedData();
        Pair<String, SqlCreateTable> primaryTableInfo =
            genPrimaryTableInfoAfterModifyColumn(logicalDdlPlan, executionContext, oldPrimaryKeys,
                rebuildTablePrepareData);

        checkAutoIncrement4Cdc(primaryTableInfo.getKey());

        TableMeta newTableMeta = TableMetaParser.parse(tableName, primaryTableInfo.getValue());
        alterTablePreparedData.setNewTableMeta(newTableMeta);

        SqlAlterTable ast =
            (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();

        List<SqlIndexDefinition> gsiList;
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            gsiList = buildIndexDefinition4Auto(schemaName, tableName, logicalDdlPlan, executionContext,
                gsiData, alterTablePreparedData, rebuildTablePrepareData, primaryKeyNotChanged, oldPrimaryKeys,
                primaryTableInfo, ast);
        } else {
            gsiList = buildIndexDefinition4Drds(schemaName, tableName, executionContext,
                gsiData, alterTablePreparedData, rebuildTablePrepareData, primaryTableInfo, ast);
        }

        List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
            StringUtils.equalsIgnoreCase(e.getType(), "UNIQUE") ?
                new SqlAddUniqueIndex(SqlParserPos.ZERO, e.getIndexName(), e) :
                new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());

        ast.getSkAlters().addAll(sqlAddIndexList);
    }

    protected Pair<String, SqlCreateTable> genPrimaryTableInfoAfterModifyColumn(BaseDdlOperation logicalDdlPlan,
                                                                                ExecutionContext executionContext,
                                                                                List<String> oldPrimaryKeys,
                                                                                RebuildTablePrepareData rebuildTablePrepareData) {
        Map<String, String> srcVirtualColumnMap = rebuildTablePrepareData.getSrcVirtualColumnMap();
        Map<String, String> dstVirtualColumnMap = rebuildTablePrepareData.getDstVirtualColumnMap();
        Map<String, SQLColumnDefinition> srcColumnNewDef = rebuildTablePrepareData.getSrcColumnNewDef();
        Map<String, SQLColumnDefinition> dstColumnNewDef = rebuildTablePrepareData.getDstColumnNewDef();
        Map<String, String> backfillColumnMap = rebuildTablePrepareData.getBackfillColumnMap();
        List<String> modifyStringColumns = rebuildTablePrepareData.getModifyStringColumns();
        List<String> addNewColumns = rebuildTablePrepareData.getAddNewColumns();
        List<String> dropOldColumns = rebuildTablePrepareData.getDropColumns();

        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);
        oldPrimaryKeys.addAll(primaryTableInfo.getValue().getPrimaryKey().getColumns()
            .stream().map(e -> e.getColumnNameStr().toLowerCase()).collect(Collectors.toList()));

        final List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(primaryTableInfo.getKey(), JdbcConstants.MYSQL);
        final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        final List<SQLStatement> alterStatement =
            SQLUtils.parseStatementsWithDefaultFeatures(sqlAlterTable.getSourceSql(), JdbcConstants.MYSQL);
        final SQLAlterTableStatement alterStmt = (SQLAlterTableStatement) alterStatement.get(0);

        List<String> columnsDef = new ArrayList<>();
        Set<String> dropColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, SQLColumnDefinition> newColumnDefinitionMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, SQLColumnDefinition> newColumnDefinitionMap4AddColumn =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> newColumnAfter = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Boolean> newColumnFirst = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> newColumnAfter4AddColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Boolean> newColumnFirst4AddColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (SQLAlterTableItem sqlAlterTableItem : alterStmt.getItems()) {
            String colName;
            SQLName afterColumn;
            boolean first;
            SQLColumnDefinition newColumnDefinition;
            if (sqlAlterTableItem instanceof MySqlAlterTableModifyColumn) {
                MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) sqlAlterTableItem;
                newColumnDefinition = modifyColumn.getNewColumnDefinition();
                colName = SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName());
                afterColumn = modifyColumn.getAfterColumn();
                first = modifyColumn.isFirst();

                newColumnDefinitionMap.put(colName, newColumnDefinition);
                if (afterColumn != null) {
                    newColumnAfter.put(colName, SQLUtils.normalizeNoTrim(afterColumn.getSimpleName()));
                }
                if (first) {
                    newColumnFirst.put(colName, true);
                }
                columnsDef.add(colName.toLowerCase());
            } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                newColumnDefinition = changeColumn.getNewColumnDefinition();
                colName = SQLUtils.normalizeNoTrim(changeColumn.getColumnName().getSimpleName());
                afterColumn = changeColumn.getAfterColumn();
                first = changeColumn.isFirst();

                backfillColumnMap.put(colName.toLowerCase(),
                    SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName()).toLowerCase());

                newColumnDefinitionMap.put(colName, newColumnDefinition);
                if (afterColumn != null) {
                    newColumnAfter.put(colName, SQLUtils.normalizeNoTrim(afterColumn.getSimpleName()));
                }
                if (first) {
                    newColumnFirst.put(colName, true);
                }
                columnsDef.add(colName.toLowerCase());
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
                newColumnDefinition = addColumn.getColumns().get(0);
                colName = SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName());
                afterColumn = addColumn.getAfterColumn();
                first = addColumn.isFirst();

                newColumnDefinitionMap4AddColumn.put(colName, newColumnDefinition);
                if (afterColumn != null) {
                    newColumnAfter4AddColumn.put(colName, SQLUtils.normalizeNoTrim(afterColumn.getSimpleName()));
                }
                if (first) {
                    newColumnFirst4AddColumn.put(colName, true);
                }
                columnsDef.add(colName.toLowerCase());
                addNewColumns.add(colName.toLowerCase());
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                SQLAlterTableDropColumnItem dropColumnItem = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                colName = SQLUtils.normalizeNoTrim(dropColumnItem.getColumns().get(0).getSimpleName());

                dropColumns.add(colName.toLowerCase());
                dropOldColumns.add(colName.toLowerCase());
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropPrimaryKey) {
                final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
                while (it.hasNext()) {
                    SQLTableElement tableElement = it.next();
                    if (tableElement instanceof SQLColumnDefinition) {
                        final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                        if (null != columnDefinition.getConstraints()) {
                            final Iterator<SQLColumnConstraint> constraintIt =
                                columnDefinition.getConstraints().iterator();
                            while (constraintIt.hasNext()) {
                                final SQLColumnConstraint constraint = constraintIt.next();
                                if (constraint instanceof SQLColumnPrimaryKey) {
                                    constraintIt.remove();
                                } else if (constraint instanceof SQLColumnReference) {
                                    // remove foreign key
                                    constraintIt.remove();
                                }
                            }
                        }
                    } else if (tableElement instanceof MySqlPrimaryKey) {
                        it.remove();
                    }
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddConstraint) {
                SQLAlterTableAddConstraint addConstraint = (SQLAlterTableAddConstraint) sqlAlterTableItem;
                if (addConstraint.getConstraint() instanceof MySqlPrimaryKey) {
                    MySqlPrimaryKey addPrimaryKey = (MySqlPrimaryKey) addConstraint.getConstraint();
                    final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
                    Set<String> colNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    addPrimaryKey.getColumns()
                        .forEach(e -> colNameSet.add(SQLUtils.normalizeNoTrim(e.getExpr().toString())));

                    boolean hasImplicitKey = false;
                    while (it.hasNext()) {
                        SQLTableElement tableElement = it.next();
                        if (tableElement instanceof SQLColumnDefinition) {
                            final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                            if (colNameSet.contains(SQLUtils.normalizeNoTrim(columnDefinition.getColumnName()))) {
                                SQLNotNullConstraint sqlNotNullConstraint = new SQLNotNullConstraint();
                                columnDefinition.addConstraint(sqlNotNullConstraint);
                                if (columnDefinition.getDefaultExpr() instanceof SQLNullExpr) {
                                    columnDefinition.setDefaultExpr(null);
                                }
                                colNameSet.remove(SQLUtils.normalizeNoTrim(columnDefinition.getColumnName()));
                            }
                            if (com.alibaba.polardbx.druid.util.StringUtils.equalsIgnoreCase(IMPLICIT_COL_NAME,
                                SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName()))) {
                                hasImplicitKey = true;
                            }
                        } else if (tableElement instanceof MySqlPrimaryKey) {
                            it.remove();
                        }
                    }

                    if (!colNameSet.isEmpty()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Unknown column " + colNameSet);
                    }

                    // add new primary key
                    List<SQLSelectOrderByItem> colNames = addPrimaryKey.getColumns().stream()
                        .map(e -> new SQLSelectOrderByItem(new SQLIdentifierExpr(e.getExpr().toString())))
                        .collect(Collectors.toList());

                    MySqlPrimaryKey newPrimaryKey = new MySqlPrimaryKey();
                    SQLIndexDefinition indexDefinition = newPrimaryKey.getIndexDefinition();
                    indexDefinition.setKey(true);
                    indexDefinition.setType("PRIMARY");

                    indexDefinition.getColumns().addAll(colNames);

                    stmt.getTableElementList().add(newPrimaryKey);

                    // add local index for implicit key (auto increment)
                    if (hasImplicitKey) {
                        MySqlKey implicitKey = new MySqlKey();
                        implicitKey.setName(IMPLICIT_KEY_NAME);
                        SQLIndexDefinition indexDef = implicitKey.getIndexDefinition();
                        indexDef.setKey(true);
                        indexDef.getColumns().add(new SQLSelectOrderByItem(new SQLIdentifierExpr(IMPLICIT_COL_NAME)));

                        stmt.getTableElementList().add(implicitKey);
                    }
                }
            }
        }

        List<SQLTableElement> oldTableElementList = stmt.getTableElementList();
        List<SQLTableElement> newTableElementList = new ArrayList<>();
        Map<String, SQLColumnDefinition> newColumnDefinitionMapTmp = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        newColumnDefinitionMapTmp.putAll(newColumnDefinitionMap);

        // 先处理 change modify 以及 drop column
        for (SQLTableElement oldTableElement : oldTableElementList) {
            SQLTableElement tableElement = oldTableElement.clone();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                boolean autoIncrement = columnDefinition.isAutoIncrement();
                boolean isStringType = (columnDefinition.getDataType() instanceof SQLCharacterDataType);

                if (dropColumns.contains(columnName)) {
                    // ignore
                } else if (newColumnDefinitionMap.containsKey(columnName)) {
                    SQLColumnDefinition newColumnDefinition = newColumnDefinitionMap.get(columnName);
                    newTableElementList.add(newColumnDefinition);
                    if (!autoIncrement && !newColumnDefinition.isAutoIncrement()) {
                        // for checker prepare
                        String colNameStr = columnName.toLowerCase();
                        srcVirtualColumnMap.put(colNameStr, GsiUtils.generateRandomGsiName(colNameStr));
                        srcColumnNewDef.put(colNameStr, newColumnDefinition);

                        String newColNameStr = colNameStr;
                        if (MapUtils.isNotEmpty(backfillColumnMap) && backfillColumnMap.containsKey(colNameStr)) {
                            newColNameStr = backfillColumnMap.get(colNameStr);
                        }
                        dstVirtualColumnMap.put(newColNameStr, GsiUtils.generateRandomGsiName(newColNameStr));
                        dstColumnNewDef.put(newColNameStr, columnDefinition);
                    }
                    if (!newColumnAfter.containsKey(columnName) && !newColumnFirst.containsKey(columnName)) {
                        newColumnDefinitionMap.remove(columnName);
                        columnsDef.remove(columnName.toLowerCase());
                    }
                    if (isStringType) {
                        modifyStringColumns.add(columnName.toLowerCase());
                    }
                } else {
                    newTableElementList.add(tableElement);
                }
            } else if (tableElement instanceof MySqlKey) {
                final MySqlKey mySqlKey = (MySqlKey) tableElement;
                SQLIndexDefinition sqlIndexDefinition = mySqlKey.getIndexDefinition();
                List<SQLSelectOrderByItem> columns = sqlIndexDefinition.getColumns();

                final Iterator<SQLSelectOrderByItem> it = columns.iterator();
                while (it.hasNext()) {
                    SQLSelectOrderByItem column = it.next();
                    String columnName = SQLUtils.normalizeNoTrim(column.getExpr().toString());
                    if (dropColumns.contains(columnName)) {
                        it.remove();
                    } else if (newColumnDefinitionMapTmp.containsKey(columnName)) {
                        SQLColumnDefinition newColumnDefinition = newColumnDefinitionMapTmp.get(columnName);
                        column.setExpr(new SQLIdentifierExpr(newColumnDefinition.getColumnName()));
                    }
                }
                newTableElementList.add(tableElement);
            } else {
                newTableElementList.add(tableElement);
            }
        }

        for (String colName : columnsDef) {
            int idx = findIdxFromTableElementList(newTableElementList, SQLUtils.normalizeNoTrim(colName));
            // 处理add column
            if (newColumnDefinitionMap4AddColumn.containsKey(colName)) {
                if (idx != newTableElementList.size()) {
                    // add column 存在重复
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Duplicate column name '%s'", colName));
                } else if (!newColumnAfter4AddColumn.containsKey(colName)
                    && !newColumnFirst4AddColumn.containsKey(colName)) {
                    newTableElementList.add(newColumnDefinitionMap4AddColumn.get(colName));
                } else if (newColumnFirst4AddColumn.containsKey(colName)) {
                    // 处理 add column first 顺序
                    newTableElementList.add(0, newColumnDefinitionMap4AddColumn.get(colName));
                } else {
                    // 处理 add column after 顺序
                    String afterColName = newColumnAfter4AddColumn.get(colName);
                    int afterIdx = findIdxFromTableElementList(newTableElementList, afterColName);
                    if (afterIdx != newTableElementList.size()) {
                        newTableElementList.add(afterIdx + 1, newColumnDefinitionMap4AddColumn.get(colName));
                    }
                }
            } else {
                // remove old
                SQLColumnDefinition newColDef = newColumnDefinitionMap.get(colName);
                int removeItemIdx =
                    findIdxFromTableElementList(newTableElementList,
                        SQLUtils.normalizeNoTrim(newColDef.getColumnName()));
                if (removeItemIdx != newTableElementList.size()) {
                    newTableElementList.remove(removeItemIdx);
                }

                if (!newColumnAfter.containsKey(colName) && !newColumnFirst.containsKey(colName)) {
                    newTableElementList.add(newColDef);
                } else if (newColumnFirst.containsKey(colName)) {
                    // 处理 change modify first 的顺序
                    newTableElementList.add(0, newColDef);
                } else {
                    // 处理 change modify after 顺序
                    String afterColName = newColumnAfter.get(colName);
                    int afterIdx = findIdxFromTableElementList(newTableElementList, afterColName);
                    if (afterIdx != newTableElementList.size()) {
                        newTableElementList.add(afterIdx + 1, newColDef);
                    }
                }
            }
        }

        stmt.setTableElementList(newTableElementList);

        String sourceSql = stmt.toString();
        SqlCreateTable primaryTableNode =
            (SqlCreateTable) new FastsqlParser().parse(sourceSql, executionContext).get(0);

        return new Pair<>(sourceSql, primaryTableNode);
    }

    private List<SqlIndexDefinition> buildIndexDefinition4Auto(String schemaName, String tableName,
                                                               BaseDdlOperation logicalDdlPlan,
                                                               ExecutionContext executionContext,
                                                               AlterTableWithGsiPreparedData gsiData,
                                                               AlterTablePreparedData alterTablePreparedData,
                                                               RebuildTablePrepareData rebuildTablePrepareData,
                                                               AtomicBoolean primaryKeyNotChanged,
                                                               List<String> oldPrimaryKeys,
                                                               Pair<String, SqlCreateTable> primaryTableInfo,
                                                               SqlAlterTable ast) {
        Map<String, String> tableNameMap = rebuildTablePrepareData.getTableNameMap();
        Map<String, String> tableNameMapReverse = rebuildTablePrepareData.getTableNameMapReverse();
        Map<String, Boolean> needReHash = rebuildTablePrepareData.getNeedReHash();
        List<String> dropColumns = rebuildTablePrepareData.getDropColumns();

        boolean isUnique = false;
        List<SqlIndexDefinition> gsiList = new ArrayList<>();
        {
            String targetTableName;
            List<String> indexKeys = new ArrayList<>();
            List<String> coveringKeys = new ArrayList<>();

            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            PartitionInfo refPartitionInfo = tableMeta.getPartitionInfo();

            TableGroupConfig tableGroupConfig =
                executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableGroupInfoManager()
                    .getTableGroupConfigById(refPartitionInfo.getTableGroupId());
            String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
            String firstTbInTg = tableGroupConfig.getTables().get(0);
            PartitionInfo firstTblIgPartInfo =
                executionContext.getSchemaManager(schemaName).getTable(firstTbInTg).getPartitionInfo();

            SqlPartitionBy sqlPartitionBy =
                AlterRepartitionUtils.generateSqlPartitionBy(tableName, tableGroupName,
                    refPartitionInfo, firstTblIgPartInfo);
            SqlConverter sqlConverter = SqlConverter.getInstance(logicalDdlPlan.getSchemaName(), executionContext);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalDdlPlan.getCluster());
            Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.getRexInfoFromPartition(sqlPartitionBy, plannerContext);
            ((AlterTable) (logicalDdlPlan.relDdl)).getAllRexExprInfo().putAll(partRexInfoCtx);

            if (alterTablePreparedData.getAddedPrimaryKeyColumns() != null
                && !alterTablePreparedData.getAddedPrimaryKeyColumns().isEmpty()) {

                // check if add generated column as primary key
                for (String column : alterTablePreparedData.getAddedPrimaryKeyColumns()) {
                    ColumnMeta cm = tableMeta.getColumnIgnoreCase(column);
                    if (cm != null && cm.isGeneratedColumn()) {
                        if (!executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "create unique index on VIRTUAL/STORED generated column is not enabled");
                        }
                    }
                }

                primaryKeyNotChanged.set(
                    primaryKeyChanged(oldPrimaryKeys, alterTablePreparedData.getAddedPrimaryKeyColumns()));

                for (SqlNode item : sqlPartitionBy.getColumns()) {
                    SqlIdentifier column = (SqlIdentifier) item;
                    if (StringUtils.equalsIgnoreCase(column.getSimple(), IMPLICIT_COL_NAME)) {
                        sqlPartitionBy.getColumns().remove(column);
                        sqlPartitionBy.getColumns().add(
                            new SqlIdentifier(alterTablePreparedData.getAddedPrimaryKeyColumns().get(0),
                                SqlParserPos.ZERO));
                        sqlPartitionBy.setSourceSql(sqlPartitionBy.getSourceSql()
                            .replace(IMPLICIT_COL_NAME, alterTablePreparedData.getAddedPrimaryKeyColumns().get(0)));
                        alterTablePreparedData.setKeepPartitionKeyRange(false);
                        break;
                    }
                }
            }

            targetTableName = GsiUtils.generateRandomGsiName(tableName);
            if (tableMeta.isGsi()) {
                GsiMetaManager.GsiTableMetaBean gsiMeta = tableMeta.getGsiTableMetaBean();
                indexKeys.addAll(gsiMeta.gsiMetaBean.indexColumns
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList()));
                coveringKeys.addAll(gsiMeta.gsiMetaBean.coveringColumns
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList()));
                isUnique = !gsiMeta.gsiMetaBean.nonUnique;
            } else {
                indexKeys.addAll(getShardColumnsFromPartitionBy(sqlPartitionBy));
                ast.setLogicalSecondaryTableName(targetTableName);
            }

            tableNameMap.put(tableName, targetTableName);
            tableNameMapReverse.put(targetTableName, tableName);
            needReHash.put(targetTableName, alterTablePreparedData.isNeedRepartition());

            if (!alterTablePreparedData.isKeepPartitionKeyRange()
                && (refPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY
                || refPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH)) {
                sqlPartitionBy.getPartitions().clear();
            }

            if (refPartitionInfo.isSingleTable() || refPartitionInfo.isBroadcastTable()) {
                sqlPartitionBy = null;
            }

            SqlIdentifier tgName = alterTablePreparedData.isKeepPartitionKeyRange() ?
                new SqlIdentifier(tableGroupName, SqlParserPos.ZERO) : null;

            boolean withImplicitTg = StringUtils.isNotEmpty(ast.getTargetImplicitTableGroupName());
            SqlIndexDefinition repartitionGsi =
                AlterRepartitionUtils.initIndexInfo(
                    targetTableName,
                    indexKeys,
                    coveringKeys,
                    !tableMeta.isGsi(),
                    isUnique,
                    primaryTableInfo.getKey(),
                    primaryTableInfo.getValue(),
                    sqlPartitionBy,
                    withImplicitTg ? new SqlIdentifier(ast.getTargetImplicitTableGroupName(), SqlParserPos.ZERO) :
                        tgName,
                    withImplicitTg
                );
            repartitionGsi.setBroadcast(refPartitionInfo.isBroadcastTable());
            repartitionGsi.setSingle(refPartitionInfo.isSingleTable());

            gsiList.add(repartitionGsi);
        }
        Set<String> indexNames = new TreeSet<>(String::compareToIgnoreCase);
        indexNames.addAll(ast.getIndexTableGroupMap().keySet());
        if (gsiData != null) {
            List<AlterTablePreparedData> globalIndexPreparedDataList = gsiData.getGlobalIndexPreparedData();
            for (AlterTablePreparedData globalIndexPreparedData : globalIndexPreparedDataList) {
                String indexName = globalIndexPreparedData.getTableName();
                TableMeta indexMeta = executionContext.getSchemaManager(schemaName).getTable(indexName);
                PartitionInfo refPartitionInfo = indexMeta.getPartitionInfo();

                Long indexTgId = refPartitionInfo.getTableGroupId();
                TableGroupConfig tableGroupConfig =
                    executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableGroupInfoManager()
                        .getTableGroupConfigById(indexTgId);
                String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
                String firstTbInTg = tableGroupConfig.getTables().get(0);
                PartitionInfo firstTablePartInfo =
                    executionContext.getSchemaManager(schemaName).getTable(firstTbInTg).getPartitionInfo();

                SqlPartitionBy sqlPartitionBy =
                    AlterRepartitionUtils.generateSqlPartitionBy(indexName, tableGroupName, refPartitionInfo,
                        firstTablePartInfo);

                SqlConverter sqlConverter = SqlConverter.getInstance(logicalDdlPlan.getSchemaName(), executionContext);
                PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalDdlPlan.getCluster());
                Map<SqlNode, RexNode> partRexInfoCtx =
                    sqlConverter.getRexInfoFromPartition(sqlPartitionBy, plannerContext);
                ((AlterTable) (logicalDdlPlan.relDdl)).getAllRexExprInfo().putAll(partRexInfoCtx);

                if (alterTablePreparedData.getAddedPrimaryKeyColumns() != null
                    && !alterTablePreparedData.getAddedPrimaryKeyColumns().isEmpty()) {
                    for (SqlNode item : sqlPartitionBy.getColumns()) {
                        SqlIdentifier column = (SqlIdentifier) item;
                        if (StringUtils.equalsIgnoreCase(column.getSimple(), IMPLICIT_COL_NAME)) {
                            sqlPartitionBy.getColumns().remove(column);
                            sqlPartitionBy.getColumns().add(
                                new SqlIdentifier(alterTablePreparedData.getAddedPrimaryKeyColumns().get(0),
                                    SqlParserPos.ZERO));
                            sqlPartitionBy.setSourceSql(sqlPartitionBy.getSourceSql()
                                .replace(IMPLICIT_COL_NAME, alterTablePreparedData.getAddedPrimaryKeyColumns().get(0)));
                            alterTablePreparedData.setKeepPartitionKeyRange(false);
                            break;
                        }
                    }
                }

                if (!globalIndexPreparedData.isKeepPartitionKeyRange()
                    && (refPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY
                    || refPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH)) {
                    sqlPartitionBy.getPartitions().clear();
                }

                GsiMetaManager.GsiTableMetaBean gsiMeta = indexMeta.getGsiTableMetaBean();
                List<String> gsiIndexKeys = gsiMeta.gsiMetaBean.getIndexColumns()
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList());
                List<String> gsiCoveringKeys = gsiMeta.gsiMetaBean.getCoveringColumns()
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList());
                isUnique = !gsiMeta.gsiMetaBean.nonUnique;

                List<String> partitionKeys = indexMeta.getPartitionInfo().getPartitionColumns();

                for (String partitionKey : partitionKeys) {
                    if (!gsiIndexKeys.contains(partitionKey.toLowerCase())
                        && !gsiCoveringKeys.contains(partitionKey.toLowerCase())) {
                        gsiCoveringKeys.add(partitionKey.toLowerCase());
                    }
                }

                gsiIndexKeys = gsiIndexKeys.stream()
                    .filter(e -> !dropColumns.contains(e.toLowerCase()))
                    .map(e -> rebuildTablePrepareData.getBackfillColumnMap().getOrDefault(e.toLowerCase(), e))
                    .collect(Collectors.toList());

                gsiCoveringKeys = gsiCoveringKeys.stream()
                    .filter(e -> !dropColumns.contains(e.toLowerCase()))
                    .map(e -> rebuildTablePrepareData.getBackfillColumnMap().getOrDefault(e.toLowerCase(), e))
                    .collect(Collectors.toList());

                if (gsiMeta.gsiMetaBean.clusteredIndex) {
                    // 聚簇索引也需要加列
                    gsiCoveringKeys.addAll(rebuildTablePrepareData.getAddNewColumns());
                }

                String targetGsiName = GsiUtils.generateRandomGsiName(indexName);
                tableNameMap.put(indexName, targetGsiName);
                tableNameMapReverse.put(targetGsiName, indexName);
                needReHash.put(targetGsiName, globalIndexPreparedData.isNeedRepartition());

                SqlIdentifier tgName = globalIndexPreparedData.isKeepPartitionKeyRange() ?
                    new SqlIdentifier(tableGroupName, SqlParserPos.ZERO) : null;

                String logicalIndexName = TddlSqlToRelConverter.unwrapGsiName(indexName);
                String targetTableGroupName = ast.getIndexTableGroupMap().get(logicalIndexName);
                boolean indexWithImplicitTg = (targetTableGroupName != null);
                if (indexWithImplicitTg) {
                    indexNames.remove(logicalIndexName);
                }
                SqlIndexDefinition repartitionGsiInfo =
                    AlterRepartitionUtils.initIndexInfo(
                        targetGsiName,
                        gsiIndexKeys,
                        gsiCoveringKeys,
                        false,
                        isUnique,
                        primaryTableInfo.getKey(),
                        primaryTableInfo.getValue(),
                        sqlPartitionBy,
                        //todo may specific the gsi's implicit tablegroup
                        indexWithImplicitTg ? new SqlIdentifier(targetTableGroupName, SqlParserPos.ZERO) : tgName,
                        indexWithImplicitTg
                    );
                gsiList.add(repartitionGsiInfo);
                if (!globalIndexPreparedData.isNeedRepartition() && indexWithImplicitTg
                    && !tableGroupName.equalsIgnoreCase(targetTableGroupName)) {
                    needReHash.put(targetGsiName, true);
                }
            }
        }
        if (!indexNames.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                indexNames + " is not exists or not need to repartition");
        }
        List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
            StringUtils.equalsIgnoreCase(e.getType(), "UNIQUE") ?
                new SqlAddUniqueIndex(SqlParserPos.ZERO, e.getIndexName(), e) :
                new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());

        return gsiList;
    }

    private List<SqlIndexDefinition> buildIndexDefinition4Drds(String schemaName, String tableName,
                                                               ExecutionContext executionContext,
                                                               AlterTableWithGsiPreparedData gsiData,
                                                               AlterTablePreparedData alterTablePreparedData,
                                                               RebuildTablePrepareData rebuildTablePrepareData,
                                                               Pair<String, SqlCreateTable> primaryTableInfo,
                                                               SqlAlterTable ast) {
        Map<String, String> tableNameMap = rebuildTablePrepareData.getTableNameMap();
        Map<String, String> tableNameMapReverse = rebuildTablePrepareData.getTableNameMapReverse();
        Map<String, Boolean> needReHash = rebuildTablePrepareData.getNeedReHash();
        List<String> dropColumns = rebuildTablePrepareData.getDropColumns();

        List<SqlIndexDefinition> gsiList = new ArrayList<>();

        {
            TableRule tableRule =
                executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(tableName);
            String targetTableName = GsiUtils.generateRandomGsiName(tableName);
            tableNameMap.put(tableName, targetTableName);
            tableNameMapReverse.put(targetTableName, tableName);
            needReHash.put(targetTableName, tableRule.isBroadcast() || alterTablePreparedData.isNeedRepartition());
            ast.setLogicalSecondaryTableName(targetTableName);

            SqlAlterTablePartitionKey sqlAlterTablePartitionKey =
                generateSqlPartitionKey(schemaName, tableName, executionContext);
            sqlAlterTablePartitionKey.setLogicalSecondaryTableName(targetTableName);

            gsiList.add(AlterRepartitionUtils.initIndexInfo(primaryTableInfo.getValue(), sqlAlterTablePartitionKey,
                primaryTableInfo.getKey()));
        }

        if (gsiData != null) {
            List<AlterTablePreparedData> globalIndexPreparedDataList = gsiData.getGlobalIndexPreparedData();
            for (AlterTablePreparedData globalIndexPreparedData : globalIndexPreparedDataList) {
                String indexName = globalIndexPreparedData.getTableName();

                String targetGsiName = GsiUtils.generateRandomGsiName(indexName);
                tableNameMap.put(indexName, targetGsiName);
                tableNameMapReverse.put(targetGsiName, indexName);
                needReHash.put(targetGsiName, globalIndexPreparedData.isNeedRepartition());

                TableRule tableRule =
                    executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(indexName);
                TableMeta indexMeta = executionContext.getSchemaManager(schemaName).getTable(indexName);

                GsiMetaManager.GsiTableMetaBean gsiMeta = indexMeta.getGsiTableMetaBean();
                List<String> gsiIndexKeys = gsiMeta.gsiMetaBean.indexColumns
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList());
                List<String> gsiCoveringKeys = gsiMeta.gsiMetaBean.coveringColumns
                    .stream().map(e -> e.columnName.toLowerCase()).collect(Collectors.toList());
                boolean isUnique = !gsiMeta.gsiMetaBean.nonUnique;

                List<String> shardingKeys = tableRule.getShardColumns();

                for (String shardingKey : shardingKeys) {
                    if (!gsiIndexKeys.contains(shardingKey.toLowerCase())
                        && !gsiCoveringKeys.contains(shardingKey.toLowerCase())) {
                        gsiCoveringKeys.add(shardingKey.toLowerCase());
                    }
                }

                gsiIndexKeys = gsiIndexKeys.stream()
                    .filter(e -> !dropColumns.contains(e.toLowerCase()))
                    .map(e -> rebuildTablePrepareData.getBackfillColumnMap().getOrDefault(e.toLowerCase(), e))
                    .collect(Collectors.toList());

                gsiCoveringKeys = gsiCoveringKeys.stream()
                    .filter(e -> !dropColumns.contains(e.toLowerCase()))
                    .map(e -> rebuildTablePrepareData.getBackfillColumnMap().getOrDefault(e.toLowerCase(), e))
                    .collect(Collectors.toList());

                if (gsiMeta.gsiMetaBean.clusteredIndex) {
                    // 聚簇索引也需要加列
                    gsiCoveringKeys.addAll(rebuildTablePrepareData.getAddNewColumns());
                }

                SqlAlterTablePartitionKey sqlAlterTablePartitionKey =
                    generateSqlPartitionKey(schemaName, indexName, executionContext);

                gsiList.add(
                    AlterRepartitionUtils.initIndexInfo4DrdsOmc(targetGsiName, gsiIndexKeys, gsiCoveringKeys, false,
                        isUnique, primaryTableInfo.getKey(), primaryTableInfo.getValue(), sqlAlterTablePartitionKey));
            }
        }

        return gsiList;
    }

    private static int findIdxFromTableElementList(List<SQLTableElement> tableElementList, String columnName) {
        int idx = 0;
        for (SQLTableElement tableElement : tableElementList) {
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                String curColumnName = SQLUtils.normalizeNoTrim(columnDefinition.getColumnName());
                if (com.alibaba.polardbx.druid.util.StringUtils.equalsIgnoreCase(curColumnName, columnName)) {
                    break;
                }
            }
            idx++;
        }
        return idx;
    }

    public List<SqlIndexDefinition> buildIndexDefinition4AutoForTest(String schemaName, String tableName,
                                                                     BaseDdlOperation logicalDdlPlan,
                                                                     ExecutionContext executionContext,
                                                                     AlterTableWithGsiPreparedData gsiData,
                                                                     AlterTablePreparedData alterTablePreparedData,
                                                                     RebuildTablePrepareData rebuildTablePrepareData,
                                                                     AtomicBoolean primaryKeyNotChanged,
                                                                     List<String> oldPrimaryKeys,
                                                                     Pair<String, SqlCreateTable> primaryTableInfo,
                                                                     SqlAlterTable ast) {
        return buildIndexDefinition4Auto(schemaName, tableName, logicalDdlPlan, executionContext, gsiData,
            alterTablePreparedData, rebuildTablePrepareData, primaryKeyNotChanged, oldPrimaryKeys, primaryTableInfo,
            ast);
    }

    /**
     * Check if need add a subjob task of auto refresh the cci view of arc tbl
     */
    public void addRefreshArcViewSubJobIfNeed(DdlJob ddlJob, LogicalAlterTable alterTable, ExecutionContext ec) {
        if (!alterTable.needRefreshArcTblView(ec)) {
            return;
        }

        String tableSchema = alterTable.getSchemaName();
        String tableName = alterTable.getTableName();
        TtlDefinitionInfo ttlInfo = TtlUtil.fetchTtlDefinitionInfoByDbAndTb(tableSchema, tableName, ec);
        if (ttlInfo == null) {
            return;
        }
        String arcTblSchema = ttlInfo.getArchiveTableSchema();
        String arcTblName = ttlInfo.getArchiveTableName();

        String createViewSqlForArcTbl =
            TtlTaskSqlBuilder.buildCreateViewSqlForArcTbl(arcTblSchema, arcTblName, ttlInfo);
        String dropViewSqlForArcTbl = ""; // ignore rollback
        SubJobTask freshViewSubJobTask =
            new SubJobTask(tableSchema, createViewSqlForArcTbl, dropViewSqlForArcTbl);
        freshViewSubJobTask.setParentAcquireResource(true);

        ExecutableDdlJob executableDdlJob = (ExecutableDdlJob) ddlJob;
//        DdlTask tailTask = executableDdlJob.getTail();
        List<DdlTask> tailNodes =
            executableDdlJob.getAllZeroOutDegreeVertexes().stream().map(o -> o.getObject()).collect(
                Collectors.toList());
        executableDdlJob.addTask(freshViewSubJobTask);
        for (int i = 0; i < tailNodes.size(); i++) {
            DdlTask tailTask = tailNodes.get(i);
            executableDdlJob.addTaskRelationship(tailTask, freshViewSubJobTask);
        }
        executableDdlJob.labelAsTail(freshViewSubJobTask);
    }
}
