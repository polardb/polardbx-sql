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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.StatisticSampleTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalCreateIndexHandler extends LogicalCommonDdlHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateIndexHandler.class);

    public LogicalCreateIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalCreateIndex logicalCreateIndex = (LogicalCreateIndex) logicalDdlPlan;

        if (logicalCreateIndex.needRewriteToGsi(false)) {
            logicalCreateIndex.needRewriteToGsi(true);
        }

        boolean globalIndex = logicalCreateIndex.isClustered() || logicalCreateIndex.isGsi();
        boolean expressionIndex = isExpressionIndex(logicalCreateIndex, executionContext);

        if (expressionIndex) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CREATE_EXPRESSION_INDEX)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "create expression index is not enabled");
            }

            if (globalIndex) {
                return buildCreateGlobalExpressionIndexJob();
            } else {
                return buildCreateLocalExpressionIndexJob(logicalCreateIndex, executionContext);
            }
        } else {
            if (globalIndex) {
                return buildCreateGsiJob(logicalCreateIndex, executionContext);
            } else {
                return buildCreateLocalIndexJob(logicalCreateIndex, executionContext);
            }
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) logicalDdlPlan.getNativeSqlNode();

        final String tableName = sqlCreateIndex.getOriginTableName().getLastName();
        TableValidator.validateTableName(tableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), tableName, executionContext);

        final String indexName = sqlCreateIndex.getIndexName().getLastName();
        IndexValidator.validateIndexNameLength(indexName);
        IndexValidator.validateIndexNonExistence(logicalDdlPlan.getSchemaName(), tableName, indexName);

        return false;
    }

    private DdlJob buildCreateLocalIndexJob(LogicalCreateIndex logicalCreateIndex, ExecutionContext executionContext) {
        logicalCreateIndex.prepareData();

        ExecutableDdlJob localIndexJob = CreateIndexJobFactory.createLocalIndex(
            logicalCreateIndex.relDdl, logicalCreateIndex.getNativeSqlNode(),
            logicalCreateIndex.getCreateLocalIndexPreparedDataList(),
            executionContext);

        Map<String, Set<String>> validSchema = Maps.newTreeMap(String::compareToIgnoreCase);
        if (localIndexJob != null && GeneralUtil.isNotEmpty(logicalCreateIndex.getCreateLocalIndexPreparedDataList())) {
            List<DdlTask> validateTasks = Lists.newArrayList();
            for (CreateLocalIndexPreparedData preparedData : logicalCreateIndex.getCreateLocalIndexPreparedDataList()) {
                if (validSchema.computeIfAbsent(
                        preparedData.getSchemaName(), x -> Sets.newTreeSet(String::compareToIgnoreCase)).
                    add(preparedData.getTableName())) {
                    Map<String, Long> tableVersions = new HashMap<>();
                    tableVersions.put(preparedData.getTableName(), preparedData.getTableVersion());
                    validateTasks.add(new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions));
                }
            }
            localIndexJob.addSequentialTasks(validateTasks);

            localIndexJob.addTaskRelationship(Util.last(validateTasks), localIndexJob.getHead());
        }
        return localIndexJob;
    }

    private DdlJob buildCreateGsiJob(LogicalCreateIndex logicalCreateIndex, ExecutionContext executionContext) {
        initPrimaryTableDefinition(logicalCreateIndex, executionContext);

        // Should prepare data after initializing the primary table definition.
        logicalCreateIndex.prepareData();

        CreateIndexWithGsiPreparedData preparedData = logicalCreateIndex.getCreateIndexWithGsiPreparedData();
        CreateGlobalIndexPreparedData globalIndexPreparedData = preparedData.getGlobalIndexPreparedData();

        ExecutableDdlJob gsiJob = CreatePartitionGsiJobFactory.create(
            logicalCreateIndex.relDdl, globalIndexPreparedData, executionContext);

        if (globalIndexPreparedData.isNeedToGetTableGroupLock()) {
            return gsiJob;
        }
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(globalIndexPreparedData.getSchemaName(), tableVersions);

        gsiJob.addTask(validateTableVersionTask);
        gsiJob.addTaskRelationship(validateTableVersionTask, gsiJob.getHead());

        ExecutableDdlJob localIndexJob = CreateIndexJobFactory.createLocalIndex(
            logicalCreateIndex.relDdl, logicalCreateIndex.getNativeSqlNode(),
            logicalCreateIndex.getCreateLocalIndexPreparedDataList(),
            executionContext);
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        gsiJob.addSequentialTasksAfter(gsiJob.getTail(), Lists.newArrayList(new StatisticSampleTask(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getIndexTableName()
        )));
        return gsiJob;
    }

    /**
     * Get table definition from primary table and generate index table definition with it
     */
    private void initPrimaryTableDefinition(LogicalCreateIndex logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) logicalDdlPlan.getNativeSqlNode();
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);
        sqlCreateIndex.setPrimaryTableDefinition(primaryTableInfo.getKey());
        sqlCreateIndex.setPrimaryTableNode(primaryTableInfo.getValue());

        // TODO(moyi) these two AST is duplicated, choose one of them, but right row somehow both of them are used
        sqlCreateIndex = logicalDdlPlan.getSqlCreateIndex();
        sqlCreateIndex.setPrimaryTableDefinition(primaryTableInfo.getKey());
        sqlCreateIndex.setPrimaryTableNode(primaryTableInfo.getValue());
    }

    private boolean isExpressionIndex(LogicalCreateIndex logicalCreateIndex, ExecutionContext executionContext) {
        if (logicalCreateIndex.getSqlCreateIndex().getOriginalSql() == null) {
            return false;
        }

        String schemaName = logicalCreateIndex.getSchemaName();
        String tableName = logicalCreateIndex.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        Set<String> tableColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableColumns.addAll(
            tableMeta.getPhysicalColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        SQLCreateIndexStatement sqlCreateIndexStmt;
        try {
            sqlCreateIndexStmt = (SQLCreateIndexStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                logicalCreateIndex.getSqlCreateIndex().getOriginalSql(), JdbcConstants.MYSQL).get(0);
        } catch (Throwable e) {
            logger.error("fail to parse sql " + logicalCreateIndex.getSqlCreateIndex().getOriginalSql(), e);
            return false;
        }

        // rewrite expression index
        boolean isExprIndex = sqlCreateIndexStmt.getColumns() != null && sqlCreateIndexStmt.getColumns().stream()
            .anyMatch(c -> GeneratedColumnUtil.isExpression(c, tableColumns));

        if (isExprIndex && sqlCreateIndexStmt.getIndexDefinition().isUnique()) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "create unique expression index is not enabled");
            }
        }

        // check if we build unique index on virtual/stored generated column
        boolean isOnGeneratedColumn =
            sqlCreateIndexStmt.getColumns() != null && sqlCreateIndexStmt.getColumns().stream()
                .anyMatch(c -> GeneratedColumnUtil.isGeneratedColumn(c, tableColumns, tableMeta));

        if (isOnGeneratedColumn && sqlCreateIndexStmt.getIndexDefinition().isUnique()) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "create unique index on VIRTUAL/STORED generated column is not enabled");
            }
        }

        return isExprIndex;
    }

    private DdlJob buildCreateLocalExpressionIndexJob(LogicalCreateIndex logicalCreateIndex,
                                                      ExecutionContext executionContext) {
        String schemaName = logicalCreateIndex.getSchemaName();
        String tableName = logicalCreateIndex.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        SQLCreateIndexStatement sqlCreateIndexStatement =
            (SQLCreateIndexStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                logicalCreateIndex.getSqlCreateIndex().getOriginalSql(),
                JdbcConstants.MYSQL).get(0);

        SQLAlterTableStatement sqlAlterTableStatement = new SQLAlterTableStatement();
        sqlAlterTableStatement.setTableSource(new SQLExprTableSource(sqlCreateIndexStatement.getTableName()));
        sqlAlterTableStatement.setDbType(DbType.mysql);

        SQLIndexDefinition sqlIndexDefinition = new SQLIndexDefinition();
        sqlCreateIndexStatement.getIndexDefinition().cloneTo(sqlIndexDefinition);
        sqlIndexDefinition.setColumns(new ArrayList<>());

        List<SQLAlterTableItem> sqlAlterTableItems =
            GeneratedColumnUtil.rewriteExprIndex(tableMeta, sqlCreateIndexStatement.getIndexDefinition(),
                executionContext);

        for (SQLAlterTableItem item : sqlAlterTableItems) {
            sqlAlterTableStatement.addItem(item);
        }

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), executionContext);
        sqlAlterTableStatement.accept(visitor);
        SqlAlterTable sqlAlterTable = (SqlAlterTable) visitor.getSqlNode();

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(schemaName);
        ExecutionPlan executionPlan = Planner.getInstance().getPlan(sqlAlterTable, plannerContext);
        LogicalAlterTable logicalAlterTable = (LogicalAlterTable) executionPlan.getPlan();
        logicalAlterTable.setRewrittenAlterSql(true);

        LogicalAlterTableHandler logicalAlterTableHandler = new LogicalAlterTableHandler(repo);
        return logicalAlterTableHandler.buildDdlJob(logicalAlterTable, executionContext);
    }

    private DdlJob buildCreateGlobalExpressionIndexJob() {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "add global expression index");
    }
}
