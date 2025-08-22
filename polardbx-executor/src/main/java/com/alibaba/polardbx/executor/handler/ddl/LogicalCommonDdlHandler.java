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

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraintImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.CommonValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddPrimaryKey;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;
import static com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler.reorgLogicalColumnOrder;
import static com.alibaba.polardbx.executor.handler.ddl.LogicalCreateTableHandler.generateCreateTableSqlForLike;
import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapGsiName;

public abstract class LogicalCommonDdlHandler extends HandlerCommon {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalCommonDdlHandler.class);

    public LogicalCommonDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

        initDdlContext(logicalDdlPlan, executionContext);

        // Validate the plan on file storage first
        TableValidator.validateTableEngine(logicalDdlPlan, executionContext);
        // Validate the plan first and then return immediately if needed.
        boolean returnImmediately = validatePlan(logicalDdlPlan, executionContext);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName());

        if (isNewPartDb) {
            setPartitionDbIndexAndPhyTable(logicalDdlPlan);
        } else {
            setDbIndexAndPhyTable(logicalDdlPlan);
        }

        // Build a specific DDL job by subclass.
        DdlJob ddlJob = returnImmediately ?
            new TransientDdlJob() :
            buildDdlJob(logicalDdlPlan, executionContext);

        // Validate the DDL job before request.
        validateJob(logicalDdlPlan, ddlJob, executionContext);

        if (executionContext.getDdlContext().getExplain()) {
            return buildExplainResultCursor(logicalDdlPlan, ddlJob, executionContext);
        }

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, executionContext);

        if (executionContext.getDdlContext().isSubJob()) {
            return buildSubJobResultCursor(ddlJob, executionContext);
        }
        return buildResultCursor(logicalDdlPlan, ddlJob, executionContext);
    }

    /**
     * Build a DDL job that can be executed by new DDL Engine.
     */
    protected abstract DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext);

    /**
     * Build a cursor as result, which is empty as default.
     * Some special DDL command could override this method to generate its own result.
     */
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        if (ec.getParamManager().getBoolean(ConnectionParams.RETURN_JOB_ID_ON_ASYNC_DDL_MODE)) {
            ArrayResultCursor result = new ArrayResultCursor("Execution Plan");
            result.addColumn("job_id", DataTypes.LongType);
            result.initMeta();
            long jobId = 0;
            if (ec.getDdlContext() != null) {
                jobId = ec.getDdlContext().getJobId();
            }
            result.addRow(new Long[] {jobId});
            return result;
        }

        // Always return 0 rows affected or throw an exception to report error messages.
        // SHOW DDL RESULT can provide more result details for the DDL execution.
        return new AffectRowCursor(new int[] {0});
    }

    protected Cursor buildExplainResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        ArrayResultCursor result = new ArrayResultCursor("Logical ExecutionPlan");
        result.addColumn("Execution Plan", DataTypes.StringType);
        result.initMeta();
        for (String row : ddlJob.getExplainInfo()) {
            result.addRow(new String[] {row});
        }
        return result;
    }

    protected Cursor buildSubJobResultCursor(DdlJob ddlJob, ExecutionContext executionContext) {
        long taskId = executionContext.getDdlContext().getParentTaskId();
        long subJobId = executionContext.getDdlContext().getJobId();
        if ((ddlJob instanceof TransientDdlJob) && subJobId == 0L) {
            // -1 means no need to run
            subJobId = -1L;
        }
        ArrayResultCursor result = new ArrayResultCursor("SubJob");
        result.addColumn(DdlConstants.PARENT_TASK_ID, DataTypes.LongType);
        result.addColumn(DdlConstants.JOB_ID, DataTypes.LongType);
        result.addRow(new Object[] {taskId, subJobId});
        return result;
    }

    /**
     * A subclass may need extra validation.
     *
     * @return Indicate if need to return immediately
     */
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return false;
    }

    protected void validateJob(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob instanceof TransientDdlJob) {
            return;
        }
        CommonValidator.validateDdlJob(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName(), ddlJob, LOGGER,
            executionContext);

        checkTaskName(ddlJob.getAllTasks());
    }

    protected void initDdlContext(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String schemaName = logicalDdlPlan.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        DdlType ddlType = logicalDdlPlan.getDdlType();
        String objectName = getObjectName(logicalDdlPlan);

        DdlContext ddlContext =
            DdlContext.create(schemaName, objectName, ddlType, executionContext);

        executionContext.setDdlContext(ddlContext);

        rewriteOriginSqlToCdcMarkSql(logicalDdlPlan, executionContext, schemaName, objectName);
    }

    protected void handleDdlRequest(DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob instanceof TransientDdlJob) {
            return;
        }
        DdlContext ddlContext = executionContext.getDdlContext();
        ddlContext.setDdlJobFactoryName(ddlJob.getDdlJobFactoryName());
        if (ddlContext.isSubJob()) {
            DdlEngineRequester.create(ddlJob, executionContext).executeSubJob(
                ddlContext.getParentJobId(), ddlContext.getParentTaskId(), ddlContext.isForRollback());
        } else {
            DdlEngineRequester.create(ddlJob, executionContext).execute();
        }
    }

    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return logicalDdlPlan.getTableName();
    }

    private static void checkTaskName(List<DdlTask> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        for (DdlTask t : taskList) {
            if (!SerializableClassMapper.containsClass(t.getClass())) {
                String errMsg = String.format("Task:%s not registered yet", t.getClass().getCanonicalName());
                throw new TddlNestableRuntimeException(errMsg);
            }
        }
    }

    protected void setDbIndexAndPhyTable(BaseDdlOperation logicalDdlPlan) {
        final TddlRuleManager rule = OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getRuleManager();
        final boolean singleDbIndex = rule.isSingleDbIndex();

        String dbIndex = rule.getDefaultDbIndex(null);
        String phyTable = RelUtils.lastStringValue(logicalDdlPlan.getTableNameNode());
        if (null != logicalDdlPlan.getTableNameNode() && !singleDbIndex) {
            final TargetDB target = rule.shardAny(phyTable);
            phyTable = target.getTableNames().iterator().next();
            dbIndex = target.getDbIndex();
        }

        logicalDdlPlan.setDbIndex(dbIndex);
        logicalDdlPlan.setPhyTable(phyTable);
    }

    protected void setPartitionDbIndexAndPhyTable(BaseDdlOperation logicalDdlPlan) {
        final PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getPartitionInfoManager();
        final TddlRuleManager rule = OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getRuleManager();

        String dbIndex = rule.getDefaultDbIndex(null);
        String phyTable = RelUtils.lastStringValue(logicalDdlPlan.getTableNameNode());
        if (null != logicalDdlPlan.getTableNameNode()) {
            final PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(phyTable);
            if (partitionInfo != null) {
                PartitionLocation location =
                    partitionInfo.getPartitionBy().getPhysicalPartitions().get(0).getLocation();
                phyTable = location.getPhyTableName();
                dbIndex = location.getGroupKey();
            }
        }

        logicalDdlPlan.setDbIndex(dbIndex);
        logicalDdlPlan.setPhyTable(phyTable);
    }

    protected Pair<String, SqlCreateTable> genPrimaryTableInfo(BaseDdlOperation logicalDdlPlan,
                                                               ExecutionContext executionContext) {
        Cursor cursor = null;
        try {
            cursor = repo.getCursorFactory().repoCursor(executionContext,
                new PhyShow(logicalDdlPlan.getCluster(), logicalDdlPlan.getTraitSet(),
                    SqlShowCreateTable.create(SqlParserPos.ZERO,
                        new SqlIdentifier(logicalDdlPlan.getPhyTable(), SqlParserPos.ZERO)
                    ),
                    logicalDdlPlan.getRowType(), logicalDdlPlan.getDbIndex(), logicalDdlPlan.getPhyTable()
                )
            );

            Row row;
            if ((row = cursor.next()) != null) {
                final String primaryTableDefinition = row.getString(1);
                // reorder column
                String sql = reorgLogicalColumnOrder(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName(),
                    primaryTableDefinition);
                final SqlCreateTable primaryTableNode =
                    (SqlCreateTable) new FastsqlParser().parse(sql, executionContext).get(0);
                return new Pair<>(sql, primaryTableNode);
            }

            return null;
        } finally {
            if (null != cursor) {
                cursor.close(new ArrayList<>());
            }
        }
    }

    public static boolean isAvailableForRecycleBin(String tableName, ExecutionContext executionContext) {
        final String appName = executionContext.getAppName();
        final RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(appName);
        return executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_RECYCLEBIN) &&
            !RecycleBin.isRecyclebinTable(tableName) &&
            recycleBin != null && !recycleBin.hasForeignConstraint(appName, tableName);
    }

    // rewrite sql for cdc, @see com.alibaba.polardbx.cdc.ImplicitTableGroupUtil
    // rewrite reason : adding a name for anonymous indexes, then we can add implicit table-group for the indexes
    protected void rewriteOriginSqlToCdcMarkSql(BaseDdlOperation logicalDdlPlan, ExecutionContext ec,
                                                String schemaName, String tableName) {
        // some sql has no schema name.
        // e.g. rebalance database policy='partition_balance'
        if (StringUtils.isEmpty(logicalDdlPlan.getSchemaName())) {
            return;
        }

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName());

        // only auto mode database support rewrite, because drds mode do not support table group and do not support using same GSI name in different table
        // rewrite create like to actual create sql for adding implicit table group
        // because target table`s table-group may be different from the base table`s table-group
        if (logicalDdlPlan instanceof LogicalCreateTable) {
            LogicalCreateTable logicalCreateTable = (LogicalCreateTable) logicalDdlPlan;
            SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;
            if (sqlCreateTable.getLikeTableName() != null) {
                if (isNewPartDb) {
                    final String sourceCreateTableSql = generateCreateTableSqlForLike(sqlCreateTable, ec);
                    MySqlCreateTableStatement stmt =
                        (MySqlCreateTableStatement) FastsqlUtils.parseSql(sourceCreateTableSql).get(0);
                    stmt.getTableSource()
                        .setSimpleName(SqlIdentifier.surroundWithBacktick(logicalCreateTable.getTableName()));
                    ec.getDdlContext().setCdcRewriteDdlStmt(stmt.toString(VisitorFeature.OutputHashPartitionsByRange));
                }
                return;
            }
        }

        String originSql = ec.getOriginSql();
        final List<SQLStatement> statementList =
            SQLParserUtils.createSQLStatementParser(originSql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList();
        if (statementList.isEmpty()) {
            return;
        }

        if (logicalDdlPlan.getDdlType() == DdlType.CREATE_TABLE &&
            statementList.get(0) instanceof MySqlCreateTableStatement) {
            // MySqlExplainStatement also  belongs to CREATE_TABLE
            final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

            Set<String> indexNamesSet = new HashSet<>();
            for (final SQLTableElement tableElement : stmt.getTableElementList()) {
                if (tableElement instanceof SQLConstraintImpl) {
                    final SQLConstraintImpl constraint = (SQLConstraintImpl) tableElement;
                    final SQLName indexName = constraint.getName();
                    if (indexName != null && indexName.getSimpleName() != null
                        && !indexName.getSimpleName().isEmpty()) {
                        indexNamesSet.add(indexName.getSimpleName());
                    }
                }
            }

            for (final SQLTableElement tableElement : stmt.getTableElementList()) {
                if (tableElement instanceof SQLConstraintImpl) {
                    final SQLConstraintImpl constraint = (SQLConstraintImpl) tableElement;

                    // Assign name if no name.
                    if (!(tableElement instanceof MySqlPrimaryKey) &&
                        (null == constraint.getName() || constraint.getName().getSimpleName().isEmpty())) {
                        String baseName = null;
                        int prob = 0;
                        if (tableElement instanceof MySqlKey) {
                            baseName = ((MySqlKey) tableElement).getColumns().get(0).getExpr().toString();
                        } else if (tableElement instanceof MySqlTableIndex) {
                            SQLExpr expr = ((MySqlTableIndex) tableElement).getColumns().get(0).getExpr();
                            if (expr instanceof SQLMethodInvokeExpr) {
                                baseName = ((SQLMethodInvokeExpr) expr).getMethodName();
                            } else {
                                baseName = expr.toString();
                            }
                        }
                        if (baseName != null) {
                            baseName = SQLUtils.normalizeNoTrim(baseName);
                            if (!indexNamesSet.contains(baseName)) {
                                constraint.setName(baseName);
                                indexNamesSet.add(baseName);
                            } else {
                                prob = 2;
                                baseName = baseName + "_";
                                while (indexNamesSet.contains(baseName + prob)) {
                                    ++prob;
                                }
                                constraint.setName(baseName + prob);
                                indexNamesSet.add(baseName + prob);
                            }
                        } else {
                            if (tableElement instanceof MysqlForeignKey) {
                                baseName = tableName.toLowerCase() + "_ibfk_";
                                prob++;
                            } else {
                                baseName = "i_";
                            }
                            while (indexNamesSet.contains(baseName + prob)) {
                                ++prob;
                            }
                            constraint.setName(baseName + prob);
                            indexNamesSet.add(baseName + prob);
                        }
                    }
                }
            }
            String cdcRewriteDdlStmt = ForeignKeyUtils.rewriteOriginSqlWithForeignKey(
                logicalDdlPlan, ec, schemaName, tableName,
                stmt.toString(VisitorFeature.OutputHashPartitionsByRange)
            );
            ec.getDdlContext().setCdcRewriteDdlStmt(cdcRewriteDdlStmt);
        } else if (logicalDdlPlan.getDdlType() == DdlType.ALTER_TABLE &&
            statementList.get(0) instanceof SQLAlterTableStatement) {
            final SQLAlterTableStatement stmt = (SQLAlterTableStatement) statementList.get(0);

            final Set<String> existsNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            tableMeta.getSecondaryIndexes().forEach(meta -> existsNames.add(meta.getPhysicalIndexName()));
            if (tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().indexMap != null) {
                tableMeta.getGsiTableMetaBean().indexMap.forEach((k, v) -> existsNames.add(unwrapGsiName(k)));
            }

            for (final SQLAlterTableItem item : stmt.getItems()) {
                if (item instanceof SQLAlterTableAddIndex) {
                    SQLName indexName = ((SQLAlterTableAddIndex) item).getName();

                    if (null == indexName || null == indexName.getSimpleName() || indexName.getSimpleName().isEmpty()) {
                        String realName;
                        String baseName = ((SQLAlterTableAddIndex) item).getColumns().get(0).getExpr().toString();
                        baseName = SQLUtils.normalizeNoTrim(baseName);
                        if (!existsNames.contains(baseName)) {
                            realName = baseName;
                            existsNames.add(realName);
                        } else {
                            baseName = baseName + "_";
                            int prob = 2;
                            while (existsNames.contains(baseName + prob)) {
                                ++prob;
                            }
                            realName = baseName + prob;
                            existsNames.add(realName);
                        }
                        ((SQLAlterTableAddIndex) item).setName(new SQLIdentifierExpr(realName));
                    }
                } else if (item instanceof SQLAlterTableAddConstraint) {
                    SQLConstraint constraint = ((SQLAlterTableAddConstraint) item).getConstraint();
                    SQLName indexName = constraint.getName();

                    if (constraint instanceof MySqlUnique && (null == indexName || null == indexName.getSimpleName()
                        || indexName.getSimpleName().isEmpty())) {
                        String realName;
                        String baseName = ((MySqlUnique) ((SQLAlterTableAddConstraint) item).getConstraint())
                            .getIndexDefinition().getColumns().get(0).getExpr().toString();
                        baseName = SQLUtils.normalizeNoTrim(baseName);

                        if (!existsNames.contains(baseName)) {
                            realName = baseName;
                        } else {
                            baseName = baseName + "_";
                            int prob = 2;
                            while (existsNames.contains(baseName + prob)) {
                                ++prob;
                            }
                            realName = baseName + prob;
                            existsNames.add(realName);
                        }
                        ((SQLAlterTableAddConstraint) item).getConstraint().setName(new SQLIdentifierExpr(realName));
                    }
                }
            }
            String cdcRewriteDdlStmt = ForeignKeyUtils.rewriteOriginSqlWithForeignKey(
                logicalDdlPlan, ec, schemaName, tableName,
                stmt.toString(VisitorFeature.OutputHashPartitionsByRange)
            );
            ec.getDdlContext().setCdcRewriteDdlStmt(cdcRewriteDdlStmt);
        }
    }
}
