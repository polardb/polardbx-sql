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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
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
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
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
import org.apache.calcite.sql.SqlDropPrimaryKey;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_KEY_NAME;
import static com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler.reorgLogicalColumnOrder;

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

        rewriteOriginSqlWithForeignKey(logicalDdlPlan, executionContext, schemaName, objectName);
    }

    protected void handleDdlRequest(DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob instanceof TransientDdlJob) {
            return;
        }
        DdlContext ddlContext = executionContext.getDdlContext();
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

    protected Pair<String, SqlCreateTable> genPrimaryTableInfoAfterModifyColumn(BaseDdlOperation logicalDdlPlan,
                                                                                ExecutionContext executionContext,
                                                                                Map<String, String> virtualColumnMap,
                                                                                Map<String, String> columnNewDef,
                                                                                List<String> oldPrimaryKeys) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);
        oldPrimaryKeys.addAll(primaryTableInfo.getValue().getPrimaryKey().getColumns()
            .stream().map(e -> e.getColumnNameStr().toLowerCase()).collect(Collectors.toList()));

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();

        final List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(primaryTableInfo.getKey(), JdbcConstants.MYSQL);
        final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        boolean isFirst = false;
        String alterSourceSql;
        SqlIdentifier colName = null;
        SqlIdentifier afterColName = null;
        SqlColumnDeclaration newColumnDef = null;
        SQLColumnDefinition newColumnDefinition = null;
        for (SqlAlterSpecification sqlAlterSpecification : sqlAlterTable.getAlters()) {
            if (sqlAlterSpecification instanceof SqlModifyColumn) {
                SqlModifyColumn sqlModifyColumn = (SqlModifyColumn) sqlAlterSpecification;
                isFirst = sqlModifyColumn.isFirst();
                afterColName = sqlModifyColumn.getAfterColumn();
                colName = sqlModifyColumn.getColName();
                alterSourceSql = sqlModifyColumn.getSourceSql();

                final List<SQLStatement> alterStatement =
                    SQLUtils.parseStatementsWithDefaultFeatures(alterSourceSql, JdbcConstants.MYSQL);
                newColumnDefinition = ((MySqlAlterTableModifyColumn) alterStatement.get(0)
                    .getChildren().get(1)).getNewColumnDefinition();
            } else if (sqlAlterSpecification instanceof SqlChangeColumn) {
                SqlChangeColumn sqlChangeColumn = (SqlChangeColumn) sqlAlterSpecification;
                if (!sqlChangeColumn.getOldName()
                    .equalsDeep(sqlChangeColumn.getNewName(), Litmus.IGNORE, EqualsContext.DEFAULT_EQUALS_CONTEXT)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support changing column name of sharding key");
                }

                isFirst = sqlChangeColumn.isFirst();
                afterColName = sqlChangeColumn.getAfterColumn();
                colName = sqlChangeColumn.getOldName();
                alterSourceSql = sqlChangeColumn.getSourceSql();

                final List<SQLStatement> alterStatement =
                    SQLUtils.parseStatementsWithDefaultFeatures(alterSourceSql, JdbcConstants.MYSQL);
                newColumnDefinition = ((MySqlAlterTableChangeColumn) alterStatement.get(0)
                    .getChildren().get(1)).getNewColumnDefinition();
            } else if (sqlAlterSpecification instanceof SqlDropPrimaryKey) {
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
                continue;
            } else if (sqlAlterSpecification instanceof SqlAddPrimaryKey) {
                SqlAddPrimaryKey sqlAddPrimaryKey = (SqlAddPrimaryKey) sqlAlterSpecification;
                final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
                Set<String> colNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                sqlAddPrimaryKey.getColumns().forEach(e -> colNameSet.add(e.getColumnNameStr()));

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
                        if (StringUtils.equalsIgnoreCase(IMPLICIT_COL_NAME,
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
                List<SQLSelectOrderByItem> colNames = sqlAddPrimaryKey.getColumns().stream()
                    .map(e -> new SQLSelectOrderByItem(new SQLIdentifierExpr(e.getColumnNameStr())))
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
                continue;
            } else {
                continue;
            }

            int first = -1;
            int m = 0;
            boolean isAutoIncrement = false;
            for (; m < stmt.getTableElementList().size(); ++m) {
                SQLTableElement tableElement = stmt.getTableElementList().get(m);
                if (tableElement instanceof SQLColumnDefinition) {
                    first = first == -1 ? m : first;
                    final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                    final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                    if (columnName.equalsIgnoreCase(colName.getLastName())) {
                        isAutoIncrement = columnDefinition.isAutoIncrement();
                        stmt.getTableElementList().remove(m);
                        break;
                    }
                }
            }

            int n = 0;
            if (afterColName != null) {
                for (; n < stmt.getTableElementList().size(); ++n) {
                    SQLTableElement tableElement = stmt.getTableElementList().get(n);
                    if (tableElement instanceof SQLColumnDefinition) {
                        final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                        final String columnName =
                            SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                        if (columnName.equalsIgnoreCase(afterColName.getLastName())) {
                            break;
                        }
                    }
                }
            }

            if (isFirst) {
                stmt.getTableElementList().add(first, newColumnDefinition);
            } else if (afterColName != null) {
                stmt.getTableElementList().add(n + 1, newColumnDefinition);
            } else {
                stmt.getTableElementList().add(m, newColumnDefinition);
            }

            if (!isAutoIncrement) {
                String colNameStr = colName.getLastName();
                virtualColumnMap.put(colNameStr, GsiUtils.generateRandomGsiName(colNameStr));
                columnNewDef.put(colNameStr, TableColumnUtils.getDataDefFromColumnDefNoDefault(newColumnDefinition));
            }
        }

        String sourceSql = stmt.toString();
        SqlCreateTable primaryTableNode =
            (SqlCreateTable) new FastsqlParser().parse(sourceSql, executionContext).get(0);

        return new Pair<>(sourceSql, primaryTableNode);
    }

    protected boolean isAvailableForRecycleBin(String tableName, ExecutionContext executionContext) {
        final String appName = executionContext.getAppName();
        final RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(appName);
        return executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_RECYCLEBIN) &&
            !RecycleBin.isRecyclebinTable(tableName) &&
            recycleBin != null && !recycleBin.hasForeignConstraint(appName, tableName);
    }

    protected void rewriteOriginSqlWithForeignKey(BaseDdlOperation logicalDdlPlan, ExecutionContext ec,
                                                  String schemaName, String tableName) {
        // rewrite origin sql for different naming behaviours in 5.7 & 8.0
        boolean createTableWithFk = logicalDdlPlan.getDdlType() == DdlType.CREATE_TABLE
            && !((LogicalCreateTable) logicalDdlPlan).getSqlCreateTable().getAddedForeignKeys().isEmpty();
        boolean alterTableAddFk = logicalDdlPlan.getDdlType() == DdlType.ALTER_TABLE
            && ((LogicalAlterTable) logicalDdlPlan).getSqlAlterTable().getAlters().get(0).getKind()
            == SqlKind.ADD_FOREIGN_KEY;
        boolean alterTableDropFk = logicalDdlPlan.getDdlType() == DdlType.ALTER_TABLE
            && ((LogicalAlterTable) logicalDdlPlan).getSqlAlterTable().getAlters().get(0).getKind()
            == SqlKind.DROP_FOREIGN_KEY;
        if (createTableWithFk) {
            ec.getDdlContext().setForeignKeyOriginalSql(
                ((LogicalCreateTable) logicalDdlPlan).getSqlCreateTable().toString());
        } else if (alterTableAddFk) {
            final SqlAlterTable sqlTemplate = ((LogicalAlterTable) logicalDdlPlan).getSqlAlterTable();

            SqlAddForeignKey sqlAddForeignKey =
                (SqlAddForeignKey) ((LogicalAlterTable) logicalDdlPlan).getSqlAlterTable().getAlters().get(0);
            // create foreign key constraints symbol
            String symbol =
                ForeignKeyUtils.getForeignKeyConstraintName(schemaName, tableName);
            if (sqlAddForeignKey.getConstraint() == null) {
                sqlAddForeignKey.setConstraint(new SqlIdentifier(SQLUtils.normalizeNoTrim(symbol), SqlParserPos.ZERO));
            }
            SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer.setAlwaysUseParentheses(true);
            writer.setSelectListItemsOnSeparateLines(false);
            writer.setIndentation(0);
            final int leftPrec = sqlTemplate.getOperator().getLeftPrec();
            final int rightPrec = sqlTemplate.getOperator().getRightPrec();
            sqlTemplate.getAlters().clear();
            sqlTemplate.getAlters().add(sqlAddForeignKey);
            sqlTemplate.unparse(writer, leftPrec, rightPrec, true);

            ec.getDdlContext().setForeignKeyOriginalSql(writer.toSqlString().getSql());
        } else if (alterTableDropFk) {
            ec.getDdlContext().setForeignKeyOriginalSql(ec.getOriginSql());
        }
    }
}
