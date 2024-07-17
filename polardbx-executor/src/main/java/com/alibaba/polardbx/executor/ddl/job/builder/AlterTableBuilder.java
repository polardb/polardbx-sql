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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan builder for ALTER TABLE
 *
 * @author moyi
 * @since 2021/07
 */
public class AlterTableBuilder extends DdlPhyPlanBuilder {

    protected final AlterTablePreparedData preparedData;
    protected LogicalAlterTable logicalAlterTable;

    protected AlterTableBuilder(DDL ddl, AlterTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    protected AlterTableBuilder(DDL ddl, AlterTablePreparedData preparedData, LogicalAlterTable logicalAlterTable,
                                ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
        this.logicalAlterTable = logicalAlterTable;
    }

    public static AlterTableBuilder createGsiAddColumnsBuilder(String schemaName,
                                                               String logicalTableName,
                                                               String sql,
                                                               List<String> columns,
                                                               ExecutionContext executionContext,
                                                               RelOptCluster cluster) {
        SqlIdentifier logicalTableNameNode = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
        Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
        columnOpts.put(SqlAlterTable.ColumnOpt.ADD, columns);

        SqlAlterTable sqlAlterTable =
            SqlDdlNodes.alterTable(null, logicalTableNameNode, columnOpts, sql, null, new ArrayList<>(),
                SqlParserPos.ZERO);

        AlterTable alterTable = AlterTable.create(cluster, sqlAlterTable, logicalTableNameNode, null);

        LogicalAlterTable logicalAlterTable = LogicalAlterTable.create(alterTable);
        logicalAlterTable.setSchemaName(schemaName);
        logicalAlterTable.prepareData();
        logicalAlterTable.getAlterTablePreparedData().setBackfillColumns(columns);

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return new AlterPartitionTableBuilder(alterTable, logicalAlterTable.getAlterTablePreparedData(),
                logicalAlterTable,
                executionContext);
        }
        return new AlterTableBuilder(alterTable, logicalAlterTable.getAlterTablePreparedData(), logicalAlterTable,
            executionContext);
    }

    public static AlterTableBuilder createAlterTableBuilder(String schemaName,
                                                            AlterTable alterTable,
                                                            ExecutionContext executionContext) {
        LogicalAlterTable logicalAlterTable = LogicalAlterTable.create(alterTable);
        logicalAlterTable.setSchemaName(schemaName);
        logicalAlterTable.prepareData();

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return new AlterPartitionTableBuilder(alterTable, logicalAlterTable.getAlterTablePreparedData(),
                logicalAlterTable,
                executionContext);
        }
        return new AlterTableBuilder(alterTable, logicalAlterTable.getAlterTablePreparedData(), logicalAlterTable,
            executionContext);
    }

    public static AlterTableBuilder create(DDL ddl,
                                           AlterTablePreparedData preparedData,
                                           ExecutionContext ec) {
        return DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName()) ?
            new AlterPartitionTableBuilder(ddl, preparedData, ec) :
            new AlterTableBuilder(ddl, preparedData, ec);
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        buildAlterReferenceTableTopology();
    }

    @Override
    public void buildPhysicalPlans() {
        if (preparedData.getIsGsi()) {
            relDdl.setSqlNode(((SqlAlterTable) relDdl.getSqlNode()).removeAfterColumns());
        }

        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
        handleInstantAddColumn();
    }

    @Override
    protected void buildSqlTemplate() {
        super.buildSqlTemplate();
        sqlTemplateFkRewrite();
        sqlTemplateWithTableGroupRewrite();
        this.sequenceBean = ((SqlAlterTable) this.sqlTemplate).getAutoIncrement();
    }

    protected void handleInstantAddColumn() {
        PhyDdlTableOperation ddl = this.physicalPlans.get(0);

        boolean instantAddColumnSupported =
            executionContext.getParamManager().getBoolean(ConnectionParams.SUPPORT_INSTANT_ADD_COLUMN) &&
                ddl.getKind() == SqlKind.ALTER_TABLE &&
                ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlAlterTable;

        if (!instantAddColumnSupported) {
            return;
        }

        SqlAlterTable sqlAlterTable = (SqlAlterTable) ddl.getNativeSqlNode();
        List<String> addColumns = LogicalAlterTable.getAlteredColumns(sqlAlterTable, SqlAlterTable.ColumnOpt.ADD);

        // Only support INSTANT ADD COLUMN when all the Alter Table items are Add Column.
        if (instantAddColumnSupported && addColumns != null && addColumns.size() == sqlAlterTable.getAlters().size()) {
            // Check if underlying physical database supports INSTANT ADD COLUMN.
            DataSource dataSource = DdlHelper.getPhyDataSource(preparedData.getSchemaName(), ddl.getDbIndex());
            if (TableInfoManager.isInstantAddColumnSupportedByPhyDb(dataSource, ddl.getDbIndex())) {
                String newSql = reorgColumnsToGenerateNewPhysicalDdl(sqlAlterTable);
                if (TStringUtil.isNotEmpty(newSql)) {
                    String origSql = sqlAlterTable.getSourceSql();
                    sqlAlterTable.setSourceSql(newSql);
                    BytesSql nativeSql = BytesSql.getBytesSql(RelUtils.toNativeSql(sqlAlterTable));
                    for (PhyDdlTableOperation phyDdl : physicalPlans) {
                        phyDdl.setBytesSql(nativeSql);
                    }
                    sqlAlterTable.setSourceSql(origSql);
                }
            }
        }
    }

    private String reorgColumnsToGenerateNewPhysicalDdl(SqlAlterTable sqlAlterTable) {
        SQLAlterTableStatement alterTableStmt =
            (SQLAlterTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(sqlAlterTable.getSourceSql(),
                JdbcConstants.MYSQL).get(0);

        if (GeneralUtil.isNotEmpty(alterTableStmt.getTableOptions())) {
            for (SQLAssignItem tableOption : alterTableStmt.getTableOptions()) {
                if (tableOption.getTarget() instanceof SQLIdentifierExpr &&
                    TStringUtil.equalsIgnoreCase(((SQLIdentifierExpr) tableOption.getTarget()).getName(),
                        Attribute.ALTER_TABLE_COMPRESSION_CLAUSE)) {
                    // XDB doesn't support compression for INSTANT ADD COLUMN.
                    preparedData.setLogicalColumnOrder(false);
                    return null;
                }
            }
        }

        boolean hasUnsupportedOperations = false;
        List<SQLAlterTableItem> addColumnsItems = new ArrayList<>();

        for (SQLAlterTableItem item : alterTableStmt.getItems()) {
            if (item instanceof SQLAlterTableAddColumn) {
                SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) item;

                boolean isPrimaryKey = false;
                SQLColumnDefinition colDef = addColumn.getColumns().get(0);
                if (colDef.getConstraints() != null && !colDef.getConstraints().isEmpty()) {
                    for (SQLColumnConstraint constraint : colDef.getConstraints()) {
                        if (constraint instanceof SQLColumnPrimaryKey) {
                            isPrimaryKey = true;
                            break;
                        }
                    }
                }

                if (isPrimaryKey) {
                    // XDB doesn't support primary key for INSTANT ADD COLUMN except table rebuild.
                    hasUnsupportedOperations = true;
                    break;
                } else if (addColumn.isFirst()) {
                    addColumn.setFirst(false);
                    addColumn.setFirstColumn(null);
                    addColumn.setAfterColumn(null);
                    addColumnsItems.add(addColumn);
                } else if (addColumn.getAfterColumn() != null) {
                    String newColumnName = addColumn.getColumns().get(0).getColumnName();
                    String afterColumnName = addColumn.getAfterColumn().getSimpleName();
                    if (!TStringUtil.equalsIgnoreCase(newColumnName, afterColumnName)) {
                        addColumn.setFirst(false);
                        addColumn.setFirstColumn(null);
                        addColumn.setAfterColumn(null);
                        addColumnsItems.add(addColumn);
                    } else {
                        // Don't allow the invalid operation.
                        hasUnsupportedOperations = true;
                        break;
                    }
                } else {
                    addColumnsItems.add(item);
                }
            } else if (item instanceof MySqlAlterTableOption) {
                MySqlAlterTableOption alterTableOption = (MySqlAlterTableOption) item;
                if (TStringUtil.equalsIgnoreCase(alterTableOption.getName(), Attribute.ALTER_TABLE_ALGORITHM_CLAUSE)) {
                    String algorithm = ((SQLIdentifierExpr) alterTableOption.getValue()).getName();
                    if (TStringUtil.equalsIgnoreCase(algorithm, Attribute.ALTER_TABLE_ALGORITHM_DEFAULT) ||
                        TStringUtil.equalsIgnoreCase(algorithm, Attribute.ALTER_TABLE_ALGORITHM_INSTANT)) {
                        addColumnsItems.add(item);
                    } else {
                        // INSTANT ADD COLUMN doesn't allow other algorithms except "default" or "instant".
                        hasUnsupportedOperations = true;
                        break;
                    }
                } else {
                    // INSTANT ADD COLUMN doesn't allow other options except "algorithm".
                    hasUnsupportedOperations = true;
                    break;
                }
            } else {
                // INSTANT ADD COLUMN doesn't allow to have any other operation except "Add Column".
                hasUnsupportedOperations = true;
                break;
            }
        }

        if (!hasUnsupportedOperations && !addColumnsItems.isEmpty()) {
            // Remove first and after clauses to generate physical
            // Alter Table supported by INSTANT ADD COLUMN.
            alterTableStmt.getItems().clear();
            alterTableStmt.getItems().addAll(addColumnsItems);
            preparedData.setLogicalColumnOrder(true);
            return alterTableStmt.toString();
        }

        preparedData.setLogicalColumnOrder(false);
        return null;
    }

    public AlterTablePreparedData getPreparedData() {
        return preparedData;
    }

    public LogicalAlterTable getLogicalAlterTable() {
        return logicalAlterTable;
    }

    public void buildAlterReferenceTableTopology() {
        // Dealing extra referenced tables.
        if (preparedData.getReferencedTables() != null) {
            for (String referencedTable : preparedData.getReferencedTables()) {
                final List<List<TargetDB>> targetDBs =
                    DataNodeChooser.shardChangeTable(preparedData.getSchemaName(), referencedTable, executionContext);
                if (OptimizerContext.getContext(preparedData.getSchemaName()).getRuleManager()
                    .isBroadCast(referencedTable)) {
                    final String tableName = targetDBs.get(0).get(0).getTableNames().stream().findFirst().orElse(null);
                    assert tableName != null;
                    for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
                        for (List<String> l : entry.getValue()) {
                            l.add(tableName);
                        }
                    }
                } else {
                    final Map<String, List<List<String>>> refTopo =
                        convertTargetDBs(preparedData.getSchemaName(), targetDBs);
                    assert refTopo.size() == tableTopology.size();
                    for (Map.Entry<String, List<List<String>>> entry : refTopo.entrySet()) {
                        final List<List<String>> match = tableTopology.get(entry.getKey());
                        assert match != null;
                        assert match.size() == entry.getValue().size();
                        // Concat one by one.
                        for (int i = 0; i < match.size(); ++i) {
                            match.get(i).addAll(entry.getValue().get(i));
                        }
                    }
                }
            }
        }
    }

    public void sqlTemplateFkRewrite() {
        final SqlAlterTable sqlTemplate = (SqlAlterTable) this.sqlTemplate;
        if (sqlTemplate.getAlters().size() == 0 || !(sqlTemplate.getAlters().get(0) instanceof SqlAddForeignKey)) {
            return;
        }
        SqlAddForeignKey sqlAddForeignKey = (SqlAddForeignKey) sqlTemplate.getAlters().get(0);
        if (sqlAddForeignKey.isPushDown()) {
            return;
        }

        // create foreign key constraints symbol
        String symbol =
            ForeignKeyUtils.getForeignKeyConstraintName(preparedData.getSchemaName(), preparedData.getTableName());

        if (sqlAddForeignKey.getIndexName() == null) {
            // Rewrite foreign key to normal index.
            SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer.setAlwaysUseParentheses(true);
            writer.setSelectListItemsOnSeparateLines(false);
            writer.setIndentation(0);
            final int leftPrec = sqlTemplate.getOperator().getLeftPrec();
            final int rightPrec = sqlTemplate.getOperator().getRightPrec();
            sqlTemplate.getAlters().clear();
            sqlTemplate.getAlters().add(new SqlAddIndex(SqlParserPos.ZERO,
                sqlAddForeignKey.getConstraint() == null ? new SqlIdentifier(symbol, SqlParserPos.ZERO) :
                    sqlAddForeignKey.getConstraint(),
                sqlAddForeignKey.getIndexDef()));
            sqlTemplate.unparse(writer, leftPrec, rightPrec, true);
            sqlTemplate.setSourceSql(writer.toSqlString().getSql());
        }

        this.sqlTemplate = sqlTemplate;
    }

    public void sqlTemplateWithTableGroupRewrite() {
        final SqlAlterTable sqlTemplate = (SqlAlterTable) this.sqlTemplate;
        if (sqlTemplate.getIndexTableGroupMap().isEmpty() && StringUtils.isEmpty(
            sqlTemplate.getTargetImplicitTableGroupName())) {
            return;
        }
        SQLAlterTableStatement alterTableStmt =
            (SQLAlterTableStatement) FastsqlUtils.parseSql(sqlTemplate.getSourceSql()).get(0);
        alterTableStmt.setTargetImplicitTableGroup(null);
        alterTableStmt.getIndexTableGroupPair().clear();
        //alterTableStmt.getTableSource().setExpr("?");
        sqlTemplate.setSourceSql(alterTableStmt.toString());

        this.sqlTemplate = sqlTemplate;
    }
}
