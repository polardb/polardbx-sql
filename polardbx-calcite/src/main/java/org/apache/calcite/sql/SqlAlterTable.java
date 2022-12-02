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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * DESCRIPTION
 *
 * @author
 */
public class SqlAlterTable extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);
    private final Map<ColumnOpt, List<String>> columnOpts;

    public static enum ColumnOpt {
        ADD, MODIFY, CHANGE, DROP
    }

    private final SqlTableOptions tableOptions;
    private final List<SqlAlterSpecification> alters;
    private final SqlIdentifier originTableName;
    private final List<SqlIdentifier> objectNames;

    private List<String> logicalReferencedTables = null;
    private List<String> physicalReferencedTables = null;

    /**
     * Creates a SqlCreateIndex.
     */
    public SqlAlterTable(List<SqlIdentifier> objectNames, SqlIdentifier tableName, Map<ColumnOpt, List<String>> columnOpts, String sql,
                         SqlTableOptions tableOptions, List<SqlAlterSpecification> alters, SqlParserPos pos) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.tableOptions = tableOptions;
        this.alters = alters;
        this.name = tableName;
        this.originTableName = tableName;
        this.sourceSql = sql;
        this.columnOpts = columnOpts;
        this.objectNames = objectNames;
    }

    public SqlAlterTable(List<SqlIdentifier> objectNames, SqlIdentifier tableName, Map<ColumnOpt, List<String>> columnOpts, String sql,
                         SqlParserPos pos) {
        this(objectNames, tableName, columnOpts, sql, null, null, pos);
    }

    private SequenceBean autoIncrement;

    public SequenceBean getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(SequenceBean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    private SqlIdentifier newTableName;
    //sourceSql maybe change
    private String sourceSql;
    //originalSql is the same as what user input
    private String originalSql;

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, newTableName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean withOriginTableName) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER TABLE", "");

        if (withOriginTableName) {
            originTableName.unparse(writer, leftPrec, rightPrec);
        } else {
            name.unparse(writer, leftPrec, rightPrec);
        }

        if (null != tableOptions) {
            tableOptions.unparse(writer, leftPrec, rightPrec);
        }

        SqlUtil.wrapSqlNodeList(alters).unparse(writer, 0, 0);

        writer.endList(frame);
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        String sqlForExecute = sourceSql;
        if (alters.size() == 1 && alters.get(0).isA(SqlKind.ALTER_ADD_INDEX)) {
            final SqlAddIndex addIndex = (SqlAddIndex) alters.get(0);

            if (addIndex.getIndexDef().isGlobal()) {
                // generate CREATE INDEX for executing on MySQL
                SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
                writer.setAlwaysUseParentheses(true);
                writer.setSelectListItemsOnSeparateLines(false);
                writer.setIndentation(0);
                final int leftPrec = getOperator().getLeftPrec();
                final int rightPrec = getOperator().getRightPrec();
                unparse(writer, leftPrec, rightPrec, true);
                sqlForExecute = writer.toSqlString().getSql();
            }
        } else if (alters.size() == 1 && alters.get(0).getKind() == SqlKind.TRUNCATE_PARTITION) {
            return alters.get(0).toString();
        }

        List<SQLStatement> statementList = SQLUtils.parseStatements(sqlForExecute, JdbcConstants.MYSQL);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) statementList.get(0);
        if (this.name instanceof SqlDynamicParam) {
            StringBuilder sql = new StringBuilder();
            MySqlOutputVisitor questionarkTableSource = new MySqlOutputVisitor(sql) {
                @Override
                protected void printTableSourceExpr(SQLExpr expr) {
                    print("?");
                }

                @Override
                public boolean visit(MySqlRenameTableStatement.Item x) {
                    print("?");
                    print0(ucase ? " TO " : " to ");
                    print("?");
                    return false;
                }

                @Override
                protected void printReferencedTableName(SQLExpr expr) {
                    if (!ConfigDataMode.isFastMock() && logicalReferencedTables != null) {
                        String referencedTableName = null;
                        if (expr instanceof SQLIdentifierExpr) {
                            referencedTableName = ((SQLIdentifierExpr) expr).getSimpleName();
                        } else if (expr instanceof SQLPropertyExpr) {
                            referencedTableName = ((SQLPropertyExpr) expr).getSimpleName();
                        }
                        if (TStringUtil.isNotEmpty(referencedTableName) &&
                            logicalReferencedTables.contains(referencedTableName)) {
                            print("?");
                        } else {
                            super.printReferencedTableName(expr);
                        }
                    } else {
                        super.printReferencedTableName(expr);
                    }
                }
            };
            questionarkTableSource.visit(stmt);
            return sql.toString();
        }
        return stmt.toString();
    }

    public MySqlStatement rewrite() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
        return stmt;
    }

    @Override
    public SqlNode getTargetTable() {
        return super.getTargetTable();
    }

    public SqlNodeList getTargetTables() {
        final SqlNodeList empty = SqlNodeList.EMPTY;
        empty.add(name);
        empty.add(newTableName);
        return empty;
    }

    @Override
    public String toString() {
        return prepare();
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public String getSourceSql() {
        return this.sourceSql;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public String getRenamedName(String tableName) {
        return newTableName.getSimple();
    }

    public SqlIdentifier getRenamedNode(SqlNode tableName) {
        return newTableName;
    }

    public Map<ColumnOpt, List<String>> getColumnOpts() {
        return columnOpts;
    }

    public SqlTableOptions getTableOptions() {
        return tableOptions;
    }

    public List<SqlAlterSpecification> getAlters() {
        return alters;
    }

    @Override
    public boolean createGsi() {
        return addIndex() && ((SqlAddIndex) alters.get(0)).indexDef.isGlobal();
    }

    public boolean isAllocateLocalPartition() {
        return alters != null && alters.size() == 1 && alters.get(0) instanceof SqlAlterTableAllocateLocalPartition;
    }

    public boolean isExpireLocalPartition() {
        return alters != null && alters.size() == 1 && alters.get(0) instanceof SqlAlterTableExpireLocalPartition;
    }

    public boolean createClusteredIndex() {
        return addIndex() && ((SqlAddIndex) alters.get(0)).indexDef.isClustered();
    }

    /**
     * ALTER TABLE CHANGE [COLUMN] old_col_name new_col_name column_definition [FIRST|AFTER col_name]
     */
    public boolean changeColumn() {
        return (alters.size() > 0 && alters.get(0) instanceof SqlChangeColumn);
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlAlterTable replaceTableName(SqlIdentifier newTableName) {
        List<SqlAlterSpecification> newAlters = new ArrayList<>();

        for (SqlAlterSpecification alterItem : getAlters()) {
            newAlters.add(alterItem.replaceTableName(newTableName));
        }

        return new SqlAlterTable(objectNames, newTableName,
            new HashMap<>(columnOpts),
            sourceSql,
            tableOptions,
            newAlters,
            getParserPosition());
    }

    public SqlAlterTable removeAfterColumns() {
        List<SqlAlterSpecification> newAlters = new ArrayList<>();

        for (SqlAlterSpecification alterItem : getAlters()) {
            newAlters.add(alterItem.removeAfterColumn());
        }

        SqlAlterTable sqlAlterTable = new SqlAlterTable(objectNames, null,
            new HashMap<>(columnOpts),
            genSourceSqlWithOutAfter(sourceSql),
            tableOptions,
            newAlters,
            getParserPosition());
        sqlAlterTable.setTargetTable(name);
        sqlAlterTable.setAutoIncrement(autoIncrement);

        return sqlAlterTable;
    }

    public static String genSourceSqlWithOutAfter(String sourceSql) {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) statementList.get(0);

        for (SQLAlterTableItem item : stmt.getItems()) {
            if (item instanceof MySqlAlterTableModifyColumn) {
                ((MySqlAlterTableModifyColumn) item).setAfterColumn(null);
            } else if (item instanceof MySqlAlterTableChangeColumn) {
                ((MySqlAlterTableChangeColumn) item).setAfterColumn(null);
            }
        }

        return stmt.toString();
    }

    public boolean addIndex() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAddIndex;
    }

    public boolean dropIndex() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableDropIndex;
    }

    public boolean renameIndex() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableRenameIndex;
    }

    public boolean dropColumn() {
        return alters.size() > 0 && alters.get(0) instanceof SqlDropColumn;
    }

    public boolean isAddPartition() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableAddPartition;
    }

    public boolean isDropPartition() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableDropPartition;
    }

    public boolean isTruncatePartition() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableTruncatePartition;
    }

    public boolean isModifyPartitionValues() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableModifyPartitionValues;
    }

    public List<String> getLogicalReferencedTables() {
        return logicalReferencedTables;
    }

    public void setLogicalReferencedTables(List<String> logicalReferencedTables) {
        this.logicalReferencedTables = logicalReferencedTables;
    }

    public List<String> getPhysicalReferencedTables() {
        return physicalReferencedTables;
    }

    public void setPhysicalReferencedTables(List<String> physicalReferencedTables) {
        this.physicalReferencedTables = physicalReferencedTables;
    }

    public List<SqlIdentifier> getObjectNames() {
        return objectNames;
    }

    public boolean isExchangePartition() {
        return alters != null && alters.size() == 1 && alters.get(0) instanceof SqlAlterTableExchangePartition;
    }

    public boolean isDropFile() {
        return alters.size() > 0 && alters.get(0) instanceof SqlAlterTableDropFile;
    }
}
