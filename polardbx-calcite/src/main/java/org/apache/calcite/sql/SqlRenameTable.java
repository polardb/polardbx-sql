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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;

/**
 * DESCRIPTION
 *
 * @author
 */
public class SqlRenameTable extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("RENAME TABLE", SqlKind.RENAME_TABLE);

    /**
     * Creates a SqlCreateIndex.
     */
    public SqlRenameTable(SqlIdentifier newTableName, SqlIdentifier tableName, String sql, SqlParserPos pos) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.newTableName = newTableName;
        this.sourceSql = sql;
        this.originTableName = tableName;
        this.originNewTableName = newTableName;
    }

    /**
     * Creates a SqlCreateIndex.
     */
    public SqlRenameTable(SqlIdentifier newTableName, SqlIdentifier tableName, SqlParserPos pos) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.newTableName = newTableName;
        this.sourceSql = constructSql();
        this.originTableName = tableName;
        this.originNewTableName = newTableName;
    }

    private String constructSql() {
        StringBuilder sqlBuilder = new StringBuilder(OPERATOR.getName());
        sqlBuilder.append(" ")
            .append(((SqlIdentifier) name).toStringWithBacktick())
            .append(" ")
            .append("TO")
            .append(" ")
            .append(newTableName.toStringWithBacktick());
        return sqlBuilder.toString();
    }

    private SqlIdentifier newTableName;
    private String sourceSql;
    private final SqlIdentifier originTableName;
    private final SqlIdentifier originNewTableName;

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, newTableName);
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        MySqlRenameTableStatement stmt = (MySqlRenameTableStatement) statementList.get(0);
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

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("RENAME TABLE");

        name.unparse(writer, leftPrec, rightPrec);

        writer.keyword("TO");

        newTableName.unparse(writer, leftPrec, rightPrec);

        writer.endList(selectFrame);
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public String getRenamedName() {
        if (newTableName.isSimple()) {
            return newTableName.getSimple();
        } else {
            return newTableName.getLastName();
        }
    }

    public SqlIdentifier getRenamedNode() {
        return newTableName;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlIdentifier getOriginNewTableName() {
        return originNewTableName;
    }
}
