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
import org.apache.calcite.util.Pair;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.List;

/**
 * DESCRIPTION
 *
 * @author wumu
 */
public class SqlRenameTables extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("RENAME TABLE", SqlKind.RENAME_TABLE);

    private String sourceSql;
    private final List<Pair<SqlIdentifier, SqlIdentifier>> tableNameList;

    public SqlRenameTables(List<Pair<SqlIdentifier, SqlIdentifier>> tableNameList, String sql, SqlParserPos pos) {
        super(OPERATOR, pos, false, false);
        this.sourceSql = sql;
        this.tableNameList = tableNameList;
        /* 设置 rename 的第一个表作为 ddl 主表，因为以前的 ddl 没有考虑多表的情况，暂且先这样设置 */
        setTargetTable(tableNameList.get(0).getKey());
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operandList = new ArrayList<>();
        for (Pair<SqlIdentifier, SqlIdentifier> sqlIdentifierSqlIdentifierPair : tableNameList) {
            operandList.add(sqlIdentifierSqlIdentifierPair.getKey());
            operandList.add(sqlIdentifierSqlIdentifierPair.getValue());
        }
        return operandList;
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

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    @Override
    public String toString() {
        return prepare();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("RENAME TABLE");

        for (int i = 0; i < tableNameList.size(); ++i) {
            Pair<SqlIdentifier, SqlIdentifier> item = tableNameList.get(i);

            item.getKey().unparse(writer, leftPrec, rightPrec);

            writer.keyword("TO");

            item.getValue().unparse(writer, leftPrec, rightPrec);

            if (i != tableNameList.size() - 1) {
                writer.keyword(",");
            }
        }

        writer.endList(selectFrame);
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public List<Pair<SqlIdentifier, SqlIdentifier>> getTableNameList() {
        return tableNameList;
    }
}
