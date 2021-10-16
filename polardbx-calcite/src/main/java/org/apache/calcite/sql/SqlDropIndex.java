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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class SqlDropIndex extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP INDEX", SqlKind.DROP_INDEX);

    /**
     * Creates a SqlCreateIndex.
     */
    public SqlDropIndex(SqlIdentifier indexName, SqlIdentifier tableName, String sql, SqlParserPos pos) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.indexName = indexName;
        this.sourceSql = sql;
        this.originTableName = tableName;
    }

    private SqlIdentifier originTableName;
    private SqlIdentifier indexName;
    private String sourceSql;

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, indexName);
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        SQLDropIndexStatement stmt = (SQLDropIndexStatement) statementList.get(0);
        Set<String> shardKeys = new HashSet<>();
        if (this.name instanceof SqlDynamicParam) {
            StringBuilder sql = new StringBuilder();
            MySqlOutputVisitor questionarkTableSource = new MySqlOutputVisitor(sql) {
                @Override
                protected void printTableSourceExpr(SQLExpr expr) {
                    print("?");
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
    public String toString() {
        return prepare();
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DROP INDEX", "");

        indexName.unparse(writer, leftPrec, rightPrec);

        writer.keyword("TO");

        name.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public SqlDropIndex replaceTableName(SqlIdentifier newTableName) {
        return new SqlDropIndex(indexName, null == newTableName ? originTableName : newTableName, sourceSql, pos);
    }

    public SqlDropIndex replaceIndexName(SqlIdentifier newIndexName) {
        return new SqlDropIndex(newIndexName, originTableName, sourceSql, pos);
    }
}
