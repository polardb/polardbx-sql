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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-06-09 12:22
 */
public class SqlRenameSequence extends SqlSequence {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP SEQUENCE", SqlKind.RENAME_SEQUENCE);

    /**
     * Creates a SqlDropSequence.
     *
     * @param seqName
     * @param tableName
     * @param pos
     */
    public SqlRenameSequence(SqlCharStringLiteral seqName ,SqlCharStringLiteral to, SqlIdentifier tableName, String sql, SqlParserPos pos) {
        super(OPERATOR ,SqlParserPos.ZERO);
        this.name = tableName;
        this.sequence = seqName;
        this.newSequence = to;
        this.sourceSql = sql;
    }

    private String sourceSql;

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, sequence);
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return sourceSql;
    }

    public SQLStatement rewrite() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        SQLDropSequenceStatement stmt = (SQLDropSequenceStatement) statementList.get(0);
        return stmt;
    }

    @Override
    public String toString() {
        return prepare();
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect ,sql);
    }

}
