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

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-06-09 12:22
 */
public class SqlCreateSequence extends SqlSequence {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE SEQUENCE", SqlKind.CREATE_SEQUENCE);

    /**
     * Creates a SqlCreateIndex.
     *
     * @param name
     * @param tableName
     * @param pos
     */
    public SqlCreateSequence(SqlCharStringLiteral name , SqlIdentifier tableName, String sql, SqlParserPos pos) {
        super(OPERATOR ,SqlParserPos.ZERO);
        this.name = tableName;
        this.seqName = name;
        this.sourceSql = sql;
    }

    private SqlCharStringLiteral seqName;
    private String sourceSql;

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, seqName);
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return sourceSql;
    }

    public String rewrite() {
        return sourceSql;
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
