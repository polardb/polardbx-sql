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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;

public class SqlPhyDdlWrapper extends SqlDdl {

    protected static final SqlOperator DDL_OPERATOR = new SqlSpecialOperator("DDL", SqlKind.OTHER_DDL);

    protected String phySql;

    /**
     * Creates a SqlDdl.
     */
    private SqlPhyDdlWrapper(SqlOperator sqlOperator, String phySql) {
        super(sqlOperator, SqlParserPos.ZERO);
        this.phySql = phySql;
    }

    public static SqlPhyDdlWrapper createForAllocateLocalPartition(SqlIdentifier tableName, String phySql) {
        SqlPhyDdlWrapper sqlPhyDdlWrapper =
            new SqlPhyDdlWrapper(new SqlSpecialOperator("DDL", SqlKind.ALTER_TABLE), phySql);
        sqlPhyDdlWrapper.name = tableName;
        return sqlPhyDdlWrapper;
    }

    @Override
    public String toString() {
        return phySql;
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect, phySql);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }
}
