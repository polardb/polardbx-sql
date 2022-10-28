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

import java.util.Arrays;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableSetTableGroup extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE_SET_TABLEGROUP);

    final String sourceSql;

    final String targetTableGroup;
    final List<SqlIdentifier> objectNames;
    final boolean force;

    public SqlAlterTableSetTableGroup(List<SqlIdentifier> objectNames, SqlIdentifier tableName, String targetTableGroup, String sql, SqlParserPos pos, boolean force){
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.sourceSql = sql;
        this.targetTableGroup = targetTableGroup;
        this.objectNames = objectNames;
        this.force = force;
    }

    public String getTargetTableGroup() {
        return targetTableGroup;
    }

    public List<SqlIdentifier> getObjectNames() {
        return objectNames;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public boolean isForce() {
        return force;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

}
