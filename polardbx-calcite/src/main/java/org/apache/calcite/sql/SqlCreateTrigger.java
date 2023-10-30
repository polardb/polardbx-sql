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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTriggerStatement;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlCreateTrigger extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TRIGGER", SqlKind.CREATE_TRIGGER);

    private SQLCreateTriggerStatement sqlCreateTriggerStatement;

    private String tableName;

    public SqlCreateTrigger(SQLCreateTriggerStatement sqlCreateTriggerStatement) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.sqlCreateTriggerStatement = sqlCreateTriggerStatement;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        writer.print(sqlCreateTriggerStatement.toString());
    }

    @Override
    public String toString() {
        return sqlCreateTriggerStatement.toString();
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCreateTrigger(sqlCreateTriggerStatement);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>();
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (node == this) {
            return true;
        }
        if (node instanceof SqlCreateTrigger) {
            tableName.equals(((SqlCreateTrigger) node).getTableName());
            sqlCreateTriggerStatement.toString().equals(((SqlCreateTrigger) node).getSqlCreateTriggerStatement().toString());
            return super.equalsDeep(node, litmus, context);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public SQLCreateTriggerStatement getSqlCreateTriggerStatement() {
        return sqlCreateTriggerStatement;
    }
}

