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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlAlterFunction extends SqlAlterDdl{
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER FUNCTION", SqlKind.ALTER_FUNCTION);

    private String text;

    private String tableName;

    private String functionName;

    public SqlAlterFunction(String text, String functionName) {
        super(OPERATOR, SqlParserPos.ZERO);
        this.text = text;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.functionName = functionName;
    }

    public String getText() {
        return text;
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        writer.print(text);
    }

    @Override
    public String toString() {
        return text;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlAlterFunction(text, functionName);
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
        if (node instanceof SqlAlterFunction) {
            if (!text.equals(((SqlAlterFunction) node).getText())) {
                return false;
            }
            return super.equalsDeep(node, litmus, context);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        return;
    }
}
