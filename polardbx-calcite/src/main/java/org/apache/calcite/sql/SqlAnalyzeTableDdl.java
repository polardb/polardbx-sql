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

import java.util.List;

public class SqlAnalyzeTableDdl extends SqlDdl {
    private static final SqlSpecialOperator OPERATOR = new SqlAnalyzeTable.SqlAnalyzeTableOperator();

    private final List<SqlNode> tableNames;

    public SqlAnalyzeTableDdl(SqlParserPos pos, List<SqlNode> tableNames){
        super(OPERATOR, pos);
        this.tableNames = tableNames;
    }

    public List<SqlNode> getTableNames() {
        return tableNames;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("ANALYZE TABLE");

        tableNames.get(0).unparse(writer, leftPrec, rightPrec);
        for (int index = 1; index < tableNames.size(); index++) {
            writer.print(", ");
            tableNames.get(index).unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.ANALYZE_TABLE;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return tableNames;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }
}
