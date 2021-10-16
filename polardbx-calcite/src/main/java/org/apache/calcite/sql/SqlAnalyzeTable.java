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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/14 下午12:48
 */
public class SqlAnalyzeTable extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlAnalyzeTableOperator();

    private final List<SqlNode>             tableNames;

    public SqlAnalyzeTable(SqlParserPos pos, List<SqlNode> tableNames){
        super(pos);
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

    public static class SqlAnalyzeTableOperator extends SqlSpecialOperator {

        public SqlAnalyzeTableOperator(){
            super("ANALYZE_TABLE", SqlKind.ANALYZE_TABLE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Op", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_type", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
