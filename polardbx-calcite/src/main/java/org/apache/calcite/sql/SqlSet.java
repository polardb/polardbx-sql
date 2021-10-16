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
import org.apache.calcite.util.Pair;
import com.google.common.collect.ImmutableList;

/**
 * @author chenmo.cm
 * @date 2018/6/14 下午5:40
 */
public class SqlSet extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlSetStatementOperator();

    List<Pair<SqlNode, SqlNode>>            variableAssignmentList;

    public SqlSet(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> variableAssignmentList){
        super(pos);
        this.variableAssignmentList = variableAssignmentList;
    }

    public List<Pair<SqlNode, SqlNode>> getVariableAssignmentList() {
        return variableAssignmentList;
    }

    public void setVariableAssignmentList(List<Pair<SqlNode, SqlNode>> variableAssignmentList) {
        this.variableAssignmentList = variableAssignmentList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SET");
        for (int i = 0; i < variableAssignmentList.size(); i++) {
            if (i > 0) {
                writer.print(",");
            }

            final Pair<SqlNode, SqlNode> varAssignment = variableAssignmentList.get(i);
            varAssignment.getKey().unparse(writer, leftPrec, rightPrec);
            writer.sep("=");
            if (varAssignment.getValue() instanceof SqlIdentifier) {
                writer.print(varAssignment.getValue().toString());
            } else {
                varAssignment.getValue().unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SQL_SET;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlSetStatementOperator extends SqlSpecialOperator {

        public SqlSetStatementOperator(){
            super("SQL_SET", SqlKind.SQL_SET);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}
