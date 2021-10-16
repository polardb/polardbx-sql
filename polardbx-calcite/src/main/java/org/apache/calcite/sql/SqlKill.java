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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/14 下午1:52
 */
public class SqlKill extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlKillOperator();
    private SqlNode processId;

    public SqlKill(SqlParserPos pos, SqlNode processId) {
        super(pos);
        this.processId = processId;
        this.operands = ImmutableList.of(processId);
    }

    public SqlNode getProcessId() {
        return processId;
    }

    public void setProcessId(SqlNode processId) {
        this.processId = processId;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("KILL");

        this.processId.unparse(writer, leftPrec, rightPrec);

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlKillOperator extends SqlSpecialOperator {

        public SqlKillOperator(){
            super("KILL", SqlKind.KILL);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("KILL", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlKill(this.pos, processId);
    }
}
