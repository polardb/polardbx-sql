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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class SqlChangeConsensusRole extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlChangeConsensusLeaderOperator();
    private SqlNode role;
    private SqlNode targetType;
    private SqlNode target;

    public SqlChangeConsensusRole(SqlParserPos pos, SqlNode targetType, SqlNode target, SqlNode role) {
        super(OPERATOR, pos);
        this.role = role;
        this.targetType = targetType;
        this.target = target;
    }

    public SqlNode getRole() {
        return role;
    }

    public void setRole(SqlNode role) {
        this.role = role;
    }

    public SqlNode getTargetType() {
        return targetType;
    }

    public void setTargetType(SqlNode targetType) {
        this.targetType = targetType;
    }

    public SqlNode getTarget() {
        return target;
    }

    public void setTarget(SqlNode target) {
        this.target = target;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHANGE_CONSENSUS_ROLE;
    }

    @Override
    public void validate(SqlValidator valitor, SqlValidatorScope scope) {
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER SYSTEM CHANGE_ROLE ");
        writer.print(this.targetType.toString());
        writer.print(this.target.toString());
        writer.keyword(" TO ");
        writer.print(this.role.toString());
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlChangeConsensusLeaderOperator extends SqlSpecialOperator {

        public SqlChangeConsensusLeaderOperator() {
            super("CHANGE_CONSENSUS_LEADER", SqlKind.CHANGE_CONSENSUS_ROLE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(ImmutableList.of((RelDataTypeField)
                new RelDataTypeFieldImpl("CHANGE_CONSENSUS_LEADER_RESULT", 0, columnType)));
        }
    }
}
