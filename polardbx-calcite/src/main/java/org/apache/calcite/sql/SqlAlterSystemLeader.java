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

/**
 * @author chenghui.lch
 */
public class SqlAlterSystemLeader extends SqlDal {

    public static class SqlAlterSystemLeaderOperator extends SqlSpecialOperator {

        public SqlAlterSystemLeaderOperator() {
            super("ALTER_SYSTEM_LEADER", SqlKind.ALTER_SYSTEM_LEADER);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);
            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_SYSTEM_LEADER_RESULT",
                    0,
                    columnType)));
        }
    }

    private static final SqlSpecialOperator OPERATOR =
        new SqlAlterSystemLeader.SqlAlterSystemLeaderOperator();

    // target host port
    protected SqlNode nodeId;

    public SqlAlterSystemLeader(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.print("ALTER SYSTEM LEADER ");

        nodeId.unparse(writer, leftPrec, rightPrec);
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlNode getTargetStorage() {
        return nodeId;
    }

    public void setTargetStorage(SqlNode targetStorage) {
        this.nodeId = targetStorage;
    }
}
