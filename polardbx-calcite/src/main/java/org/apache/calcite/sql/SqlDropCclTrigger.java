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

/**
 * @author busu
 * date: 2021/4/18 4:22 下午
 */
public class SqlDropCclTrigger extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlDropCclTriggerOperator();

    private List<SqlIdentifier> names;

    private boolean ifExists;

    public SqlDropCclTrigger(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_CCL_TRIGGER;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("DROP");
        writer.sep("CCL_TRIGGER");
        if (ifExists) {
            writer.sep("IF EXISTS");
        }
        if (names != null) {
            if (!names.isEmpty()) {
                writer.print(names.get(0).getSimple());
            }
            for (int i = 1; i < names.size(); ++i) {
                writer.print(", ");
                writer.print(names.get(i).getSimple());
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlDropCclTriggerOperator extends SqlSpecialOperator {
        public SqlDropCclTriggerOperator() {
            super("DROP_CCL_RIGGER", SqlKind.DROP_CCL_TRIGGER);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(
                    ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("Drop_Ccl_Trigger_Result",
                        0,
                        columnType)));
        }
    }

    public List<SqlIdentifier> getNames() {
        return names;
    }

    public void setNames(List<SqlIdentifier> names) {
        this.names = names;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }
}
