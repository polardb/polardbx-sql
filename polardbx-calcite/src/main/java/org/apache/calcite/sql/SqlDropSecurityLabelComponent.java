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
 * @author pangzhaoxing
 */
public class SqlDropSecurityLabelComponent extends SqlDal{

    private static final SqlOperator OPERATOR = new SqlDropSecurityLabelComponentOperator();

    private List<SqlIdentifier> componentNames;

    public SqlDropSecurityLabelComponent(SqlParserPos pos, List<SqlIdentifier> componentNames) {
        super(pos);
        this.componentNames = componentNames;
    }

    public List<SqlIdentifier> getComponentNames() {
        return componentNames;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP SECURITY LABEL COMPONENT");
        componentNames.get(0).unparse(writer, leftPrec, rightPrec);
        for (int i = 1; i < componentNames.size(); i++){
            writer.keyword(",");
            componentNames.get(i).unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_SECURITY_LABEL_COMPONENT;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlDropSecurityLabelComponentOperator extends SqlSpecialOperator {

        public SqlDropSecurityLabelComponentOperator() {
            super("DROP_SECURITY_LABEL_COMPONENT", SqlKind.DROP_SECURITY_LABEL_COMPONENT);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_SECURITY_LABEL_COMPONENT",
                    0,
                    columnType)));
        }
    }

}
