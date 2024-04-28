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
import org.apache.commons.lang3.StringUtils;

/**
 * @author pangzhaoxing
 */
public class SqlCreateSecurityLabelComponent extends SqlDal{

    private static final SqlOperator OPERATOR =
        new SqlCreateSecurityLabelComponentOperator();

    private SqlIdentifier componentName;
    private SqlIdentifier componentType;
    private SqlCharStringLiteral componentContent;

    public SqlCreateSecurityLabelComponent(SqlParserPos pos, SqlIdentifier componentName, SqlIdentifier componentType,
                                           SqlCharStringLiteral componentContent) {
        super(pos);
        this.componentName = componentName;
        this.componentType = componentType;
        this.componentContent = componentContent;
    }

    protected SqlCreateSecurityLabelComponent(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SECURITY_LABEL_COMPONENT;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SECURITY LABEL COMPONENT");
        this.componentName.unparse(writer, leftPrec, rightPrec);
        this.componentType.unparse(writer, leftPrec, rightPrec);
        this.componentContent.unparse(writer, leftPrec, rightPrec);
    }

    public static class SqlCreateSecurityLabelComponentOperator extends SqlSpecialOperator {

        public SqlCreateSecurityLabelComponentOperator() {
            super("CREATE_SECURITY_LABEL_COMPONENT", SqlKind.CREATE_SECURITY_LABEL_COMPONENT);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SECURITY_LABEL_COMPONENT",
                    0,
                    columnType)));
        }
    }

    public SqlIdentifier getComponentName() {
        return componentName;
    }

    public void setComponentName(SqlIdentifier componentName) {
        this.componentName = componentName;
    }

    public SqlIdentifier getComponentType() {
        return componentType;
    }

    public void setComponentType(SqlIdentifier componentType) {
        this.componentType = componentType;
    }

    public SqlCharStringLiteral getComponentContent() {
        return componentContent;
    }

    public void setComponentContent(SqlCharStringLiteral componentContent) {
        this.componentContent = componentContent;
    }
}
