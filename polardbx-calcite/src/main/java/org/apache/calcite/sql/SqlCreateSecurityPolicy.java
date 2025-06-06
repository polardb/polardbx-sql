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
 * @author pangzhaoxing
 */
public class SqlCreateSecurityPolicy extends SqlDal{

    private static final SqlOperator OPERATOR =
        new SqlCreateSecurityPolicyOperator();

    private SqlIdentifier policyName;
    private SqlCharStringLiteral policyComponents;

    protected SqlCreateSecurityPolicy(SqlParserPos pos) {
        super(pos);
    }

    public SqlCreateSecurityPolicy(SqlParserPos pos, SqlIdentifier policyName, SqlCharStringLiteral policyComponents) {
        super(pos);
        this.policyName = policyName;
        this.policyComponents = policyComponents;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SECURITY POLICY");
        this.policyName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("COMPONENTS");
        this.policyComponents.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SECURITY_POLICY;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getPolicyName() {
        return policyName;
    }

    public void setPolicyName(SqlIdentifier policyName) {
        this.policyName = policyName;
    }

    public SqlCharStringLiteral getPolicyComponents() {
        return policyComponents;
    }

    public void setPolicyComponents(SqlCharStringLiteral policyComponents) {
        this.policyComponents = policyComponents;
    }

    public static class SqlCreateSecurityPolicyOperator extends SqlSpecialOperator {

        public SqlCreateSecurityPolicyOperator() {
            super("CREATE_SECURITY_POLICY", SqlKind.CREATE_SECURITY_POLICY);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SECURITY_POLICY",
                    0,
                    columnType)));
        }
    }

    

}
