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
public class SqlGrantSecurityLabel extends SqlDal{

    private static final SqlOperator OPERATOR =
        new SqlGrantSecurityLabelOperator();

    private SqlIdentifier policyName;
    private SqlIdentifier labelName;
    private SqlUserName userName;
    private SqlIdentifier accessType;

    protected SqlGrantSecurityLabel(SqlParserPos pos) {
        super(pos);
    }

    public SqlGrantSecurityLabel(SqlParserPos pos, SqlIdentifier policyName, SqlIdentifier labelName,
                                 SqlUserName userName, SqlIdentifier accessType) {
        super(pos);
        this.policyName = policyName;
        this.labelName = labelName;
        this.userName = userName;
        this.accessType = accessType;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("GRANT SECURITY LABEL");
        writer.keyword(policyName.toString() + "." + labelName.toString());
        writer.keyword("TO USER");
        this.userName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("FOR");
        this.accessType.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ACCESS");
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GRANT_SECURITY_LABEL;
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

    public SqlIdentifier getLabelName() {
        return labelName;
    }

    public void setLabelName(SqlIdentifier labelName) {
        this.labelName = labelName;
    }

    public SqlUserName getUserName() {
        return userName;
    }

    public void setUserName(SqlUserName userName) {
        this.userName = userName;
    }

    public SqlIdentifier getAccessType() {
        return accessType;
    }

    public void setAccessType(SqlIdentifier accessType) {
        this.accessType = accessType;
    }

    public static class SqlGrantSecurityLabelOperator extends SqlSpecialOperator {

        public SqlGrantSecurityLabelOperator() {
            super("GRANT_SECURITY_LABEL", SqlKind.GRANT_SECURITY_LABEL);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("GRANT_SECURITY_LABEL",
                    0,
                    columnType)));
        }
    }

}
