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
public class SqlCreateSecurityEntity extends SqlDal{
    private static final SqlOperator OPERATOR = new SqlCreateSecurityEntity.SqlCreateSecurityEntityOperator();

    private SqlIdentifier entityType;

    private SqlIdentifier entityKey;

    private SqlIdentifier entityAttr;

    public SqlCreateSecurityEntity(SqlParserPos pos, SqlIdentifier entityType, SqlIdentifier entityKey,
                                   SqlIdentifier entityAttr) {
        super(pos);
        this.entityType = entityType;
        this.entityKey = entityKey;
        this.entityAttr = entityAttr;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SECURITY_ENTITY;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SECURITY ENTITY");
        entityType.unparse(writer, leftPrec, rightPrec);
        entityKey.unparse(writer, leftPrec, rightPrec);
        entityAttr.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getEntityType() {
        return entityType;
    }

    public void setEntityType(SqlIdentifier entityType) {
        this.entityType = entityType;
    }

    public SqlIdentifier getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(SqlIdentifier entityKey) {
        this.entityKey = entityKey;
    }

    public SqlIdentifier getEntityAttr() {
        return entityAttr;
    }

    public void setEntityAttr(SqlIdentifier entityAttr) {
        this.entityAttr = entityAttr;
    }

    public static class SqlCreateSecurityEntityOperator extends SqlSpecialOperator {

        public SqlCreateSecurityEntityOperator() {
            super("CREATE_SECURITY_ENTITY", SqlKind.CREATE_SECURITY_ENTITY);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SECURITY_ENTITY",
                    0,
                    columnType)));
        }
    }

}
