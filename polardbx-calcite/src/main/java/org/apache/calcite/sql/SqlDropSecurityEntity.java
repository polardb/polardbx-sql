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
import java.util.StringJoiner;

/**
 * @author pangzhaoxing
 */
public class SqlDropSecurityEntity extends SqlDal{


    private static final SqlOperator OPERATOR = new SqlDropSecurityEntity.SqlDropSecurityEntityOperator();

    private List<SqlIdentifier> entityTypes;
    private List<SqlIdentifier> entityKeys;

    public SqlDropSecurityEntity(SqlParserPos pos, List<SqlIdentifier> entityTypes, List<SqlIdentifier> entityKeys) {
        super(pos);
        this.entityTypes = entityTypes;
        this.entityKeys = entityKeys;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_SECURITY_ENTITY;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlIdentifier> getEntityTypes() {
        return entityTypes;
    }

    public List<SqlIdentifier> getEntityKeys() {
        return entityKeys;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP SECURITY ENTITY");
        StringJoiner sj = new StringJoiner(",");
        for (int i = 0 ; i < entityTypes.size(); i++){
            sj.add(entityTypes.get(i).getSimple() + " " + entityKeys.get(i).getSimple());
        }
        writer.keyword(sj.toString().toUpperCase());
    }

    public static class SqlDropSecurityEntityOperator extends SqlSpecialOperator {

        public SqlDropSecurityEntityOperator() {
            super("DROP_SECURITY_Entity", SqlKind.DROP_SECURITY_ENTITY);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_SECURITY_ENTITY",
                    0,
                    columnType)));
        }
    }


}
