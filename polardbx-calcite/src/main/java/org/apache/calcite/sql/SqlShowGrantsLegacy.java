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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * Drds mode only, for polarx mode, see {@link SqlShowGrants}.
 *
 * @author chenmo.cm
 * @see SqlShowGrants
 */
public class SqlShowGrantsLegacy extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowGrantsOperator();

    private SqlNode user;
    private boolean forAll = false;
    private String userName;
    private String host;

    public SqlShowGrantsLegacy(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                               SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, String userName,
                               String host) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.userName = userName;
        this.host = host;
        if (operands.size() == 0 || operands.get(0) == null) {
            forAll = true;
        } else {
            forAll = false;
            user = operands.get(0);
        }
    }

    public SqlNode getUser() {
        return user;
    }

    public void setUser(SqlNode user) {
        this.user = user;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public boolean isForAll() {
        return forAll;
    }

    public void setForAll(boolean forAll) {
        this.forAll = forAll;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_GRANTS;
    }

    public static class SqlShowGrantsOperator extends SqlSpecialOperator {

        public SqlShowGrantsOperator() {
            super("SHOW_GRANTS", SqlKind.SHOW_GRANTS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("User", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Host", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Grant statement", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
