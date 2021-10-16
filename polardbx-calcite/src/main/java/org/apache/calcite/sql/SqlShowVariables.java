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

import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/7 下午11:11
 */
public class SqlShowVariables extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowVariablesOperator();

    public boolean isSession() {
        return isSession;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    private boolean isSession;
    private boolean isGlobal;

    public SqlShowVariables(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                            SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean isSession, boolean isGlobal){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.isSession = isSession;
        this.isGlobal = isGlobal;
    }

    public static SqlShowVariables create(SqlParserPos pos, boolean isSession, boolean isGlobal, SqlNode like, SqlNode where) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (isSession) {
            specialIdentifiers.add(SqlSpecialIdentifier.SESSION);
        }
        if (isGlobal) {
            specialIdentifiers.add(SqlSpecialIdentifier.GLOBAL);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.VARIABLES);

        return new SqlShowVariables(pos, specialIdentifiers, ImmutableList.<SqlNode>of(), like, where, null, null, isSession, isGlobal);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_VARIABLES;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowVariablesOperator extends SqlSpecialOperator {

        public SqlShowVariablesOperator(){
            super("SHOW_VARIABLES", SqlKind.SHOW_VARIABLES);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Variable_name", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Value", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
