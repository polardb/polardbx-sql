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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 * @date 2018/6/14 下午7:40
 */
public class SqlSetTransaction extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlSetTransactionOperator();

    private final VariableScope             scope;
    private final IsolationLevel            isolationLevel;
    private final AccessModel               accessModel;
    private final String                    policy;
    private Boolean                         global;
    private Boolean                         session;
    private String                          access;
    private String                          level;

    public SqlSetTransaction(SqlParserPos pos, Boolean global, Boolean session, String isolationLevel,
                             String accessModel, String policy){
        super(pos);
        this.global = global;
        this.session = session;
        if (null != global && global) {
            scope = VariableScope.GLOBAL;
        } else if (null != session && session) {
            scope = VariableScope.SESSION;
        } else {
            scope = null;
        }

        this.level = isolationLevel;
        if (null != isolationLevel) {
            this.isolationLevel = IsolationLevel.of(isolationLevel);
        } else {
            this.isolationLevel = null;
        }

        this.access = accessModel;
        if (null != accessModel) {
            this.accessModel = AccessModel.of(accessModel);
        } else {
            this.accessModel = null;
        }

        this.policy = policy;
    }

    public boolean isGlobal() {
        return global != null && global;
    }

    public boolean isSession() {
        return session != null && session;
    }

    public AccessModel getAccessModel() {
        return accessModel;
    }

    public String getLevel() {
        return level;
    }

    public VariableScope getScope() {
        return scope;
    }

    public String getAccess() {
        return access;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public String getPolicy() {
        return policy;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SET");

        if (null != scope) {
            writer.sep(scope.name());
        }

        writer.sep("TRANSACTION");

        if (null != accessModel) {
            writer.sep("READ");
            writer.sep(access.toUpperCase());
            writer.endList(selectFrame);
            return;
        }

        if (null != isolationLevel) {
            writer.sep("ISOLATION LEVEL");
            writer.sep(level.toUpperCase());
            writer.endList(selectFrame);
            return;
        }

        if (null != policy) {
            writer.sep("POLICY");
            writer.sep(policy);
            writer.endList(selectFrame);
            return;
        }

        writer.endList(selectFrame);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SQL_SET_TRANSACTION;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static enum IsolationLevel {
        READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE;

        private static Map<String, IsolationLevel> valueMap = new HashMap();

        static {
            valueMap.put("READ UNCOMMITTED", READ_UNCOMMITTED);
            valueMap.put("READ COMMITTED", READ_COMMITTED);
            valueMap.put("REPEATABLE READ", REPEATABLE_READ);
            valueMap.put("SERIALIZABLE", SERIALIZABLE);
        }

        public static IsolationLevel of(String value) {
            if (null == value) {
                return null;
            }
            return valueMap.get(value.toUpperCase());
        }
    }

    public static enum AccessModel {
        READ_WRITE, READ_ONLY;

        private static Map<String, AccessModel> valueMap = new HashMap();

        static {
            valueMap.put("WRITE", READ_WRITE);
            valueMap.put("ONLY", READ_ONLY);
        }

        public static AccessModel of(String value) {
            if (null == value) {
                return null;
            }
            return valueMap.get(value.toUpperCase());
        }
    }

    public static class SqlSetTransactionOperator extends SqlSpecialOperator {

        public SqlSetTransactionOperator(){
            super("SQL_SET_TRANSACTION", SqlKind.SQL_SET_TRANSACTION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}
