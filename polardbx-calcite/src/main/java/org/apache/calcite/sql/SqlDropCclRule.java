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

import java.util.LinkedList;
import java.util.List;

/**
 * @author busu
 * date: 2020/10/26 2:37 下午
 */
public class SqlDropCclRule extends SqlDal {
    private static final SqlOperator OPERATOR = new SqlDropCclRuleOperator();

    private List<SqlIdentifier> ruleNames;

    private boolean ifExists;

    public SqlDropCclRule(SqlParserPos pos, List<SqlIdentifier> ruleNames, boolean ifExists) {
        super(pos);
        this.ruleNames = ruleNames;
        this.ifExists = ifExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_CCL_RULE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("DROP");
        writer.sep("CCL_RULE");
        if (ifExists) {
            writer.sep("IF EXISTS");
        }
        if (ruleNames != null) {
            if (!ruleNames.isEmpty()) {
                writer.print(ruleNames.get(0).getSimple());
            }
            for (int i = 1; i < ruleNames.size(); ++i) {
                writer.print(", ");
                writer.print(ruleNames.get(i).getSimple());
            }
        }
        writer.endList(selectFrame);
    }

    public static class SqlDropCclRuleOperator extends SqlSpecialOperator {

        public SqlDropCclRuleOperator() {
            super("DROP_CCL_RULE", SqlKind.DROP_CCL_RULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Dropped_Ccl_Rule_Count", 0,
                typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            return typeFactory.createStructType(columns);
        }
    }

    public List<SqlIdentifier> getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(List<SqlIdentifier> ruleNames) {
        this.ruleNames = ruleNames;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

}
