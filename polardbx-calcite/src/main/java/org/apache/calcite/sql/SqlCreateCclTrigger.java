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
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * @author busu
 * date: 2021/4/16 4:29 下午
 */
public class SqlCreateCclTrigger extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateCclTriggerOperator();

    private SqlIdentifier triggerName;

    private SqlIdentifier schemaName;

    private boolean ifNotExits;

    private List<SqlNode> leftOperands;

    private List<SqlNode> operators;

    private List<SqlNode> rightOperands;

    private List<Pair<SqlNode, SqlNode>> limits;

    private List<Pair<SqlNode, SqlNode>> ruleWiths;

    public SqlCreateCclTrigger(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_CCL_TRIGGER;
    }

    public static SqlSpecialOperator getOPERATOR() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CREATE CCL_TRIGGER");
        if (ifNotExits) {
            writer.sep("IF NOT EXISTS");
        }
        triggerName.unparse(writer, leftPrec, rightPrec);

        if (CollectionUtils.isNotEmpty(leftOperands)) {
            writer.keyword("WHEN");
            for (int i = 0; i < leftOperands.size(); ++i) {
                leftOperands.get(i).unparse(writer, leftPrec, rightPrec);
                operators.get(i).unparse(writer, leftPrec, rightPrec);
                rightOperands.get(i).unparse(writer, leftPrec, rightPrec);
                if (i != leftOperands.size() - 1) {
                    writer.keyword(",");
                }
            }
        }

        if (CollectionUtils.isNotEmpty(limits)) {
            writer.keyword("LIMIT");
            for (int i = 0; i < limits.size(); ++i) {
                Pair<SqlNode, SqlNode> limitPair = limits.get(i);
                limitPair.left.unparse(writer, leftPrec, rightPrec);
                writer.keyword("=");
                limitPair.right.unparse(writer, leftPrec, rightPrec);
                if (i != limits.size() - 1) {
                    writer.keyword(",");
                }

            }
        }

        if (CollectionUtils.isNotEmpty(ruleWiths)) {
            writer.sep("CREATE CCL_RULE WITH");
            for (int i = 0; i < ruleWiths.size(); ++i) {
                Pair<SqlNode, SqlNode> sqlNodePair = ruleWiths.get(i);
                sqlNodePair.left.unparse(writer, leftPrec, rightPrec);
                writer.keyword("=");
                sqlNodePair.right.unparse(writer, leftPrec, rightPrec);
                if (i != ruleWiths.size() - 1) {
                    writer.keyword(",");
                }
            }
        }

        writer.endList(selectFrame);
    }

    public static class SqlCreateCclTriggerOperator extends SqlSpecialOperator {
        public SqlCreateCclTriggerOperator() {
            super("CREATE_CCL_RIGGER", SqlKind.CREATE_CCL_TRIGGER);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(
                    ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("Create_Ccl_Trigger_Result",
                        0,
                        columnType)));
        }

    }

    public SqlIdentifier getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(SqlIdentifier triggerName) {
        this.triggerName = triggerName;
    }

    public boolean isIfNotExits() {
        return ifNotExits;
    }

    public void setIfNotExits(boolean ifNotExits) {
        this.ifNotExits = ifNotExits;
    }

    public List<SqlNode> getLeftOperands() {
        return leftOperands;
    }

    public void setLeftOperands(List<SqlNode> leftOperands) {
        this.leftOperands = leftOperands;
    }

    public List<SqlNode> getOperators() {
        return operators;
    }

    public void setOperators(List<SqlNode> operators) {
        this.operators = operators;
    }

    public List<SqlNode> getRightOperands() {
        return rightOperands;
    }

    public void setRightOperands(List<SqlNode> rightOperands) {
        this.rightOperands = rightOperands;
    }

    public List<Pair<SqlNode, SqlNode>> getRuleWiths() {
        return ruleWiths;
    }

    public void setRuleWiths(
        List<Pair<SqlNode, SqlNode>> ruleWiths) {
        this.ruleWiths = ruleWiths;
    }

    public List<Pair<SqlNode, SqlNode>> getLimits() {
        return limits;
    }

    public void setLimits(
        List<Pair<SqlNode, SqlNode>> limits) {
        this.limits = limits;
    }

    public SqlIdentifier getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SqlIdentifier schemaName) {
        this.schemaName = schemaName;
    }
}
