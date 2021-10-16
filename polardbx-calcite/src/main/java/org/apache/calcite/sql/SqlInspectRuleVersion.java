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

import java.util.ArrayList;

import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_CONTENT_MD5;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_SOURCE;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_VERSION;

public class SqlInspectRuleVersion extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlInspectRuleVersionOperator();

    private final boolean ignoreManager;

    public SqlInspectRuleVersion(SqlParserPos pos, boolean ignoreManager) {
        super(pos);
        this.operands = new ArrayList<>(0);
        this.ignoreManager = ignoreManager;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("INSPECT RULE VERSION");
        if (ignoreManager) {
            writer.sep("IGNORE MANAGER");
        }
        writer.endList(selectFrame);
    }

    public boolean isIgnoreManager() {
        return ignoreManager;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.INSPECT_RULE_VERSION;
    }

    public static class SqlInspectRuleVersionOperator extends SqlSpecialOperator {

        public SqlInspectRuleVersionOperator() {
            super("INSPECT_RULE_VERSION", SqlKind.INSPECT_RULE_VERSION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return typeFactory.createStructType(ImmutableList.of(
                    new RelDataTypeFieldImpl(CONSISTENCY_SOURCE, 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    new RelDataTypeFieldImpl(CONSISTENCY_VERSION, 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    new RelDataTypeFieldImpl(CONSISTENCY_CONTENT_MD5, 2, typeFactory.createSqlType(SqlTypeName.VARCHAR))
            ));
        }
    }

}
