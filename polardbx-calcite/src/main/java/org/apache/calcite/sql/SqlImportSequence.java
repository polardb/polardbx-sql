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

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SqlImportSequence extends SqlDdl {
    private static final SqlOperator OPERATOR = new SqlImportSequenceOperator();

    final String logicalDb;

    public SqlImportSequence(SqlParserPos pos, String logicalDb) {
        super(OPERATOR, pos);
        this.logicalDb = logicalDb;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public String getLogicalDb() {
        return logicalDb;
    }

    public static class SqlImportSequenceOperator extends SqlSpecialOperator {
        public SqlImportSequenceOperator() {
            super("IMPORT_SEQUENCE", SqlKind.IMPORT_SEQUENCE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("IMPORT_SEQUENCE_RESULT",
                    0,
                    columnType)));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("IMPORT SEQUENCE FROM ");
        writer.print(logicalDb);
    }

}
