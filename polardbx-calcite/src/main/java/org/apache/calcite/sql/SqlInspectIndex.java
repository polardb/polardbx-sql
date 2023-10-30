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

import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SqlInspectIndex extends SqlDdl {
    private final SqlNode tableName;
    private final boolean fromTable;

    private final boolean full;

    private final String mode;

    private static final SqlSpecialOperator OPERATOR = new SqlInspectIndexOperator();

    public SqlInspectIndex(SqlParserPos pos, SqlNode tableName, boolean fromTable, boolean full, String mode) {
        super(OPERATOR, pos);
        this.tableName = tableName;
        this.fromTable = fromTable;
        this.full = full;
        this.mode = mode;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public boolean isFromTable() {
        return fromTable;
    }

    public boolean isFull() {
        return full;
    }

    public String getMode() {
        return mode;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("INSPECT INDEX");
        if (this.fromTable) {
            writer.keyword("FROM");
            tableName.unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.INSPECT_INDEX;
    }

    public static class SqlInspectIndexOperator extends SqlSpecialOperator {
        public SqlInspectIndexOperator() {
            super("INSPECT_INDEX", SqlKind.INSPECT_INDEX);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return typeFactory.createStructType(ImmutableList.of(
                new RelDataTypeFieldImpl("SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("TABLE", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("INDEX", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("INDEX_TYPE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("INDEX_COLUMN", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("COVERING_COLUMN", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("USE_COUNT", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("LAST_ACCESS_TIME", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("DISCRIMINATION", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("PROBLEM", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                new RelDataTypeFieldImpl("ADVICE", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR))
            ));
        }
    }
}
