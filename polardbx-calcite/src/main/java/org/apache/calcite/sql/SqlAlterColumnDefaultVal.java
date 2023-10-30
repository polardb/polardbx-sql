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

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

/**
 * @author chenmo.cm
 */
// | ALTER [COLUMN] col_name {SET DEFAULT literal | DROP DEFAULT}
public class SqlAlterColumnDefaultVal extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER COLUMN", SqlKind.ALTER_COLUMN_DEFAULT_VAL);
    private final SqlIdentifier originTableName;
    private final SqlIdentifier columnName;
    private final SqlLiteral defaultVal;
    private final SqlCall defaultExpr;
    private final boolean dropDefault;
    private final String sourceSql;
    private SqlNode tableName;

    /**
     * Creates a SqlAlterColumnDefaultVal.
     */
    public SqlAlterColumnDefaultVal(SqlIdentifier tableName, SqlIdentifier columnName, SqlLiteral defaultVal,
                                    SqlCall defaultExpr,
                                    boolean dropDefault, String sql, SqlParserPos pos) {
        super(pos);
        this.tableName = tableName;
        this.originTableName = tableName;
        this.columnName = columnName;
        this.defaultVal = defaultVal;
        this.defaultExpr = defaultExpr;
        this.dropDefault = dropDefault;
        this.sourceSql = sql;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableName, columnName);
    }

    public void setTargetTable(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER COLUMN", "");

        columnName.unparse(writer, leftPrec, rightPrec);

        if (dropDefault) {
            writer.keyword("DROP DEFAULT");
        } else {
            writer.keyword("SET DEFAULT");
            if (defaultVal != null) {
                defaultVal.unparse(writer, leftPrec, rightPrec);
            } else {
                final SqlWriter.Frame frameExpr = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                defaultExpr.unparse(writer, leftPrec, rightPrec);
                writer.endList(frameExpr);
            }

        }

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public SqlIdentifier getColumnName() {
        return columnName;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    @Override
    public SqlAlterSpecification replaceTableName(SqlIdentifier newTableName) {
        return new SqlAlterColumnDefaultVal(newTableName,
            columnName,
            defaultVal,
            defaultExpr,
            dropDefault,
            sourceSql,
            getParserPosition());
    }

    public SqlLiteral getDefaultVal() {
        return defaultVal;
    }

    public SqlCall getDefaultExpr() {
        return defaultExpr;
    }

    @Override
    public boolean supportFileStorage() {
        return true;
    }
}
