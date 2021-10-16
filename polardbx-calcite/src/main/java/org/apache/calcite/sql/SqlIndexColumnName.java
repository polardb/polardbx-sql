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

import java.util.List;

import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * @author chenmo.cm
 * @date 2018/12/4 5:45 PM
 */
public class SqlIndexColumnName extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("INDEX_COLUMN_NAME",
                                                         SqlKind.INDEX_COLUMN_NAME);

    private final SqlIdentifier             columnName;
    /** null is possible */
    private final SqlLiteral                length;
    private final Boolean                   asc;

    public SqlIndexColumnName(SqlParserPos pos, SqlIdentifier columnName, SqlLiteral length, Boolean asc){
        super(pos);
        this.columnName = columnName;
        this.length = length;
        this.asc = asc;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(columnName, length, null == asc ? null : SqlLiteral.createBoolean(asc, pos));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList("", "");

        columnName.unparse(writer, leftPrec, rightPrec);

        if (null != length) {
            final Frame frame1 = writer.startList("(", ")");
            length.unparse(writer, leftPrec, rightPrec);
            writer.endList(frame1);
        }

        if (null != asc) {
            writer.keyword(this.asc ? "ASC" : "DESC");
        }

        writer.endList(frame);
    }

    public SqlIdentifier getColumnName() {
        return columnName;
    }

    public SqlLiteral getLength() {
        return length;
    }

    public Boolean isAsc() {
        return asc;
    }

    public String getColumnNameStr() {
        return columnName.getLastName();
    }
}
