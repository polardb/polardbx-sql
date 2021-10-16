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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlAddPrimaryKey extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD PRIMARY KEY", SqlKind.ADD_PRIMARY_KEY);

    private final SqlNode tableName;
    private final List<SqlIndexColumnName> columns;

    public SqlAddPrimaryKey(SqlParserPos pos, SqlNode tableName, List<SqlIndexColumnName> columns) {
        super(pos);
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD PRIMARY KEY", "");

        if (columns != null) {
            final SqlWriter.Frame frame1 = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            SqlUtil.wrapSqlNodeList(columns).commaList(writer);
            writer.endList(frame1);
        }

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public List<SqlIndexColumnName> getColumns() {
        return columns;
    }

}
