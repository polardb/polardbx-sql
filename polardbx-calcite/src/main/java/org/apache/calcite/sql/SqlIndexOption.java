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

import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * @author chenmo.cm
 * @date 2018/12/4 9:59 PM
 */
public class SqlIndexOption extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("INDEX_OPTION", SqlKind.INDEX_OPTION);

    private final SqlLiteral                keyBlockSize;
    private final SqlIndexType              indexType;
    private final SqlIdentifier             parserName;
    private final SqlCharStringLiteral      comment;

    private SqlIndexOption(SqlParserPos pos, SqlLiteral keyBlockSize, SqlIndexType indexType, SqlIdentifier parserName,
                           SqlCharStringLiteral comment){
        super(pos);
        this.keyBlockSize = keyBlockSize;
        this.indexType = indexType;
        this.parserName = parserName;
        this.comment = comment;
    }

    public static SqlIndexOption createKeyBlockSize(SqlParserPos pos, SqlLiteral keyBlockSize) {
        return new SqlIndexOption(pos, keyBlockSize, null, null, null);
    }

    public static SqlIndexOption createIndexType(SqlParserPos pos, SqlIndexType indexType) {
        return new SqlIndexOption(pos, null, indexType, null, null);
    }

    public static SqlIndexOption createParserName(SqlParserPos pos, SqlIdentifier parserName) {
        return new SqlIndexOption(pos, null, null, parserName, null);
    }

    public static SqlIndexOption createComment(SqlParserPos pos, SqlCharStringLiteral comment) {
        return new SqlIndexOption(pos, null, null, null, comment);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(keyBlockSize, SqlUtil.wrapSqlLiteralSymbol(indexType), parserName, comment);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList("", "");

        if (null != keyBlockSize) {
            writer.keyword("KEY_BLOCK_SIZE");
            writer.keyword("=");
            keyBlockSize.unparse(writer, leftPrec, rightPrec);
        }

        if (null != indexType) {
            writer.keyword("USING");
            SqlUtil.wrapSqlLiteralSymbol(indexType).unparse(writer, leftPrec, rightPrec);
        }

        if (null != parserName) {
            writer.keyword("WITH PARSER");
            parserName.unparse(writer, leftPrec, rightPrec);
        }

        if (null != comment) {
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(frame);
    }

    public SqlLiteral getKeyBlockSize() {
        return keyBlockSize;
    }

    public SqlIndexType getIndexType() {
        return indexType;
    }

    public SqlIdentifier getParserName() {
        return parserName;
    }

    public SqlCharStringLiteral getComment() {
        return comment;
    }
}
