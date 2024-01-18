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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/12/29 2:04 PM
 */
public class SqlReferenceDefinition extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("COLUMN_REFERENCE",
                                                         SqlKind.COLUMN_REFERENCE);
    /* 只有名字是可变的，为了后面替换成物理名字用 */
    private SqlIdentifier                   tableName;
    private final List<SqlIdentifier>       columns;
    private final MatchType                 matchType;
    private final List<SqlReferenceOption>  referenceOptions;

    public SqlReferenceDefinition(SqlParserPos pos, SqlIdentifier tableName, List<SqlIdentifier> columns,
                                  MatchType matchType, List<SqlReferenceOption> referenceOptions){
        super(pos);
        this.tableName = tableName;
        this.columns = columns;
        this.matchType = matchType;
        this.referenceOptions = referenceOptions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName,
            SqlUtil.wrapSqlNodeList(columns),
            SqlUtil.wrapSqlLiteralSymbol(matchType),
            SqlUtil.wrapSqlNodeList(referenceOptions));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REFERENCES");
        tableName.unparse(writer, leftPrec, rightPrec);

        final Frame frame = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
        SqlUtil.wrapSqlNodeList(columns).commaList(writer);
        writer.endList(frame);

        if (null != matchType) {
            switch (matchType) {
                case MATCH_FULL:
                    writer.keyword("MATCH FULL");
                    break;
                case MATCH_PARTIAL:
                    writer.keyword("MATCH PARTIAL");
                    break;
                case MATCH_SIMPLE:
                    writer.keyword("MATCH SIMPLE");
                    break;
                default:
                    break;
            } // end of switch
        }

        if (GeneralUtil.isNotEmpty(referenceOptions)) {
            for (SqlReferenceOption option : referenceOptions) {
                option.unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public List<SqlIdentifier> getColumns() {
        return columns;
    }

    public List<SqlReferenceOption> getreferenceOptions() {
        return referenceOptions;
    }
    public static enum MatchType {
        MATCH_FULL, MATCH_PARTIAL, MATCH_SIMPLE
    }
}
