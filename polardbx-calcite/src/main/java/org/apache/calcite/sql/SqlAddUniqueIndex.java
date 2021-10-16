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

import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * @author chenmo.cm
 * @date 2018/12/13 8:15 PM
 */
// | ADD [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type]
// (index_col_name,...) [index_option] ...
public class SqlAddUniqueIndex extends SqlAddIndex {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD UNIQUE INDEX", SqlKind.ADD_UNIQUE_INDEX);

    public SqlAddUniqueIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIndexDefinition indexDef){
        super(pos, indexName, indexDef);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD", "");

        if (indexDef.isHasConstraint()) {
            writer.keyword("CONSTRAINT");
            if (null != indexDef.getUniqueConstraint()) {
                indexDef.unparse(writer, leftPrec, rightPrec);
            }
        }

        writer.keyword("UNIQUE");

        if (indexDef.isGlobal()) {
            SqlUtil.wrapSqlLiteralSymbol(indexDef.getIndexResiding()).unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword("INDEX");

        if (null != indexName) {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        indexDef.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    @Override
    public boolean isClusteredIndex() { return indexDef.isClustered(); }
}
