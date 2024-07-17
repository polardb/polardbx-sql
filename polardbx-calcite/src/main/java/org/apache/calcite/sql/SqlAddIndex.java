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

/**
 * @author chenmo.cm
 */
public class SqlAddIndex extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD INDEX", SqlKind.ADD_INDEX);

    protected final SqlIdentifier indexName;
    protected final SqlIndexDefinition indexDef;

    public SqlAddIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIndexDefinition indexDef) {
        super(pos);
        this.indexName = indexName;
        this.indexDef = indexDef;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(indexName, indexDef);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD", "");

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

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public SqlIndexDefinition getIndexDef() {
        return indexDef;
    }

    public boolean isClusteredIndex() {
        return indexDef.isClustered();
    }

    public boolean isColumnarIndex() {
        return indexDef.isColumnar();
    }

    @Override
    public boolean supportFileStorage() {
        return true;
    }
}
