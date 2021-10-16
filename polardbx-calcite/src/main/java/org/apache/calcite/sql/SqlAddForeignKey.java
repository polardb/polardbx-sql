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

/**
 * @author chenmo.cm
 * @date 2018/12/13 8:15 PM
 */
// ADD [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...)
//     reference_definition
public class SqlAddForeignKey extends SqlAddIndex {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD FOREIGN INDEX", SqlKind.ADD_FOREIGN_KEY);

    private final SqlIdentifier constraint;
    private final SqlReferenceDefinition referenceDefinition;

    public SqlAddForeignKey(SqlParserPos pos, SqlIdentifier indexName, SqlIndexDefinition indexDef, SqlIdentifier constraint, SqlReferenceDefinition referenceDefinition){
        super(pos, indexName, indexDef);
        this.constraint = constraint;
        this.referenceDefinition = referenceDefinition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD", "");

        if (null != constraint) {
            writer.keyword("CONSTRAINT");
            constraint.unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword("FOREIGN KEY");

        if (null != indexName) {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        indexDef.unparse(writer, leftPrec, rightPrec);

        referenceDefinition.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }
}
