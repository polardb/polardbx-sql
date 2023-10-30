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

public class SqlInspectSeqRange extends SqlDal {

    private static final SqlSpecialOperator OPERATOR =
        new SqlAffectedRowsOperator("INSPECT_SEQ_RANGE", SqlKind.INSPECT_SEQ_RANGE);

    private final SqlNode name;

    public SqlInspectSeqRange(SqlParserPos pos, SqlNode name) {
        super(pos);
        this.name = name;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("INSPECT SEQUENCE RANGE FOR");
        name.unparse(writer, leftPrec, rightPrec);
    }

    public SqlNode getName() {
        return name;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.INSPECT_SEQ_RANGE;
    }

}
