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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author eric.fy
 */
public class SqlUnlockTable extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlAffectedRowsOperator("UNLOCK_TABLE", SqlKind.UNLOCK_TABLE);

    public SqlUnlockTable(SqlParserPos pos){
        super(pos);
        this.operands = ImmutableList.of();
        this.tableIndexes = ImmutableList.of();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // do nothing
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("UNLOCK TABLES");
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.LOCK_TABLE;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlUnlockTable(this.pos);
    }
}
