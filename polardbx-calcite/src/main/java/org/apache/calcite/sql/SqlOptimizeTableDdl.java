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

import com.google.common.base.Joiner;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.List;

/**
 * @author guxu.ygh
 */
public class SqlOptimizeTableDdl extends SqlDdl { // Use DDL here to utilize async DDL framework.

    private static final SqlSpecialOperator OPERATOR = new SqlOptimizeTable.SqlOptimizeTableOperator();

    private final List<SqlNode>             tableNames;
    private final boolean                   noWriteToBinlog;
    private final boolean                   local;

    public SqlOptimizeTableDdl(SqlParserPos pos, List<SqlNode> tableNames, boolean noWriteToBinlog, boolean local){
        super(OPERATOR, pos);
        this.name = tableNames.get(0);
        this.tableNames = tableNames;
        this.noWriteToBinlog = noWriteToBinlog;
        this.local = local;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("OPTIMIZE");

        if (isNoWriteToBinlog()) {
            writer.sep("NO_WRITE_TO_BINLOG");
        } else if (isLocal()) {
            writer.sep("LOCAL");
        }

        writer.sep("TABLE");

        writer.print(Joiner.on(",").join(tableNames));

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OPTIMIZE_TABLE;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlOptimizeTableDdl(this.pos, tableNames, noWriteToBinlog, local);
    }

    public List<SqlNode> getTableNames() {
        return tableNames;
    }

    public boolean isNoWriteToBinlog() {
        return noWriteToBinlog;
    }

    public boolean isLocal() {
        return local;
    }
}
