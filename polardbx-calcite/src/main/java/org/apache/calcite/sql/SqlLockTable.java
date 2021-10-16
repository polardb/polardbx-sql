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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author eric.fy
 */
public class SqlLockTable extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlAffectedRowsOperator("LOCK_TABLE", SqlKind.LOCK_TABLE);

    private List<LockType> lockTypes;

    public SqlLockTable(SqlParserPos pos, List<SqlNode> tableNames, List<LockType> lockTypes){
        super(pos);
        this.operands = tableNames;
        this.tableIndexes = new LinkedList<>();
        for (int i = 0; i < tableNames.size(); i++) {
            tableIndexes.add(i);
        }
        this.lockTypes = lockTypes;
    }

    public LockType getLockType(int index) {
        return lockTypes.get(index);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // do nothing
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("LOCK TABLES");
        for (int index = 0; index < operands.size(); index++) {
            if (index > 0) {
                writer.print(", ");
            }
            operands.get(index).unparse(writer, leftPrec, rightPrec);
            writer.print(lockTypes.get(index).name());
        }
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
        return new SqlLockTable(this.pos, operands, lockTypes);
    }

    public enum LockType {
        READ("READ"), READ_LOCAL("READ LOCAL"), WRITE("WRITE"), LOW_PRIORITY_WRITE("LOW_PRIORITY WRITE");

        public final String name;

        LockType(String name){
            this.name = name;
        }
    }
}
