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

public class SqlSavepoint extends SqlDal {

    private static final SqlOperator OPERATOR = new SqlAffectedRowsOperator("SAVEPOINT", SqlKind.SAVEPOINT);

    private SqlIdentifier name;
    private Action action;

    public SqlSavepoint(SqlParserPos pos, SqlIdentifier name, Action action) {
        super(pos);
        this.name = name;
        this.action = action;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public Action getAction() {
        return action;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String prec;
        switch (action) {
            case SET_SAVEPOINT:
                prec = "SAVEPOINT";
                break;
            case ROLLBACK_TO:
                prec = "ROLLBACK TO";
                break;
            case RELEASE:
                prec = "RELEASE SAVEPOINT";
                break;
            default:
                throw new AssertionError();
        }
        writer.keyword(prec);
        name.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SAVEPOINT;
    }

    public enum Action {
        ROLLBACK_TO, SET_SAVEPOINT, RELEASE
    }
}
