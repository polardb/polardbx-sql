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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLBeginStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCloseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLFetchStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLOpenStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowHelpStatement;

import java.util.HashSet;
import java.util.Set;

public class StatementKind {
    public static Set<Class> notSQLStmt = new HashSet<Class>() {
        {
            add(SQLIfStatement.Else.class);
            add(SQLIfStatement.ElseIf.class);
            add(MySqlCaseStatement.MySqlWhenStatement.class);
        }
    };

    public static Set<Class> cursorStmt = new HashSet<Class>() {
        {
            add(MySqlCursorDeclareStatement.class);
            add(SQLOpenStatement.class);
            add(SQLCloseStatement.class);
            add(SQLFetchStatement.class);
            // Notice this, currently all declare handler should be used to handle cursor end exception
            add(MySqlDeclareHandlerStatement.class);
            add(MySqlDeclareConditionStatement.class);
        }
    };

    public static Set<Class> controlStmt = new HashSet<Class>() {
        {
            add(SQLBlockStatement.class);
            add(MySqlDeclareStatement.class);
            add(SQLSetStatement.class);
            add(SQLIfStatement.class);
            add(MySqlCaseStatement.class);
            add(SQLWhileStatement.class);
            add(SQLLoopStatement.class);
            add(MySqlRepeatStatement.class);
            add(MySqlLeaveStatement.class);
            add(MySqlIterateStatement.class);
        }
    };

    public static Set<Class> procedureNotSupportStmt = new HashSet<Class>() {
        {
            add(SQLSetStatement.class);
            add(MySqlLoadDataInFileStatement.class);
            add(SQLGrantStatement.class);
            add(SQLBeginStatement.class);
            add(SQLUseStatement.class);
            add(MySqlShowHelpStatement.class);
            add(MySqlKillStatement.class);
        }
    };

    public static Set<Class> ignoredStatement = new HashSet<Class>() {
        {
            add(MySqlFlushStatement.class);
        }
    };
}
