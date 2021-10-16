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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCurrentOfCursorExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAlterColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCommentStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprHint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;
import junit.framework.TestCase;

public class SQLASTVisitorAdapterTest extends TestCase {

    public void test_adapter() throws Exception {
        SQLASTVisitorAdapter adapter = new SQLASTVisitorAdapter();
        new SQLBinaryOpExpr().accept(adapter);
        new SQLInListExpr().accept(adapter);
        new SQLSelectQueryBlock().accept(adapter);
        new SQLDropTableStatement().accept(adapter);
        new SQLCreateTableStatement().accept(adapter);
        new SQLDeleteStatement().accept(adapter);
        new SQLCurrentOfCursorExpr().accept(adapter);
        new SQLInsertStatement().accept(adapter);
        new SQLUpdateStatement().accept(adapter);
        new SQLNotNullConstraint().accept(adapter);
        new SQLMethodInvokeExpr().accept(adapter);
        new SQLCallStatement().accept(adapter);
        new SQLSomeExpr().accept(adapter);
        new SQLAnyExpr().accept(adapter);
        new SQLAllExpr().accept(adapter);
        new SQLDefaultExpr().accept(adapter);
        new SQLCommentStatement().accept(adapter);
        new SQLDropViewStatement().accept(adapter);
        new SQLSavePointStatement().accept(adapter);
        new SQLReleaseSavePointStatement().accept(adapter);
        new SQLCreateDatabaseStatement().accept(adapter);
        new SQLAlterTableDropIndex().accept(adapter);
        new SQLOver().accept(adapter);
        new SQLWithSubqueryClause().accept(adapter);
        new SQLAlterTableAlterColumn().accept(adapter);
        new SQLAlterTableStatement().accept(adapter);
        new SQLAlterTableDisableConstraint().accept(adapter);
        new SQLAlterTableEnableConstraint().accept(adapter);
        new SQLColumnCheck().accept(adapter);
        new SQLExprHint().accept(adapter);
        new SQLAlterTableDropConstraint().accept(adapter);
    }
}
