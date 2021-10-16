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

package com.alibaba.polardbx.druid.sql.semantic;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.List;

public class SemanticCheck extends SQLASTVisitorAdapter {

    public boolean visit(SQLCreateTableStatement stmt) {
        stmt.containsDuplicateColumnNames(true);
        return true;
    }

    public static boolean check(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        SemanticCheck v = new SemanticCheck();
        for (SQLStatement stmt : stmtList) {
            stmt.accept(v);
        }

        return false;
    }
}
