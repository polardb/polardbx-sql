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

package com.alibaba.polardbx.druid.bvt.sql.refactor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import junit.framework.TestCase;

public class AddWhere_0 extends TestCase {
    public void test_select_0() throws Exception {
        String sql = "select * from t";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "WHERE id = 1", stmt.toString());
    }

    public void test_select_1() throws Exception {
        String sql = "select * from t where name = 'xx'";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "WHERE name = 'xx'\n" +
                "\tAND id = 1", stmt.toString());
    }

    public void test_select_1_union() throws Exception {
        String sql = "select * from t1 union all select * from t2";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT *\n" +
                "\tFROM t1\n" +
                "\tUNION ALL\n" +
                "\tSELECT *\n" +
                "\tFROM t2\n" +
                ") u", stmt.toString());
    }

    public void test_delete_0() throws Exception {
        String sql = "delete from t";
        SQLDeleteStatement stmt = (SQLDeleteStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("DELETE FROM t\n" +
                "WHERE id = 1", stmt.toString());
    }

    public void test_delete_1() throws Exception {
        String sql = "delete from t where name = 'xx'";
        SQLDeleteStatement stmt = (SQLDeleteStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("DELETE FROM t\n" +
                "WHERE name = 'xx'\n" +
                "\tAND id = 1", stmt.toString());
    }

    public void test_update_0() throws Exception {
        String sql = "update t set val = 'abc' where name = 'xx'";
        SQLUpdateStatement stmt = (SQLUpdateStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        stmt.addWhere(SQLUtils.toSQLExpr("id = 1", DbType.mysql));

        assertEquals("UPDATE t\n" +
                "SET val = 'abc'\n" +
                "WHERE name = 'xx'\n" +
                "\tAND id = 1", stmt.toString());
    }
}
