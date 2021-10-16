/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import junit.framework.TestCase;

import java.util.List;

public class PurgeTest extends TestCase {

    public void test_0() throws Exception {
        String sql = "PURGE TABLE bin_name";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

        String text = SQLUtils.toSQLString(stmtList, null);

        assertEquals("PURGE TABLE bin_name", text);
    }

    public void test_1() throws Exception {
        String sql = "PURGE BINARY LOGS TO 'mysql-bin.010';";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

        String text = SQLUtils.toSQLString(stmtList, null);

        assertEquals("PURGE BINARY LOGS TO 'mysql-bin.010';", text);
    }

    public void test_2() throws Exception {
        String sql = "PURGE MASTER LOGS BEFORE 'mysql-bin.010';";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

        String text = SQLUtils.toSQLString(stmtList, null);

        assertEquals("PURGE MASTER LOGS BEFORE 'mysql-bin.010';", text);
    }

    public void test_3() throws Exception {
        String sql = "PURGE recyclebin";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

        String text = SQLUtils.toSQLString(stmtList, null);

        assertEquals("PURGE RECYCLEBIN", text);
    }

//    public void test_4() throws Exception {
//        String sql = "FLASHBACK TABLE bin_name TO BEFORE DROP";
//
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);
//
//        String text = SQLUtils.toSQLString(stmtList, null);
//
//        assertEquals("PURGE RECYCLEBIN", text);
//    }
}
