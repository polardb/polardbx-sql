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
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import junit.framework.TestCase;

public class MySqlInsertTest_49 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into test partition(ds='qqq', dt='2020-03') if not exists (select * from test);";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT INTO test PARTITION (ds='qqq', dt='2020-03') IF NOT EXISTS \n" +
                "(SELECT *\n" +
                "FROM test);", stmt.toString());
    }

    public void test_insert_1() throws Exception {
        String sql = "insert into test partition(ds='qqq', dt='2020-03')  (select * from test);";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT INTO test PARTITION (ds='qqq', dt='2020-03')\n" +
                "(SELECT *\n" +
                "FROM test);", stmt.toString());
    }

    public void test_insert_2() throws Exception {
        String sql = "insert overwrite into test partition(ds='qqq', dt='2020-03') if not exists (select * from test);";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT OVERWRITE INTO test PARTITION (ds='qqq', dt='2020-03') IF NOT EXISTS \n" +
                "(SELECT *\n" +
                "FROM test);", stmt.toString());
    }

    public void test_insert_3() throws Exception {
        String sql = "insert overwrite table test partition(ds='qqq', dt='2020-03')  (select * from test);";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT OVERWRITE INTO test PARTITION (ds='qqq', dt='2020-03')\n" +
                "(SELECT *\n" +
                "FROM test);", stmt.toString());
    }

    public void test_insert_4() throws Exception {
        String sql = "INSERT INTO t1 (a, b)\n" +
                "SELECT * FROM\n" +
                "  (SELECT c, d FROM t2\n" +
                "   UNION\n" +
                "   SELECT e, f FROM t3) AS dt\n" +
                "ON DUPLICATE KEY UPDATE b = b + c;";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT INTO t1 (a, b)\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "\tSELECT c, d\n" +
                "\tFROM t2\n" +
                "\tUNION\n" +
                "\tSELECT e, f\n" +
                "\tFROM t3\n" +
                ") dt\n" +
                "ON DUPLICATE KEY UPDATE b = b + c;", stmt.toString());
    }

    public void test_insert_5() throws Exception {
        String sql = "INSERT OVERWRITE INTO t1 (a, b)\n" +
                "SELECT * FROM\n" +
                "  (SELECT c, d FROM t2\n" +
                "   UNION\n" +
                "   SELECT e, f FROM t3) AS dt\n" +
                "ON DUPLICATE KEY UPDATE b = b + c;";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT OVERWRITE INTO t1 (a, b)\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "\tSELECT c, d\n" +
                "\tFROM t2\n" +
                "\tUNION\n" +
                "\tSELECT e, f\n" +
                "\tFROM t3\n" +
                ") dt\n" +
                "ON DUPLICATE KEY UPDATE b = b + c;", stmt.toString());
    }

    public void test_insert_6() throws Exception {
        String sql = "INSERT OVERWRITE INTO tablename1 PARTITION (partcol1=val1, partcol2=val2) IF NOT EXISTS  select * FROM from_statement;";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT OVERWRITE INTO tablename1 PARTITION (partcol1=val1, partcol2=val2) IF NOT EXISTS \n" +
                "SELECT *\n" +
                "FROM from_statement;", stmt.toString());
    }
}
