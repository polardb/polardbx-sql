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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import org.junit.Assert;

public class MySqlSelectTest_315_drds_flashback extends MysqlTest {
    private void check(String expected, SQLStatement actual) {
        check(expected, SQLUtils.toMySqlString(actual));
    }

    private void check(String expected, String actual) {
        Assert.assertEquals(expected, actual);
    }

    public void test_0() throws Exception {
        SQLStatement stmt = parse("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14';");

        check("SELECT s1\nFROM t1\nAS OF TIMESTAMP '2021-03-02 11:45:14';", stmt);
        check("select s1\nfrom t1\nas of timestamp '2021-03-02 11:45:14';",
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_1() throws Exception {
        SQLStatement stmt = parse("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' AS t;");
        check("SELECT s1\n"
            + "FROM t1\n"
            + "AS OF TIMESTAMP '2021-03-02 11:45:14' t;", stmt);

        stmt = parse("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' t;");
        check("SELECT s1\n"
            + "FROM t1\n"
            + "AS OF TIMESTAMP '2021-03-02 11:45:14' t;", stmt);
    }

    public void test_2() throws Exception {
        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' AS a FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");
    }

    public void test_3() throws Exception {
        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' PARTITION(p1,p2) FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' PARTITION(p1,p2) AS a FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' PARTITION(p1,p2) a FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' PARTITION(p1,p2) a FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");
    }

    public void test_4() throws Exception {
        parseTrue("SELECT s1 FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' AS a PARTITION(p1,p2) FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");

        parseTrue("SELECT s1 FROM t1 a PARTITION(p1,p2) AS OF TIMESTAMP '2021-03-02 11:45:14' FORCE INDEX(g_i_1);",
            "SELECT s1\n"
                + "FROM t1 PARTITION (p1, \n"
                + "p2)\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (g_i_1);");

        // PARTITION and AS OF must ahead of index hint
        parseFalse("SELECT s1 "
                + "FROM t1 a FORCE INDEX(g_i_1) "
                + "PARTITION(p1,p2) "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14';",
            "syntax error, error in :'INDEX(g_i_1) PARTITION(p1,p2) AS OF TIMESTA, pos 48, line 1, column 40, token PARTITION");

        parseFalse("SELECT s1 "
                + "FROM t1 FORCE INDEX(g_i_1) "
                + "PARTITION(p1,p2) "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14';",
            "syntax error, error in :'INDEX(g_i_1) PARTITION(p1,p2) AS OF TIMESTA, pos 46, line 1, column 38, token PARTITION");
    }

    public void test_5() throws Exception {
        parseFalse("SELECT s1 "
                + "FROM (SELECT s2, 1 FROM t1) "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' "
                + "AS a;",
            "syntax error, error in :'(SELECT s2, 1 FROM t1) AS OF TIMESTAMP '2021-03-02 11, pos 40, line 1, column 45, token AS");

        parseFalse("SELECT s1 "
                + "FROM (SELECT s2, 1 FROM t1) "
                + "AS a "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14';",
            "syntax error, error in :'(SELECT s2, 1 FROM t1) AS a AS OF TIMESTAMP '2021-03-02 11, pos 45, line 1, column 50, token AS");
    }

    public void test_6() throws Exception {
        parseFalse("SELECT s1 "
                + "FROM (SELECT 1, 2 UNION SELECT 3,4) "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' "
                + "AS a;",
            "syntax error, error in :'2 UNION SELECT 3,4) AS OF TIMESTAMP '2021-03-02 11, pos 48, line 1, column 53, token AS");

        parseFalse("SELECT s1 "
                + "FROM (SELECT 1, 2 UNION SELECT 3,4) "
                + "AS a "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14';",
            "syntax error, error in :'2 UNION SELECT 3,4) AS a AS OF TIMESTAMP '2021-03-02 11, pos 53, line 1, column 58, token AS");
    }

    public void test_join_0() throws Exception {
        parseTrue(
            "SELECT s1 "
                + "FROM t1 AS OF TIMESTAMP '2021-03-02 11:45:14' AS a "
                + "JOIN t2 AS OF TIMESTAMP '2021-04-02 11:45:14' AS b;",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a\n"
                + "\tJOIN t2\n"
                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b;");
    }

    public void test_join_1() throws Exception {
        parseTrue(
            "SELECT s1 "
                + "FROM t1 "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' "
                + "AS a FORCE INDEX(`g_i`) "
                + "INNER JOIN t2 "
                + "AS OF TIMESTAMP '2021-04-02 11:45:14' "
                + "AS b;",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (`g_i`)\n"
                + "\tINNER JOIN t2\n"
                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b;");

        parseTrue(
            "SELECT s1 "
                + "FROM t1 "
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' "
                + "AS a FORCE INDEX(`g_i`) "
                + "INNER JOIN t2 "
                + "AS OF TIMESTAMP '2021-04-02 11:45:14' "
                + "AS b USE INDEX(`g_i_1`);",
            "SELECT s1\n"
                + "FROM t1\n"
                + "AS OF TIMESTAMP '2021-03-02 11:45:14' a FORCE INDEX (`g_i`)\n"
                + "\tINNER JOIN t2\n"
                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b USE INDEX (`g_i_1`);");
    }

    public void test_join_2() throws Exception {
        parseTrue(
            "SELECT s1 "
                + "FROM t1 "
                + "AS a "
                + "IGNORE INDEX(`g_i`) "
                + "INNER JOIN t2 PARTITION (p1,p2) "
                + "AS OF TIMESTAMP '2021-04-02 11:45:14' "
                + "AS b "
                + "USE INDEX(`g_i_1`);",
            "SELECT s1\n"
                + "FROM t1 a IGNORE INDEX (`g_i`)\n"
                + "\tINNER JOIN t2 PARTITION (p1, \n"
                + "\tp2)\n"
                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b USE INDEX (`g_i_1`);");

        parseTrue(
            "SELECT s1 "
                + "FROM t1 "
                + "AS a "
                + "IGNORE INDEX(`g_i`) "
                + "INNER JOIN t2 PARTITION (p1,p2) "
                + "AS b "
                + "AS OF TIMESTAMP '2021-04-02 11:45:14' "
                + "USE INDEX(`g_i_1`);",
            "SELECT s1\n"
                + "FROM t1 a IGNORE INDEX (`g_i`)\n"
                + "\tINNER JOIN t2 PARTITION (p1, \n"
                + "\tp2)\n"
                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b USE INDEX (`g_i_1`);");

        // JOIN PARTITION is not ready, skip for now
//        parseTrue(
//            "SELECT s1 "
//                + "FROM t1 "
//                + "AS a "
//                + "IGNORE INDEX(`g_i`) "
//                + "INNER JOIN t2 b "
//                + "USE INDEX(`g_i_1`) "
//                + "PARTITION (p1,p2) "
//                + "AS OF TIMESTAMP '2021-04-02 11:45:14';",
//            "SELECT s1\n"
//                + "FROM t1 a IGNORE INDEX (`g_i`)\n"
//                + "\tINNER JOIN t2 PARTITION (p1, \n"
//                + "\tp2)\n"
//                + "\tAS OF TIMESTAMP '2021-04-02 11:45:14' b USE INDEX (`g_i_1`);");
    }
}