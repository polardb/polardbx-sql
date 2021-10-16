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

package com.alibaba.polardbx.druid.bvt.bug;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import junit.framework.TestCase;

public class MySqlLogicTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "SELECT ALL + + CASE + 31 WHEN CAST( NULL AS SIGNED ) THEN + CAST( + 4 AS SIGNED ) END * + - ( + 91 )";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL (+(+CASE +31\n" +
                "\t\t\tWHEN CAST(NULL AS SIGNED) THEN +CAST(+4 AS SIGNED)\n" +
                "\t\tEND)) * +(-(+91))", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "SELECT ALL + 42 + - 29 / - CASE - - 30 WHEN 59 THEN 95 * 23 WHEN 67 THEN NULL ELSE NULL END + 85 ";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL +42 + -29 / -CASE --30\n" +
                "\t\tWHEN 59 THEN 95 * 23\n" +
                "\t\tWHEN 67 THEN NULL\n" +
                "\t\tELSE NULL\n" +
                "\tEND + 85", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "SELECT ALL - CASE - ( + COALESCE ( 55, - + COUNT( * ) ) ) WHEN - 52 THEN - + 22 WHEN - - COUNT( * ) * 94 + - - 23 THEN NULL ELSE NULL END ";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL -CASE -(+COALESCE(55, -(+COUNT(*))))\n" +
                "\t\tWHEN -52 THEN -(+22)\n" +
                "\t\tWHEN (-(-COUNT(*))) * 94 + --23 THEN NULL\n" +
                "\t\tELSE NULL\n" +
                "\tEND", stmt.toString());
    }

    public void test_3() throws Exception {
        String sql = "SELECT ALL CASE WHEN NOT COUNT( * ) IS NOT NULL THEN NULL WHEN NULL <= - COUNT( * ) THEN + 11 / + 69 WHEN NOT ( + 31 / - 78 ) IS NULL THEN - 18 + 83 ELSE NULL END ";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL CASE \n" +
                "\t\tWHEN NOT COUNT(*) IS NOT NULL THEN NULL\n" +
                "\t\tWHEN NULL <= -COUNT(*) THEN (+11) / +69\n" +
                "\t\tWHEN NOT (+31) / -78 IS NULL THEN -18 + 83\n" +
                "\t\tELSE NULL\n" +
                "\tEND", stmt.toString());
    }

    public void test_4() throws Exception {
        String sql = "SELECT ALL - 31 * + 6 DIV + 74 DIV - + MAX( + - 82 ) DIV + + 69 + - 93";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL -31 * +6 DIV (+74) DIV (-(+MAX(+-82))) DIV +(+69) + -93", stmt.toString());
    }

    public void test_5() throws Exception {
        String sql = "SELECT + 10 * + 12 DIV + + 52 DIV - 4";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT (+10) * +12 DIV (+(+52)) DIV -4", stmt.toString());
    }

    public void test_6() throws Exception {
        String sql = "SELECT + 10 * - 12 DIV + + 52 DIV - 4";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT (+10) * -12 DIV (+(+52)) DIV -4", stmt.toString());
    }

    public void test_7() throws Exception {
        String sql = "SELECT - 21 DIV - - 14 * + 74 DIV - 44 DIV - 15 AS col0 ";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT -21 DIV --14 * +74 DIV -44 DIV -15 AS col0", stmt.toString());
    }

    public void test_8() throws Exception {
        String sql = "SELECT ALL + + COUNT( * ) + + \n" +
                "CASE WHEN NOT 45 <> AVG ( - 35 ) THEN - 38 \n" +
                "WHEN NOT NULL IS NOT NULL THEN - 36 * NULLIF ( - 63, + - 17 ) - 20 + + MAX( - + 66 ) ELSE NULL END * + 24 * + COUNT( * ) ";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT ALL +(+COUNT(*)) + (+CASE \n" +
                "\t\tWHEN NOT 45 <> AVG(-35) THEN -38\n" +
                "\t\tWHEN NOT NULL IS NOT NULL THEN -36 * NULLIF(-63, +-17) - 20 + +MAX(-(+66))\n" +
                "\t\tELSE NULL\n" +
                "\tEND) * (+24) * +COUNT(*)", stmt.toString());
    }

    public void test_9() throws Exception {
        String sql = "SELECT stddev_pop(1)\n" +
                "ORDER BY 1";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT stddev_pop(1)\n" +
                "ORDER BY 1", stmt.toString());
    }

    public void test_10() throws Exception {
        String sql = "SELECT - col1 BETWEEN 3 AND 46";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT (-col1) BETWEEN 3 AND 46", stmt.toString());
    }

    public void test_11() throws Exception {
        String sql = "select a.id,a.coid,b.coname from student_hobby a natural left join community b order by 1,2,3;";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT a.id, a.coid, b.coname\n" +
                "FROM student_hobby a\n" +
                "\tNATURAL LEFT JOIN community b\n" +
                "ORDER BY 1, 2, 3;", stmt.toString());
    }

    public void test_12() throws Exception {
        String sql = "SELECT `date_add`('1992-12-31 23:59:59.000002', INTERVAL  '1.999' SECOND TO MICROSECOND)\n" +
                "\n";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT `date_add`('1992-12-31 23:59:59.000002', INTERVAL '1.999' SECOND_MICROSECOND)", stmt.toString());
    }

    public void test_13() throws Exception {
        String sql = "select 1.21E10001";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("SELECT 1.21E10001", stmt.toString());
    }

}
