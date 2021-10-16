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

import com.alibaba.polardbx.druid.sql.PagerUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class PagerUtilsTest_Limit_mysql_0 extends TestCase {

    public void test_mysql_0() throws Exception {
        String sql = "select * from t";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 0, 10);
        assertEquals("SELECT *" + //
                            "\nFROM t" + //
                            "\nLIMIT 10", result);
    }

    public void test_mysql_1() throws Exception {
        String sql = "select * from t";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 10, 10);
        assertEquals("SELECT *" + //
                            "\nFROM t" + //
                            "\nLIMIT 10, 10", result);
    }

    public void test_mysql_2() throws Exception {
        String sql = "select * from t";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 20, 10);
        assertEquals("SELECT *" + //
                            "\nFROM t" + //
                            "\nLIMIT 20, 10", result);
    }

    public void test_mysql_3() throws Exception {
        String sql = "select id, name, salary from t order by id, name";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 20, 10);
        assertEquals("SELECT id, name, salary" + //
                            "\nFROM t" + //
                            "\nORDER BY id, name" + //
                            "\nLIMIT 20, 10", result);
    }

    public void test_mysql_4() throws Exception {
        String sql = "SELECT *\n" +
                "FROM test.a\n" +
                "WHERE a.field0 = 0\n" +
                "\tAND (a.field1 LIKE BINARY '1'\n" +
                "\tOR a.field2 = 2)\n";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 0, 10);
        assertEquals("SELECT *\n" +
                "FROM test.a\n" +
                "WHERE a.field0 = 0\n" +
                "\tAND (a.field1 LIKE BINARY '1'\n" +
                "\t\tOR a.field2 = 2)\n" +
                "LIMIT 10", result);
    }

    public void test_mysql_5() throws Exception {
        String sql = " SELECT * FROM order_biz GROUP BY product_id";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 0, 10);
        assertEquals("SELECT *\n" +
                "FROM order_biz\n" +
                "GROUP BY product_id\n" +
                "LIMIT 10", result);
    }

    public void test_mysql_6() throws Exception {
        String sql = "select * from t limit 10";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 0, 100, true);
        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "LIMIT 10", result);
    }

    public void test_mysql_7() throws Exception {
        String sql = "select * from t limit 1000, 1000";
        String result = PagerUtils.limit(sql, JdbcConstants.MYSQL, 0, 100, true);
        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "LIMIT 1000, 100", result);
    }
}
