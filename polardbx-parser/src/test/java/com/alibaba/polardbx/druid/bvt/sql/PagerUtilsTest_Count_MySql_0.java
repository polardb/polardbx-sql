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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.PagerUtils;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

public class PagerUtilsTest_Count_MySql_0 extends TestCase {

    public void test_mysql_0() throws Exception {
        String sql = "select * from t";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(*)\n" + //
                            "FROM t", result);
    }

    public void test_mysql_1() throws Exception {
        String sql = "select id, name from t";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(*)\n" + //
                            "FROM t", result);
    }

    public void test_mysql_2() throws Exception {
        String sql = "select id, name from t order by id";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(*)\n" + //
                            "FROM t", result);
    }
    
    public void test_mysql_3() throws Exception {
        String sql = "select distinct id from t order by id";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(DISTINCT id)\n" + //
                            "FROM t", result);
    }

    public void test_mysql_4() throws Exception {
        String sql = "select distinct a.col1,a.col2 from test a";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        assertEquals("SELECT COUNT(DISTINCT a.col1, a.col2)\n" +
                "FROM test a", result);
    }

    public void test_mysql_group_0() throws Exception {
        String sql = "select type, count(*) from t group by type";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(*)\n" +
                "FROM (\n" +
                "\tSELECT type, count(*)\n" +
                "\tFROM t\n" +
                "\tGROUP BY type\n" +
                ") ALIAS_COUNT", result);
    }

    public void test_mysql_union_0() throws Exception {
        String sql = "select id, name from t1 union select id, name from t2 order by id";
        String result = PagerUtils.count(sql, JdbcConstants.MYSQL);
        Assert.assertEquals("SELECT COUNT(*)\n" +
                "FROM (\n" +
                "\tSELECT id, name\n" +
                "\tFROM t1\n" +
                "\tUNION\n" +
                "\tSELECT id, name\n" +
                "\tFROM t2\n" +
                ") ALIAS_COUNT", result);
    }

    public void test_mysql_select() throws Exception {
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements("select * from t", JdbcConstants.MYSQL).get(0);
        PagerUtils.limit(stmt.getSelect(), stmt.getDbType(), 10, 10);
        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "LIMIT 10, 10", stmt.toString());
    }

    public void test_mysql_groupBy() throws Exception {
        String countSql = PagerUtils.count(" SELECT * FROM order_biz GROUP BY product_id", DbType.mysql);
        assertEquals("SELECT COUNT(*)\n" +
                "FROM (\n" +
                "\tSELECT 1\n" +
                "\tFROM order_biz\n" +
                "\tGROUP BY product_id\n" +
                ") ALIAS_COUNT", countSql);
    }

}
