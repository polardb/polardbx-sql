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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_138_ads_minus extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1995-09-31'\n" +
                "UNION\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1994-09-31'\n" +
                "INTERSECT\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1996-09-31'\n" +
                "MINUS\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1994-09-31'\n";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1995-09-31'\n" +
                "UNION\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1994-09-31'\n" +
                "INTERSECT\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1996-09-31'\n" +
                "MINUS\n" +
                "SELECT DISTINCT o_orderkey\n" +
                "FROM simple_query2.orders__0\n" +
                "WHERE o_orderdate > '1994-09-31'", stmt.toString());

        SQLUnionQuery q1 = (SQLUnionQuery) stmt.getSelect().getQuery();
        assertTrue(q1.getRight() instanceof SQLSelectQueryBlock);

        SQLUnionQuery q2 = (SQLUnionQuery) q1.getLeft();
        assertTrue(q2.getRight() instanceof SQLSelectQueryBlock);


        SQLUnionQuery q3 = (SQLUnionQuery) q2.getLeft();
        assertTrue(q3.getRight() instanceof SQLSelectQueryBlock);
        assertTrue(q3.getLeft() instanceof SQLSelectQueryBlock);
    }


}