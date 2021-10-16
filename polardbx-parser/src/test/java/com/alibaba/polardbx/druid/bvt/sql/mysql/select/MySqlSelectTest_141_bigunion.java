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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_141_bigunion extends MysqlTest {

    public void test_small_10() throws Exception {
        StringBuilder buf = new StringBuilder();
        {
            buf.append("select * from (\n");

            for (int i = 0; i < 10; ++i) {
                if (i != 0) {
                    buf.append("union all\n");
                }
                buf.append("select :").append(i).append(" as sid from dual\n");
            }

            buf.append("\n) ux_x");
        }
        String sql = buf.toString();

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);

        assertEquals(1, statementList.size());

        SQLSelectStatement stmt = (SQLSelectStatement) statementList.get(0);
        System.out.println(stmt);

        assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT :0 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :1 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :2 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :3 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :4 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :5 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :6 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :7 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :8 AS sid\n" +
                "\tFROM dual\n" +
                "\tUNION ALL\n" +
                "\tSELECT :9 AS sid\n" +
                "\tFROM dual\n" +
                ") ux_x", stmt.toString());
    }


    public void test_big_100000() throws Exception {
        StringBuilder buf = new StringBuilder();
        {
            buf.append("select * from (\n");

            for (int i = 0; i < 1000 * 100; ++i) {
                if (i != 0) {
                    buf.append("union all\n");
                }
                buf.append("select :").append(i).append(" as sid from dual\n");
            }

            buf.append("\n) ux_x");
        }
        String sql = buf.toString();

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);

        assertEquals(1, statementList.size());

        SQLSelectStatement stmt = (SQLSelectStatement) statementList.get(0);
        stmt.toString();

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(statVisitor);
    }

}