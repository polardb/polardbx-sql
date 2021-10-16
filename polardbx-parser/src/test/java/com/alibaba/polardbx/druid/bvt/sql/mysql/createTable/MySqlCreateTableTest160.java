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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableLike;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;


public class MySqlCreateTableTest160 extends MysqlTest {

    public void test_0() throws Exception {
        //for ADB
        String sql = "CREATE TABLE IF NOT EXISTS bar (LIKE a INCLUDING PROPERTIES)";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertTrue(stmt.getTableElementList().get(0) instanceof SQLTableLike);
        assertEquals("CREATE TABLE IF NOT EXISTS bar (\n" +
                "\tLIKE a INCLUDING PROPERTIES\n" +
                ")", stmt.toString());
    }

    public void test_1() throws Exception {
        //for ADB
        String sql = "CREATE TABLE IF NOT EXISTS bar2 (c TIMESTAMP, LIKE bar, d DATE)";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertTrue(stmt.getTableElementList().get(1) instanceof SQLTableLike);
        assertEquals("CREATE TABLE IF NOT EXISTS bar2 (\n" +
                "\tc TIMESTAMP,\n" +
                "\tLIKE bar,\n" +
                "\td DATE\n" +
                ")", stmt.toString());
    }
}