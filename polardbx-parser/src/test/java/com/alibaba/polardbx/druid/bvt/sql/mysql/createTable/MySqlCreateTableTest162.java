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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;


public class MySqlCreateTableTest162 extends MysqlTest {

    public void test_0() throws Exception {
        //for ADB
        String sql = "CREATE TABLE zz AS\n" +
                "WITH t AS (SELECT * FROM z),\n" +
                "t1 AS (SELECT * FROM f)\n" +
                "SELECT * FROM t JOIN t1 ON t.id = t1.id\n" +
                "WHERE t1.age > 10";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE TABLE zz\n" +
                "AS\n" +
                "WITH t AS (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM z\n" +
                "\t), \n" +
                "\tt1 AS (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM f\n" +
                "\t)\n" +
                "SELECT *\n" +
                "FROM t\n" +
                "\tJOIN t1 ON t.id = t1.id\n" +
                "WHERE t1.age > 10", stmt.toString());
    }
}