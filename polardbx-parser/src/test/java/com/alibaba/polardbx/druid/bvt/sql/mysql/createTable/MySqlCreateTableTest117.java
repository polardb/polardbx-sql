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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlCreateTableTest117 extends MysqlTest {

    public void test() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (id int(10) auto_increment" +
                " ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10) AUTO_INCREMENT\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (id int(10) auto_increment by GROUP" +
                " ) auto_increment=12313 ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10) AUTO_INCREMENT BY GROUP\n" +
                ") AUTO_INCREMENT = 12313 ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_1() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (id int(10) auto_increment by simple" +
                " ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10) AUTO_INCREMENT BY SIMPLE\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_2() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (id int(10) auto_increment by simple with cache" +
                " ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10) AUTO_INCREMENT BY SIMPLE WITH CACHE\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_3() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (id int(10) auto_increment by time" +
                " ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10) AUTO_INCREMENT BY TIME\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

}