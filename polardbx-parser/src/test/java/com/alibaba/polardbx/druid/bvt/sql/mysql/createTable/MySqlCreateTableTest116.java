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

public class MySqlCreateTableTest116 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "   id int(10)" +
                " ) broadcast ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
                "\tid int(10)\n" +
                ") BROADCAST ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_1() throws Exception {
        String sql = "CREATE TABLE `t1` (\n" +
                "  `id` int NOT NULL,\n" +
                "  `e_id` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "  `m_id` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "  `m_id1` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "  `m_id2` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "  `name` varchar(256) COLLATE utf8_bin DEFAULT NULL,\n" +
                "  `addr` varchar(45) COLLATE utf8_bin DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `index_e_id` (`e_id`),\n" +
                "  index `index_m_id` (`m_id`),\n" +
                "  KEY `index_m1_id` (m_id1(10)),\n" +
                "  index `index_m2_id` (m_id2(30))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `t1` (\n" +
                "\t`id` int NOT NULL,\n" +
                "\t`e_id` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "\t`m_id` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "\t`m_id1` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "\t`m_id2` varchar(45) COLLATE utf8_bin NOT NULL,\n" +
                "\t`name` varchar(256) COLLATE utf8_bin DEFAULT NULL,\n" +
                "\t`addr` varchar(45) COLLATE utf8_bin DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tKEY `index_e_id` (`e_id`),\n" +
                "\tINDEX `index_m_id`(`m_id`),\n" +
                "\tKEY `index_m1_id` (m_id1(10)),\n" +
                "\tINDEX `index_m2_id`(m_id2(30))\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

    public void test_2() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `Employee` (\n" +
            "   id int(10)" +
            " ) single ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS `Employee` (\n" +
            "\tid int(10)\n" +
            ") SINGLE ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin", stmt.toString());


    }

}