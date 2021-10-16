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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;

/**
 * @version 1.0
 * @ClassName MySqlCreateTableTest158_default_charset
 * @description
 * @Author zzy
 * @Date 2019/12/17 14:33
 */
public class MySqlCreateTableTest158_default_charset extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "`id` int(11) NOT NULL,\n" +
                "`name` varchar(16) DEFAULT NULL,\n" +
                "`birthday` date DEFAULT NULL,\n" +
                "PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "\t`id` int(11) NOT NULL,\n" +
                "\t`name` varchar(16) DEFAULT NULL,\n" +
                "\t`birthday` date DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "`id` int(11) NOT NULL,\n" +
                "`name` varchar(16) DEFAULT NULL,\n" +
                "`birthday` date DEFAULT NULL,\n" +
                "PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "\t`id` int(11) NOT NULL,\n" +
                "\t`name` varchar(16) DEFAULT NULL,\n" +
                "\t`birthday` date DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "`id` int(11) NOT NULL,\n" +
                "`name` varchar(16) DEFAULT NULL,\n" +
                "`birthday` date DEFAULT NULL,\n" +
                "PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "\t`id` int(11) NOT NULL,\n" +
                "\t`name` varchar(16) DEFAULT NULL,\n" +
                "\t`birthday` date DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4", stmt.toString());
    }

    public void test_3() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "`id` int(11) NOT NULL,\n" +
                "`name` varchar(16) DEFAULT NULL,\n" +
                "`birthday` date DEFAULT NULL,\n" +
                "PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "\t`id` int(11) NOT NULL,\n" +
                "\t`name` varchar(16) DEFAULT NULL,\n" +
                "\t`birthday` date DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4", stmt.toString());
    }


    public void test_4() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "`id` int(11) NOT NULL,\n" +
                "`name` varchar(16) DEFAULT NULL,\n" +
                "`birthday` date DEFAULT NULL,\n" +
                "PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB CHARSET = utf8 COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE IF NOT EXISTS `xht` (\n" +
                "\t`id` int(11) NOT NULL,\n" +
                "\t`name` varchar(16) DEFAULT NULL,\n" +
                "\t`birthday` date DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci\n" +
                "DBPARTITION BY HASH(id)\n" +
                "TBPARTITION BY hash(`id`) TBPARTITIONS 4", stmt.toString());
    }
}