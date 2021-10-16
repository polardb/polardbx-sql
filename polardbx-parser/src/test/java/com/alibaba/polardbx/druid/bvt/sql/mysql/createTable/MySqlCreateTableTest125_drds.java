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

public class MySqlCreateTableTest125_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE t_order (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  `order_snapshot` longtext DEFAULT NULL,\n" +
                "  `order_detail` longtext DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `l_i_order` (`order_id`),\n" +
                "  GLOBAL INDEX `g_i_seller` (`seller_id`) dbpartition by hash(`seller_id`),\n" +
                "  UNIQUE GLOBAL `g_i_buyer` (`buyer_id`) COVERING (order_snapshot) dbpartition by hash(`buyer_id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);";
//        System.out.println(sql);

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE t_order (\n" +
                "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n" +
                "\t`order_id` varchar(20) DEFAULT NULL,\n" +
                "\t`buyer_id` varchar(20) DEFAULT NULL,\n" +
                "\t`seller_id` varchar(20) DEFAULT NULL,\n" +
                "\t`order_snapshot` longtext DEFAULT NULL,\n" +
                "\t`order_detail` longtext DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tKEY `l_i_order` (`order_id`),\n" +
                "\tGLOBAL INDEX `g_i_seller`(`seller_id`) DBPARTITION BY hash(`seller_id`),\n" +
                "\tUNIQUE GLOBAL `g_i_buyer` (`buyer_id`) COVERING (order_snapshot) DBPARTITION BY hash(`buyer_id`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "DBPARTITION BY hash(`order_id`);", stmt.toString());

    }

    public void test_1() throws Exception {
        String sql = "CREATE TABLE t_order (\n" +
            "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `order_id` varchar(20) DEFAULT NULL,\n" +
            "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
            "  `seller_id` varchar(20) DEFAULT NULL,\n" +
            "  `order_snapshot` longtext DEFAULT NULL,\n" +
            "  `order_detail` longtext DEFAULT NULL,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `l_i_order` (`order_id`),\n" +
            "  GLOBAL INDEX `g_i_seller` (`seller_id`) covering(`order_snapshot`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 3\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE t_order (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext DEFAULT NULL,\n"
            + "\t`order_detail` longtext DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tKEY `l_i_order` (`order_id`),\n"
            + "\tGLOBAL INDEX `g_i_seller`(`seller_id`) COVERING (`order_snapshot`) DBPARTITION BY hash(`seller_id`) TBPARTITION BY hash"
            + "(`seller_id`) TBPARTITIONS 3\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "DBPARTITION BY hash(`order_id`);", stmt.toString());

    }

    public void test_2() throws Exception {
        String sql = "CREATE TABLE t_order (\n" +
            "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `order_id` varchar(20) DEFAULT NULL,\n" +
            "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
            "  `seller_id` varchar(20) DEFAULT NULL,\n" +
            "  `order_snapshot` longtext DEFAULT NULL,\n" +
            "  `order_detail` longtext DEFAULT NULL,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `l_i_order` (`order_id`),\n" +
            "  UNIQUE GLOBAL INDEX `g_i_seller` (`seller_id`) covering (order_snapshot) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 3\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE t_order (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext DEFAULT NULL,\n"
            + "\t`order_detail` longtext DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tKEY `l_i_order` (`order_id`),\n"
            + "\tUNIQUE GLOBAL INDEX `g_i_seller` (`seller_id`) COVERING (order_snapshot) DBPARTITION BY hash(`seller_id`)"
            + " TBPARTITION BY hash(`seller_id`) TBPARTITIONS 3\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "DBPARTITION BY hash(`order_id`);", stmt.toString());

    }

    public void test_3() throws Exception {
        String sql = "CREATE TABLE t_order (\n" + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                     + "  `order_id` varchar(20) DEFAULT NULL,\n" + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
                     + "  `seller_id` varchar(20) DEFAULT NULL,\n" + "  `order_snapshot` longtext DEFAULT NULL,\n"
                     + "  `order_detail` longtext DEFAULT NULL,\n" + "  PRIMARY KEY (`id`),\n"
                     + "  KEY `l_i_order` (`order_id`),\n"
                     + "  UNIQUE GLOBAL `g_i_seller` (`seller_id`) covering (order_snapshot) "
                     + "DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY UNI_HASH(SELLER_ID) TBPARTITIONS 12"
                     + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TABLE t_order (\n" + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                     + "\t`order_id` varchar(20) DEFAULT NULL,\n" + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
                     + "\t`seller_id` varchar(20) DEFAULT NULL,\n" + "\t`order_snapshot` longtext DEFAULT NULL,\n"
                     + "\t`order_detail` longtext DEFAULT NULL,\n" + "\tPRIMARY KEY (`id`),\n"
                     + "\tKEY `l_i_order` (`order_id`),\n"
                     + "\tUNIQUE GLOBAL `g_i_seller` (`seller_id`) COVERING (order_snapshot) "
                     + "DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY UNI_HASH(SELLER_ID) TBPARTITIONS 12\n"
                     + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" + "DBPARTITION BY hash(`order_id`);",
            stmt.toString());

    }

    public void test_4() throws Exception {
        String sql = "CREATE TABLE t_order (\n"
                     + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                     + "  `order_id` varchar(20) DEFAULT NULL,\n"
                     + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
                     + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                     + "  `order_snapshot` longtext DEFAULT NULL,\n"
                     + "  `order_detail` longtext DEFAULT NULL,\n"
                     + "  PRIMARY KEY (`id`),\n"
                     + "  KEY `l_i_order` (`order_id`),\n"
                     + "  UNIQUE GLOBAL `g_i_seller` (`seller_id`) covering (order_snapshot) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 3 COMMENT \"CREATE GSI TEST\"\n"
                     + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TABLE t_order (\n"
                     + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                     + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                     + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
                     + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                     + "\t`order_snapshot` longtext DEFAULT NULL,\n"
                     + "\t`order_detail` longtext DEFAULT NULL,\n"
                     + "\tPRIMARY KEY (`id`),\n"
                     + "\tKEY `l_i_order` (`order_id`),\n"
                     + "\tUNIQUE GLOBAL `g_i_seller` (`seller_id`) COVERING (order_snapshot) DBPARTITION BY hash(`seller_id`)"
                     + " TBPARTITION BY hash(`seller_id`) TBPARTITIONS 3 COMMENT 'CREATE GSI TEST'\n"
                     + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" + "DBPARTITION BY hash(`order_id`);",
            stmt.toString());

    }

}
