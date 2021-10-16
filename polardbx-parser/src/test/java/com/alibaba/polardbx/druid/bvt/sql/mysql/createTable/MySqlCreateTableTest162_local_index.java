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

/**
 * @version 1.0
 */
public class MySqlCreateTableTest162_local_index extends MysqlTest {

    public void testOne() throws Exception {
        String sql = "CREATE TABLE `local_table` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  LOCAL INDEX cl1 (`c1`),\n"
            + "  UNIQUE LOCAL INDEX cl2 (`c2`) dbpartition by hash(`c2`),\n"
            + "  LOCAL UNIQUE INDEX cl3 (`c3`) dbpartition by hash(`c3`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`c1`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals("CREATE TABLE `local_table` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(20) DEFAULT NULL,\n"
            + "\t`c2` bigint(20) DEFAULT NULL,\n"
            + "\t`c3` bigint(20) DEFAULT NULL,\n"
            + "\t`c4` bigint(20) DEFAULT NULL,\n"
            + "\t`c5` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tLOCAL INDEX cl1(`c1`),\n"
            + "\tUNIQUE LOCAL INDEX cl2 (`c2`) DBPARTITION BY hash(`c2`),\n"
            + "\tUNIQUE LOCAL INDEX cl3 (`c3`) DBPARTITION BY hash(`c3`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "DBPARTITION BY hash(`c1`);", stmt.toString());
    }

    public void testtwo() throws Exception {
        String sql = "CREATE TABLE `local_table` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  LOCAL KEY cl1 (`c1`),\n"
            + "  UNIQUE LOCAL KEY cl2 (`c2`) dbpartition by hash(`c2`),\n"
            + "  LOCAL UNIQUE KEY cl3 (`c3`) dbpartition by hash(`c3`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`c1`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals("CREATE TABLE `local_table` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(20) DEFAULT NULL,\n"
            + "\t`c2` bigint(20) DEFAULT NULL,\n"
            + "\t`c3` bigint(20) DEFAULT NULL,\n"
            + "\t`c4` bigint(20) DEFAULT NULL,\n"
            + "\t`c5` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tLOCAL KEY cl1 (`c1`),\n"
            + "\tUNIQUE LOCAL KEY cl2 (`c2`) DBPARTITION BY hash(`c2`),\n"
            + "\tUNIQUE LOCAL KEY cl3 (`c3`) DBPARTITION BY hash(`c3`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "DBPARTITION BY hash(`c1`);", stmt.toString());
    }

    public void testCreate0() throws Exception {
        String sql = "create local index `n_i` on `t_order` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE LOCAL INDEX `n_i` ON `t_order` (x)", statementList.get(0).toString());
    }

    public void testCreate1() throws Exception {
        String sql = "create local unique index `n_i` on `t_order` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE UNIQUE LOCAL INDEX `n_i` ON `t_order` (x)", statementList.get(0).toString());
    }

    public void testCreate2() throws Exception {
        String sql = "create unique local index `n_i` on `t_order` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE UNIQUE LOCAL INDEX `n_i` ON `t_order` (x)", statementList.get(0).toString());
    }

    public void testAlter0() throws Exception {
        String sql = "alter table t_order add local index `l_i` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("ALTER TABLE t_order\n"
            + "\tADD LOCAL INDEX `l_i` (x)", statementList.get(0).toString());
    }

    public void testAlter1() throws Exception {
        String sql = "alter table t_order add unique local index `l_i` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("ALTER TABLE t_order\n"
            + "\tADD UNIQUE LOCAL INDEX `l_i` (x)", statementList.get(0).toString());
    }

    public void testAlter2() throws Exception {
        String sql = "alter table t_order add local unique index `l_i` (x)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("ALTER TABLE t_order\n"
            + "\tADD UNIQUE LOCAL INDEX `l_i` (x)", statementList.get(0).toString());
    }

}
