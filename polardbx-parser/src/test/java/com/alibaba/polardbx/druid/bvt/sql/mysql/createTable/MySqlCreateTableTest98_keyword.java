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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateTableTest98_keyword extends MysqlTest {

    public void test_0() throws Exception {
        String sql = " CREATE TABLE IF NOT EXISTS meta.view (\n" +
                "                cluster_name varchar(16) ,\n" +
                "                table_schema varchar(128), \n" +
                "                view_name varchar(128), \n" +
                "                column_list text, \n" +
                "                sql text\n" +
                "                )";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(5, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS meta.view (\n" +
                "\tcluster_name varchar(16),\n" +
                "\ttable_schema varchar(128),\n" +
                "\tview_name varchar(128),\n" +
                "\tcolumn_list text,\n" +
                "\tsql text\n" +
                ")", stmt.toString());

    }

    public void test_1() throws Exception {
        String sql = " CREATE TABLE IF NOT EXISTS meta.partitions (\n" +
                "                cluster_name varchar(16) ,\n" +
                "                table_schema varchar(128), \n" +
                "                view_name varchar(128), \n" +
                "                column_list text, \n" +
                "                sql text\n" +
                "                )";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(5, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS meta.partitions (\n" +
                "\tcluster_name varchar(16),\n" +
                "\ttable_schema varchar(128),\n" +
                "\tview_name varchar(128),\n" +
                "\tcolumn_list text,\n" +
                "\tsql text\n" +
                ")", stmt.toString());

    }

    public void test_2() throws Exception {
        String sql = " CREATE TABLE IF NOT EXISTS meta.partitions (\n" +
                "                cluster_name varchar(16) ,\n" +
                "                table_schema varchar(128), \n" +
                "                partition varchar(128), \n" +
                "                column_list text, \n" +
                "                sql text\n" +
                "                )";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(5, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS meta.partitions (\n" +
                "\tcluster_name varchar(16),\n" +
                "\ttable_schema varchar(128),\n" +
                "\tpartition varchar(128),\n" +
                "\tcolumn_list text,\n" +
                "\tsql text\n" +
                ")", stmt.toString());

    }

    public void test_3() throws Exception {
        String sql = "create table IF NOT EXISTS meta.build_table_statistic_info("
                + " cluster_name varchar(16), "
                + " table_schema varchar(128), "
                + " table_name varchar(128), "
                + " key varchar(128), "
                + " value varchar(128), "
                + " table_schema_id varchar(128), "
                + " table_id varchar(128), "
                + " data_version bigint, "
                + " create_time timestamp, "
                + " update_time timestamp"
                + ")";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(10, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS meta.build_table_statistic_info (\n" +
                "\tcluster_name varchar(16),\n" +
                "\ttable_schema varchar(128),\n" +
                "\ttable_name varchar(128),\n" +
                "\tkey varchar(128),\n" +
                "\tvalue varchar(128),\n" +
                "\ttable_schema_id varchar(128),\n" +
                "\ttable_id varchar(128),\n" +
                "\tdata_version bigint,\n" +
                "\tcreate_time timestamp,\n" +
                "\tupdate_time timestamp\n" +
                ")", stmt.toString());

    }

    public void test_4() throws Exception {
        String sql = "CREATE TABLE test (\n" +
                "Host char(60) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "Db char(64) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "User char(16) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "PRIMARY KEY (Host,Db,User),\n" +
                "KEY User (User)\n" +
                ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Database privileges'";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(5, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE test (\n" +
                "\tHost char(60) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "\tDb char(64) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "\tUser char(16) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
                "\tPRIMARY KEY (Host, Db, User),\n" +
                "\tKEY User (User)\n" +
                ") ENGINE = MyISAM DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_bin COMMENT 'Database privileges'", stmt.toString());

    }
}