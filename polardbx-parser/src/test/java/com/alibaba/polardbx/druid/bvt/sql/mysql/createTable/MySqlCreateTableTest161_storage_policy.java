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
import com.alibaba.polardbx.druid.sql.parser.ParserException;


public class MySqlCreateTableTest161_storage_policy extends MysqlTest {

    public void test_0() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = 'HOT'";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE TABLE event_log (\n" +
                "\tlog_id bigint,\n" +
                "\tlog_time datetime\n" +
                ") STORAGE_POLICY = 'HOT'\n" +
                "DISTRIBUTE BY HASH(log_id)\n" +
                "PARTITION BY VALUE (date_format('%Y%m%d')) LIFECYCLE 180", stmt.toString());
    }

    public void test_1() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = 'COLD';";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE TABLE event_log (\n" +
                "\tlog_id bigint,\n" +
                "\tlog_time datetime\n" +
                ") STORAGE_POLICY = 'COLD'\n" +
                "DISTRIBUTE BY HASH(log_id)\n" +
                "PARTITION BY VALUE (date_format('%Y%m%d')) LIFECYCLE 180;", stmt.toString());
    }

    public void test_2() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = 'MIXED' hot_partition_count = 10;";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE TABLE event_log (\n" +
                "\tlog_id bigint,\n" +
                "\tlog_time datetime\n" +
                ") STORAGE_POLICY = 'MIXED' HOT_PARTITION_COUNT = 10\n" +
                "DISTRIBUTE BY HASH(log_id)\n" +
                "PARTITION BY VALUE (date_format('%Y%m%d')) LIFECYCLE 180;", stmt.toString());
    }

    public void test_3() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = 'MIXED' hot_partition_count = 0.1;";

        try {
            SQLUtils.parseSingleMysqlStatement(sql);
            fail();
        } catch (ParserException e) {

        }
    }

    public void test_4() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = \"HOT\" hot_partition_count = 20;";

        SQLUtils.parseSingleMysqlStatement(sql);
    }

    public void test_5() throws Exception {
        //for ADB
        String sql = "create table event_log(log_id bigint, log_time datetime)\n" +
                "distribute by hash(log_id)\n" +
                "partition by value(date_format('%Y%m%d')) lifecycle 180\n" +
                "storage_policy = \"COLD\" hot_partition_count = 1;";

        SQLUtils.parseSingleMysqlStatement(sql);
    }
}