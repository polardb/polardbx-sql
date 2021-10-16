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

public class MySqlCreateTableTest142 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `think_member_discount` (\n" +
                " `id`  bigint , \n" +
                " `uid`  bigint , \n" +
                " `config_id`  int(10) , \n" +
                " `cou_validity_time`  bigint(20) , \n" +
                " `order_id`  int(10) , \n" +
                " `config_order_type`  int(3) , \n" +
                " `use_order_type`  int(3) , \n" +
                " `platform`  int(5) , \n" +
                " `condition`  int(10) , \n" +
                " `consequence`  int(10) , \n" +
                " `is_use`  tinyint(3) , \n" +
                " `create_time`  bigint(20) , \n" +
                " `update_time`  bigint(20) , \n" +
                " `status`  tinyint(3) , \n" +
                " `unlock_time`  bigint , \n" +
                " `type`  smallint \n" +
                ", primary key (id) )  PARTITION BY hash key(id)\n" +
                " PARTITION NUM 128\n" +
                " TABLEGROUP mkzhan options (updateType='realtime') ";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `think_member_discount` (\n" +
                "\t`id` bigint,\n" +
                "\t`uid` bigint,\n" +
                "\t`config_id` int(10),\n" +
                "\t`cou_validity_time` bigint(20),\n" +
                "\t`order_id` int(10),\n" +
                "\t`config_order_type` int(3),\n" +
                "\t`use_order_type` int(3),\n" +
                "\t`platform` int(5),\n" +
                "\t`condition` int(10),\n" +
                "\t`consequence` int(10),\n" +
                "\t`is_use` tinyint(3),\n" +
                "\t`create_time` bigint(20),\n" +
                "\t`update_time` bigint(20),\n" +
                "\t`status` tinyint(3),\n" +
                "\t`unlock_time` bigint,\n" +
                "\t`type` smallint,\n" +
                "\tPRIMARY KEY (id)\n" +
                ")\n" +
                "OPTIONS (updateType = 'realtime')\n" +
                "PARTITION BY HASH KEY(id) PARTITION NUM 128\n" +
                "TABLEGROUP mkzhan", stmt.toString());

        assertEquals("create table `think_member_discount` (\n" +
                "\t`id` bigint,\n" +
                "\t`uid` bigint,\n" +
                "\t`config_id` int(10),\n" +
                "\t`cou_validity_time` bigint(20),\n" +
                "\t`order_id` int(10),\n" +
                "\t`config_order_type` int(3),\n" +
                "\t`use_order_type` int(3),\n" +
                "\t`platform` int(5),\n" +
                "\t`condition` int(10),\n" +
                "\t`consequence` int(10),\n" +
                "\t`is_use` tinyint(3),\n" +
                "\t`create_time` bigint(20),\n" +
                "\t`update_time` bigint(20),\n" +
                "\t`status` tinyint(3),\n" +
                "\t`unlock_time` bigint,\n" +
                "\t`type` smallint,\n" +
                "\tprimary key (id)\n" +
                ")\n" +
                "options (updateType = 'realtime')\n" +
                "partition by hash key(id) partition num 128\n" +
                "tablegroup mkzhan", stmt.toLowerCaseString());

    }





}