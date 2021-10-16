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

public class MySqlCreateTableTest130_ads extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS hm_crm.crm_wdk_hm_store_poi_di\n" +
                "(\n" +
                "    shop_id    BIGINT COMMENT '店铺id',\n" +
                "    poi_type   BIGINT COMMENT '0：家 1： 工作',\n" +
                "    poi        VARCHAR COMMENT 'poi信息',\n" +
                "    user_count BIGINT COMMENT '用户量'\n" +
                ")\n" +
                "PARTITION BY HASH KEY (shop_id) PARTITION NUM 250\n" +
                "SUBPARTITION BY LIST KEY (biz_date long)\n" +
                "SUBPARTITION OPTIONS (available_partition_num = 1)\n" +
                "TABLEGROUP crm_platform_filter\n" +
                "OPTIONS (UPDATETYPE='batch')\n" +
                "COMMENT '店铺poi数据'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE IF NOT EXISTS hm_crm.crm_wdk_hm_store_poi_di (\n" +
                "\tshop_id BIGINT COMMENT '店铺id',\n" +
                "\tpoi_type BIGINT COMMENT '0：家 1： 工作',\n" +
                "\tpoi VARCHAR COMMENT 'poi信息',\n" +
                "\tuser_count BIGINT COMMENT '用户量'\n" +
                ")\n" +
                "OPTIONS (UPDATETYPE = 'batch') COMMENT '店铺poi数据'\n" +
                "PARTITION BY HASH KEY(shop_id) PARTITION NUM 250\n" +
                "SUBPARTITION BY LIST KEY (biz_date) \n" +
                "SUBPARTITION OPTIONS (available_partition_num = 1)\n" +
                "TABLEGROUP crm_platform_filter", stmt.toString());

    }




}