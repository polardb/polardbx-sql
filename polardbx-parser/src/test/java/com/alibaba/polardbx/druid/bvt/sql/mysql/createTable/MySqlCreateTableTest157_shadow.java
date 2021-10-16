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
 * @ClassName MySqlCreateTableTest157_shadow
 * @description
 * @Author zzy
 * @Date 2019/10/8 19:25
 */
public class MySqlCreateTableTest157_shadow extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create shadow table `ar_ranking_001_t` (\n" +
                "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "  `user_id` varchar(32) NOT NULL COMMENT '用户ID',\n" +
                "  `scene_code` varchar(64) NOT NULL COMMENT '业务场景码',\n" +
                "  `record_type` varchar(16) NOT NULL COMMENT '记录类型 分数/等级',\n" +
                "  `record_value` int(11) NOT NULL COMMENT '记录值',\n" +
                "  `record_date` varchar(8) NOT NULL COMMENT '记录日期',\n" +
                "  `gmt_create` datetime NOT NULL COMMENT '创建时间',\n" +
                "  `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `idx_ranking` (`user_id`, `scene_code`, `record_type`, `record_date`) BLOCK_SIZE 16384\n" +
                ") AUTO_INCREMENT = 1000001 DEFAULT CHARSET = utf8mb4;\n";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE SHADOW TABLE `ar_ranking_001_t` (\n" +
                "\t`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "\t`user_id` varchar(32) NOT NULL COMMENT '用户ID',\n" +
                "\t`scene_code` varchar(64) NOT NULL COMMENT '业务场景码',\n" +
                "\t`record_type` varchar(16) NOT NULL COMMENT '记录类型 分数/等级',\n" +
                "\t`record_value` int(11) NOT NULL COMMENT '记录值',\n" +
                "\t`record_date` varchar(8) NOT NULL COMMENT '记录日期',\n" +
                "\t`gmt_create` datetime NOT NULL COMMENT '创建时间',\n" +
                "\t`gmt_modified` datetime NOT NULL COMMENT '修改时间',\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tKEY `idx_ranking` (`user_id`, `scene_code`, `record_type`, `record_date`)\n" +
                ") AUTO_INCREMENT = 1000001 DEFAULT CHARSET = utf8mb4;", stmt.toString());
    }
}