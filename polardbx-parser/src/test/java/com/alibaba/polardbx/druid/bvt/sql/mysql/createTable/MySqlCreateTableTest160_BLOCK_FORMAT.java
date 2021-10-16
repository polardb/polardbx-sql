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
public class MySqlCreateTableTest160_BLOCK_FORMAT extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `user_vehicle_profile` (\n"
            + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"
            + "  `user_id` bigint(20) NOT NULL COMMENT '用户id',\n"
            + "  `will_vehicle_type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '意向车型',\n"
            + "  `will_purchase_time` datetime DEFAULT NULL COMMENT '计划购买时间',\n"
            + "  `create_at` datetime DEFAULT NULL COMMENT '创建时间',\n"
            + "  `update_at` datetime DEFAULT NULL COMMENT '更新时间',\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `idx_user_vehicle_profile_user_id_UNIQUE` (`user_id`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=55 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci BLOCK_FORMAT=ENCRYPTED COMMENT='用户汽车意向信息表'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals("CREATE TABLE `user_vehicle_profile` (\n"
            + "\t`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "\t`user_id` bigint(20) NOT NULL COMMENT '用户id',\n"
            + "\t`will_vehicle_type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '意向车型',\n"
            + "\t`will_purchase_time` datetime DEFAULT NULL COMMENT '计划购买时间',\n"
            + "\t`create_at` datetime DEFAULT NULL COMMENT '创建时间',\n"
            + "\t`update_at` datetime DEFAULT NULL COMMENT '更新时间',\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tUNIQUE KEY `idx_user_vehicle_profile_user_id_UNIQUE` (`user_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 55 DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_unicode_ci BLOCK_FORMAT = ENCRYPTED COMMENT '用户汽车意向信息表'", stmt.toString());

    }

    public void test_1() throws Exception {
        String sql = "alter TABLE `user_vehicle_profile` ENGINE=InnoDB AUTO_INCREMENT=55 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci BLOCK_FORMAT=ENCRYPTED COMMENT='用户汽车意向信息表'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals("ALTER TABLE `user_vehicle_profile`\n"
            + "\tENGINE = InnoDB AUTO_INCREMENT = 55 CHARSET = utf8mb4 COLLATE utf8mb4_unicode_ci BLOCK_FORMAT = ENCRYPTED COMMENT = '用户汽车意向信息表'", stmt.toString());

    }

}
