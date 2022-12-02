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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class GsiEmojiTest extends DDLBaseNewDBTestCase {
    private static final String TABLE_NAME = "gsi_emoji_table";
    private static final String GSI_NAME = "g_i_emoji_table";

    @Test
    public void testCreateGsiWithEmoji() {
        if (!isMySQL80()) {
            return;
        }

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));

        final String CREATE_TABLE_SQL = "CREATE TABLE `" + TABLE_NAME + "` (\n"
            + "        `id` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `platform_order_no` varchar(32) NOT NULL,\n"
            + "        `goods_id` varchar(32) NOT NULL DEFAULT '0',\n"
            + "        `goods_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n"
            + "        `goods_price` int(11) NOT NULL DEFAULT '0',\n"
            + "        `goods_num` int(11) NOT NULL DEFAULT '0',\n"
            + "        `goods_img` varchar(200) DEFAULT NULL,\n"
            + "        `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n"
            + "        `created_at` datetime NOT NULL,\n"
            + "        `updated_at` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "        `cat1_id` varchar(32) DEFAULT NULL,\n"
            + "        `cat1_name` varchar(200) DEFAULT NULL,\n"
            + "        `combo_sku_id` varchar(32) DEFAULT NULL,\n"
            + "        `sku_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n"
            + "        `sale_user_nick` varchar(255) DEFAULT NULL,\n"
            + "        `pk_id` varchar(32) DEFAULT NULL,\n"
            + "        `gift_num` int(11) DEFAULT NULL,\n"
            + "        `shop_id` varchar(32) DEFAULT NULL,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        KEY `idx_order_no` (`platform_order_no`),\n"
            + "        KEY `idx_goods_id` (`goods_id`),\n"
            + "        KEY `idx_created_at` (`created_at`),\n"
            + "        KEY `idx_updated_at` USING BTREE (`updated_at`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 81857781 DEFAULT CHARSET = utf8 dbpartition by hash(`platform_order_no`)";

        final String CREATE_GSI_SQL = "ALTER TABLE " + TABLE_NAME
            + " ADD GLOBAL INDEX " + GSI_NAME + "(created_at) "
            + " covering (id,goods_name,goods_id) dbpartition  BY YYYYMM(created_at)";

        final String INSERT_SQL1 = "INSERT INTO `" + TABLE_NAME + "` VALUES "
            + "(63407994,'1','1','\uD83C\uDDFA\uD83C\uDDF8美国 500ml*3瓶',1,1,'1','1','2021-01-28 05:12:32','2021-01-28 05:12:32',NULL,NULL,'1','\uD83C\uDDFA\uD83C\uDDF8美国 500ml*3瓶',NULL,'1',NULL,NULL),"
            + "(66484957,'2','2','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',1,1,'1','2','2021-02-26 18:00:09','2021-02-26 18:00:09',NULL,NULL,'2','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',NULL,'2',NULL,NULL),"
            + "(66484961,'3','3','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',1,1,'1','2','2021-02-26 18:00:09','2021-02-26 18:00:09',NULL,NULL,'2','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',NULL,'3',NULL,NULL),"
            + "(66484964,'4','4','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',1,1,'1','2','2021-02-26 18:00:09','2021-02-26 18:00:09',NULL,NULL,'2','拍一发三\uD83C\uDDFA\uD83C\uDDF8美国 500m、',NULL,'4',NULL,NULL),"
            + "(81857734,'5','5','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',2,1,'1','2','2021-04-26 21:27:03','2021-04-26 21:27:03',NULL,NULL,'3','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',NULL,'5',NULL,NULL),"
            + "(81857756,'6','6','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',2,1,'1','2','2021-04-26 21:27:21','2021-04-26 21:27:21',NULL,NULL,'3','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',NULL,'6',NULL,NULL),"
            + "(81857763,'7','7','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',2,5,'1','2','2021-04-26 21:27:27','2021-04-26 21:27:27',NULL,NULL,'3','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',NULL,'7',NULL,NULL),"
            + "(81857764,'8','8','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',2,1,'1','2','2021-04-26 21:27:27','2021-04-26 21:27:27',NULL,NULL,'3','\uD83C\uDDFA\uD83C\uDDF8美国 洗面奶207ml。',NULL,'8',NULL,NULL),"
            + "(64082284,'9','9','【拍一发二】\uD83C\uDDEC\uD83C\uDDE7英国润唇膏 2.5g（牛奶味\uD83E\uDD5B+草莓味\uD83C\uDF53）',3,1,'1','1','2021-02-01 04:13:55','2021-02-01 04:13:55',NULL,NULL,'4','【拍一发二】\uD83C\uDDEC\uD83C\uDDE7英国润唇膏 2.5g（牛奶味\uD83E\uDD5B+草莓味\uD83C\uDF53）',NULL,'9',NULL,NULL)";

        // 1. Create primary table
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);

        // 2. Insert some data with emoji
        JdbcUtil.executeUpdateSuccess(tddlConnection, INSERT_SQL1);

        // 3. Create GSI
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_GSI_SQL);

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));
    }
}
