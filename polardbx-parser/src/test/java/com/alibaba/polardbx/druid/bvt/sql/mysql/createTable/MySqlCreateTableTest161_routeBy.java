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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowRouteStatement;

public class MySqlCreateTableTest161_routeBy extends MysqlTest {

    public void test_1() throws Exception {
        //for ADB
        String sql =
            "ALTER TABLE db_name.table_name ADD ROUTE scattered_id = '2206981672181' shard_assign_count = 8 effective_after_seconds = 20";

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("ALTER TABLE db_name.table_name\n" +
                "\tADD ROUTE SCATTERED_ID = '2206981672181' SHARD_ASSIGN_COUNT = 8 EFFECTIVE_AFTER_SECONDS = 20",
            stmt.toString());
    }

    public void test_2() throws Exception {
        //for ADB
        String sql = "SHOW ROUTE FROM db_name.table_name";

        MysqlShowRouteStatement stmt = (MysqlShowRouteStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("SHOW ROUTE FROM db_name.table_name", stmt.toString());
    }

    public void test_3() throws Exception {
        //for ADB
        String sql =
            "Create Table `tc_biz_order` (\n" +
                " `biz_order_id` bigint COMMENT '业务订单id',\n" +
                " `out_order_id` varchar(32) COMMENT '外部系统订单id',\n" +
                " `buyer_nick` varchar(32) COMMENT '买家昵称',\n" +
                " `seller_nick` varchar(32) COMMENT '卖家昵称',\n" +
                " `buyer_id` bigint COMMENT '买家id',\n" +
                " `seller_id` bigint COMMENT '卖家id',\n" +
                " `auction_id` bigint COMMENT '商品id',\n" +
                " `snap_path` varchar(64) COMMENT '商品快照在文件系统里的相对路径',\n" +
                " `auction_title` varchar(64) COMMENT '商品标题',\n" +
                " `auction_price` bigint COMMENT '商品价格',\n" +
                " `buy_amount` int COMMENT '购买数量',\n" +
                " `biz_type` int COMMENT '业务类型',\n" +
                " `pay_status` tinyint COMMENT '支付状态',\n" +
                " `logistics_status` tinyint COMMENT '物流状态',\n" +
                " `gmt_create` datetime COMMENT '创建时间',\n" +
                " `gmt_modified` datetime COMMENT '修改时间',\n" +
                " `status` tinyint COMMENT '状态',\n" +
                " `sub_biz_type` int COMMENT '子业务类型',\n" +
                " `fail_reason` varchar(256) COMMENT '失败原因',\n" +
                " `buyer_rate_status` tinyint COMMENT '买家评价状态',\n" +
                " `seller_rate_status` tinyint COMMENT '卖家评价状态',\n" +
                " `seller_memo` varchar(256) COMMENT '卖家的给交易的备注',\n" +
                " `buyer_memo` varchar(256) COMMENT '买家给交易的备注',\n" +
                " `seller_flag` int COMMENT '卖家给交易的标志',\n" +
                " `buyer_flag` int COMMENT '买家给交易的备注',\n" +
                " `buyer_message_path` varchar(64) COMMENT '买家购买留言的消息文件路径',\n" +
                " `refund_status` tinyint COMMENT '退款状态',\n" +
                " `attributes` json,\n" +
                " `attributes_cc` int COMMENT '属性更新用的校验码',\n" +
                " `ip` bigint COMMENT '购买ip',\n" +
                " `end_time` datetime COMMENT '交易结束时间',\n" +
                " `pay_time` datetime COMMENT '支付时间',\n" +
                " `auction_pict_url` varchar(100) COMMENT '商品图片标题',\n" +
                " `is_detail` tinyint COMMENT '是否是子订单 1表示子订单',\n" +
                " `is_main` tinyint COMMENT '是否是父订单 1表示父订单',\n" +
                " `point_rate` int COMMENT '积分返点比率',\n" +
                " `parent_id` bigint COMMENT '父订单ID，如果是父订单，为NULL，如果是单件商品的订单，等于biz_order_id',\n" +
                " `adjust_fee` bigint COMMENT '卖家修改价格时对单商品的价格调整',\n" +
                " `discount_fee` bigint COMMENT '单商品的系统折扣，比如折扣券',\n" +
                " `refund_fee` bigint COMMENT '退款金额',\n" +
                " `confirm_paid_fee` bigint COMMENT '已经确认收货的金额',\n" +
                " `cod_status` int COMMENT 'tc_logistics_order表中cod_status字段的冗余',\n" +
                " `trade_tag` int COMMENT '订单tag标志 1点卡软件待充 2话费软件待充',\n" +
                " `shop_id` bigint COMMENT '店铺id',\n" +
                " `sync_version` int COMMENT '更新版本号，用在tddl',\n" +
                " `out_trade_status` tinyint COMMENT '外部订单的交易状态(需要索引，外部订单状态的个性化搜索)',\n" +
                " `options` bigint COMMENT '用位数代表不同的交易来源',\n" +
                " `attribute1` bigint COMMENT '保留字段',\n" +
                " `attribute2` bigint COMMENT '保留字段',\n" +
                " `attribute3` int COMMENT '保留字段',\n" +
                " `attribute4` int COMMENT '保留字段',\n" +
                " `ignore_sold_quantity` tinyint COMMENT '1:排除聚划算类似促销导致的销量，0:表示不排除',\n" +
                " `attribute11` varchar(1000) COMMENT '保留字段',\n" +
                " `from_group` smallint DEFAULT '0' COMMENT '区分b2b订单和淘宝订单',\n" +
                " `display_nick` varchar(32),\n" +
                "  key attributes_idx(`attributes`),\n " +
                "  key biz_type_idx(`biz_type`),\n" +
                "  key buyer_flag_idx(`buyer_flag`),\n" +
                "  key buyer_id_idx(`buyer_id`),\n" +
                "  key buyer_rate_status_idx(`buyer_rate_status`),\n" +
                "  key end_time_idx(`end_time`),\n" +
                "  key from_group_idx(`from_group`),\n" +
                "  key gmt_create_idx(`gmt_create`),\n" +
                "  key gmt_modified_idx(`gmt_modified`),\n" +
                "  key is_detail_idx(`is_detail`),\n" +
                "  key is_main_idx(`is_main`),\n" +
                "  key logistics_status_idx(`logistics_status`),\n" +
                "  key options_idx(`options`),\n" +
                "  key out_order_id_idx(`out_order_id`),\n" +
                "  key out_trade_status_idx(`out_trade_status`),\n" +
                "  key parent_id_idx(`parent_id`),\n" +
                "  key pay_status_idx(`pay_status`),\n" +
                "  key refund_status_idx(`refund_status`),\n" +
                "  key seller_id_idx(`seller_id`),\n" +
                "  key status_idx(`status`),\n" +
                "  key auction_id_idx(`auction_id`), \n" +
                "  key sub_biz_type_idx(sub_biz_type),\n" +
                "  key biz_order_id_idx(`biz_order_id`),\n" +
                "  unique key biz_order_id_uk(`biz_order_id`),\n" +
                "  key mul_col_idx_1(buyer_id, is_main, from_group),\n" +
                " primary key (`seller_id`,`biz_order_id`)\n" +
                ") DISTRIBUTE BY HASH(`seller_id`) ROUTE BY HASH(biz_order_id) EFFECTED BY (gmt_create) ENGINE='RSTORE' COMMENT='交易订单表'";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);

        System.out.println(stmt.getDistributeBy());

        assertEquals("CREATE TABLE `tc_biz_order` (\n" +
            "\t`biz_order_id` bigint COMMENT '业务订单id',\n" +
            "\t`out_order_id` varchar(32) COMMENT '外部系统订单id',\n" +
            "\t`buyer_nick` varchar(32) COMMENT '买家昵称',\n" +
            "\t`seller_nick` varchar(32) COMMENT '卖家昵称',\n" +
            "\t`buyer_id` bigint COMMENT '买家id',\n" +
            "\t`seller_id` bigint COMMENT '卖家id',\n" +
            "\t`auction_id` bigint COMMENT '商品id',\n" +
            "\t`snap_path` varchar(64) COMMENT '商品快照在文件系统里的相对路径',\n" +
            "\t`auction_title` varchar(64) COMMENT '商品标题',\n" +
            "\t`auction_price` bigint COMMENT '商品价格',\n" +
            "\t`buy_amount` int COMMENT '购买数量',\n" +
            "\t`biz_type` int COMMENT '业务类型',\n" +
            "\t`pay_status` tinyint COMMENT '支付状态',\n" +
            "\t`logistics_status` tinyint COMMENT '物流状态',\n" +
            "\t`gmt_create` datetime COMMENT '创建时间',\n" +
            "\t`gmt_modified` datetime COMMENT '修改时间',\n" +
            "\t`status` tinyint COMMENT '状态',\n" +
            "\t`sub_biz_type` int COMMENT '子业务类型',\n" +
            "\t`fail_reason` varchar(256) COMMENT '失败原因',\n" +
            "\t`buyer_rate_status` tinyint COMMENT '买家评价状态',\n" +
            "\t`seller_rate_status` tinyint COMMENT '卖家评价状态',\n" +
            "\t`seller_memo` varchar(256) COMMENT '卖家的给交易的备注',\n" +
            "\t`buyer_memo` varchar(256) COMMENT '买家给交易的备注',\n" +
            "\t`seller_flag` int COMMENT '卖家给交易的标志',\n" +
            "\t`buyer_flag` int COMMENT '买家给交易的备注',\n" +
            "\t`buyer_message_path` varchar(64) COMMENT '买家购买留言的消息文件路径',\n" +
            "\t`refund_status` tinyint COMMENT '退款状态',\n" +
            "\t`attributes` json,\n" +
            "\t`attributes_cc` int COMMENT '属性更新用的校验码',\n" +
            "\t`ip` bigint COMMENT '购买ip',\n" +
            "\t`end_time` datetime COMMENT '交易结束时间',\n" +
            "\t`pay_time` datetime COMMENT '支付时间',\n" +
            "\t`auction_pict_url` varchar(100) COMMENT '商品图片标题',\n" +
            "\t`is_detail` tinyint COMMENT '是否是子订单 1表示子订单',\n" +
            "\t`is_main` tinyint COMMENT '是否是父订单 1表示父订单',\n" +
            "\t`point_rate` int COMMENT '积分返点比率',\n" +
            "\t`parent_id` bigint COMMENT '父订单ID，如果是父订单，为NULL，如果是单件商品的订单，等于biz_order_id',\n" +
            "\t`adjust_fee` bigint COMMENT '卖家修改价格时对单商品的价格调整',\n" +
            "\t`discount_fee` bigint COMMENT '单商品的系统折扣，比如折扣券',\n" +
            "\t`refund_fee` bigint COMMENT '退款金额',\n" +
            "\t`confirm_paid_fee` bigint COMMENT '已经确认收货的金额',\n" +
            "\t`cod_status` int COMMENT 'tc_logistics_order表中cod_status字段的冗余',\n" +
            "\t`trade_tag` int COMMENT '订单tag标志 1点卡软件待充 2话费软件待充',\n" +
            "\t`shop_id` bigint COMMENT '店铺id',\n" +
            "\t`sync_version` int COMMENT '更新版本号，用在tddl',\n" +
            "\t`out_trade_status` tinyint COMMENT '外部订单的交易状态(需要索引，外部订单状态的个性化搜索)',\n" +
            "\t`options` bigint COMMENT '用位数代表不同的交易来源',\n" +
            "\t`attribute1` bigint COMMENT '保留字段',\n" +
            "\t`attribute2` bigint COMMENT '保留字段',\n" +
            "\t`attribute3` int COMMENT '保留字段',\n" +
            "\t`attribute4` int COMMENT '保留字段',\n" +
            "\t`ignore_sold_quantity` tinyint COMMENT '1:排除聚划算类似促销导致的销量，0:表示不排除',\n" +
            "\t`attribute11` varchar(1000) COMMENT '保留字段',\n" +
            "\t`from_group` smallint DEFAULT '0' COMMENT '区分b2b订单和淘宝订单',\n" +
            "\t`display_nick` varchar(32),\n" +
            "\tKEY attributes_idx (`attributes`),\n" +
            "\tKEY biz_type_idx (`biz_type`),\n" +
            "\tKEY buyer_flag_idx (`buyer_flag`),\n" +
            "\tKEY buyer_id_idx (`buyer_id`),\n" +
            "\tKEY buyer_rate_status_idx (`buyer_rate_status`),\n" +
            "\tKEY end_time_idx (`end_time`),\n" +
            "\tKEY from_group_idx (`from_group`),\n" +
            "\tKEY gmt_create_idx (`gmt_create`),\n" +
            "\tKEY gmt_modified_idx (`gmt_modified`),\n" +
            "\tKEY is_detail_idx (`is_detail`),\n" +
            "\tKEY is_main_idx (`is_main`),\n" +
            "\tKEY logistics_status_idx (`logistics_status`),\n" +
            "\tKEY options_idx (`options`),\n" +
            "\tKEY out_order_id_idx (`out_order_id`),\n" +
            "\tKEY out_trade_status_idx (`out_trade_status`),\n" +
            "\tKEY parent_id_idx (`parent_id`),\n" +
            "\tKEY pay_status_idx (`pay_status`),\n" +
            "\tKEY refund_status_idx (`refund_status`),\n" +
            "\tKEY seller_id_idx (`seller_id`),\n" +
            "\tKEY status_idx (`status`),\n" +
            "\tKEY auction_id_idx (`auction_id`),\n" +
            "\tKEY sub_biz_type_idx (sub_biz_type),\n" +
            "\tKEY biz_order_id_idx (`biz_order_id`),\n" +
            "\tUNIQUE KEY biz_order_id_uk (`biz_order_id`),\n" +
            "\tKEY mul_col_idx_1 (buyer_id, is_main, from_group),\n" +
            "\tPRIMARY KEY (`seller_id`, `biz_order_id`)\n" +
            ") ENGINE = 'RSTORE' COMMENT '交易订单表'\n" +
            "DISTRIBUTE BY HASH(`seller_id`)\n" +
            "ROUTE BY (biz_order_id) EFFECTED BY (gmt_create) ", stmt.toString());
    }
}