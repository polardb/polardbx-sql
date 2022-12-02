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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */


public class GsiDmlTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "gsi_dml_table";
    private static final String GSI_NAME = "g_i_dml_table";

    private void gsiIntegrityCheck(String index) {
        final String CHECK_HINT =
            "/*+TDDL: cmd_extra(GSI_CHECK_PARALLELISM=4, GSI_CHECK_BATCH_SIZE=1024, GSI_CHECK_SPEED_LIMITATION=-1)*/";
        final ResultSet rs = JdbcUtil
            .executeQuery(CHECK_HINT + "check global index " + index, tddlConnection);
        List<String> result = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(row.size() - 1))
            .collect(Collectors.toList());
        System.out.println("Checker: " + result.get(result.size() - 1));
        Assert.assertTrue(result.get(result.size() - 1).contains("OK"));
    }

    private final String updateHint;

    @Parameterized.Parameters(name = "{index}:updateHint={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {""}, new Object[] {"/*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=TRUE)*/"});
    }

    public GsiDmlTest(String updateHint) {
        this.updateHint = updateHint;
    }

    @Test
    public void testSameSortKeyDiffShard() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));

        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "        `id` bigint(20) NOT NULL,\n"
                + "        `unit_id` bigint(20) NOT NULL,\n"
                + "        `uid` bigint(20) NOT NULL,\n"
                + "        `avatar` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,\n"
                + "        `nick_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "        `mobile` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "        `is_assess` int(1) DEFAULT \"1\",\n"
                + "        `realname` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "        `credit` decimal(10, 2) DEFAULT \"0.00\",\n"
                + "        `agent_month_income` decimal(20, 0) DEFAULT NULL,\n"
                + "        `point` decimal(10, 2) DEFAULT \"0.00\",\n"
                + "        `member_growth` decimal(10, 2) DEFAULT NULL,\n"
                + "        `agent_growth` decimal(10, 2) DEFAULT NULL,\n"
                + "        `parent_id` bigint(20) DEFAULT NULL,\n"
                + "        `is_agent` int(1) DEFAULT \"2\",\n"
                + "        `is_agent_checked` int(1) DEFAULT \"1\",\n"
                + "        `agent_id` bigint(20) DEFAULT NULL,\n"
                + "        `level_id` bigint(20) DEFAULT NULL,\n"
                + "        `level_update` int(1) DEFAULT \"1\",\n"
                + "        `is_channel` int(1) DEFAULT \"2\",\n"
                + "        `channel_id` bigint(20) DEFAULT NULL,\n"
                + "        `channel_rate` decimal(5, 2) DEFAULT \"0.00\",\n"
                + "        `is_channel2` int(1) DEFAULT \"2\",\n"
                + "        `channel2_id` bigint(20) DEFAULT NULL,\n"
                + "        `group_id` bigint(20) DEFAULT NULL,\n"
                + "        `agent_time` int(11) DEFAULT NULL,\n"
                + "        `agent_level_id` bigint(20) DEFAULT NULL,\n"
                + "        `agent_update` int(1) DEFAULT \"1\",\n"
                + "        `recommended_count` int(10) DEFAULT \"0\",\n"
                + "        `tb_order_count` int(10) DEFAULT \"0\",\n"
                + "        `is_black` int(1) DEFAULT \"2\",\n"
                + "        `remark` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,\n"
                + "        `binder` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "        `is_delete` int(1) DEFAULT \"2\",\n"
                + "        `create_time` int(11) DEFAULT NULL,\n"
                + "        `update_time` int(11) DEFAULT NULL,\n"
                + "        `delete_time` int(11) DEFAULT NULL,\n"
                + "        `terminal_member_id` bigint(20) DEFAULT NULL,\n"
                + "        `source_id` bigint(20) DEFAULT NULL,\n"
                + "        `member_center_id` bigint(20) DEFAULT NULL,\n"
                + "        `source_type` int(1) DEFAULT NULL,\n"
                + "        `source_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "        `config` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,\n"
                + "        PRIMARY KEY USING BTREE (`id`),\n"
                + "        UNIQUE KEY `member_uk_mobile_unitId` (`mobile`, `unit_id`),\n"
                + "        KEY `member_ck_uid` (`uid`),\n"
                + "        KEY `member_ck_unitId_nickName` (`unit_id`, `nick_name`),\n"
                + "        KEY `member_ck_unitId_channelId` (`unit_id`, `channel_id`),\n"
                + "        KEY `member_ck_unitId_parentId` (`unit_id`, `parent_id`),\n"
                + "        GLOBAL INDEX `{1}`(`unit_id`, `nick_name`) COVERING (`id`) DBPARTITION BY HASH(`nick_name`) tbpartition by hash(`nick_name`) tbpartitions 8\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci ROW_FORMAT = DYNAMIC dbpartition by RIGHT_SHIFT(`id`, 22) tbpartition by RIGHT_SHIFT(`id`, 22) tbpartitions 8;",
            TABLE_NAME, GSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Check emoji.
        String sql = MessageFormat.format(
            "INSERT INTO `{0}` ( id,unit_id,uid,source_id,source_type,source_name,nick_name,mobile,is_agent,is_agent_checked,level_id,is_black,create_time,avatar,member_growth,agent_growth ) VALUES( 1387777936966467584,1250056650086289408,0,1299627390452031488,3,\"xxx\",\"董依依ゆい\uD83C\uDF61   \",\"1392684638010531840\",2,2,1250056650124038144,2,1620876867,\"url\",0,0 )",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format(
            "UPDATE `{0}` SET unit_id = 1238056606952984576,nick_name = \"董依依ゆい\uD83C\uDF61\",avatar = \"url2\" WHERE id = 1387777936966467584",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateHint + sql);

        sql = MessageFormat.format(
            "INSERT INTO `{0}` ( id,unit_id,uid,source_id,source_type,source_name,nick_name,mobile,is_agent,is_agent_checked,level_id,is_black,create_time,avatar,member_growth,agent_growth ) VALUES( 1392684638434107392,1250056650086289408,0,1299627390452031488,3,\"xxx\",\"\",\"1392684638010531840\",2,2,1250056650124038144,2,1620876867,\"url\",0,0 )",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format(
            "UPDATE `{0}` SET nick_name = \"  \",avatar = \"url2\" WHERE id = 1392684638434107392",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateHint + sql);

        gsiIntegrityCheck(GSI_NAME);

        // Check same length.
        sql = MessageFormat.format(
            "INSERT INTO {0} ( id,unit_id,uid,source_id,source_type,source_name,mobile,member_growth,agent_growth,is_agent,is_agent_checked,level_id,level_update,is_black,is_delete,create_time,avatar ) VALUES( 1412350481430458369,1301727777132781568,0,1367769895986667520,2,\"A森4号\",\"1412350481430458368\",0,0,2,2,1301727777212473344,1,2,2,1625565569,\"avatar.png\" )",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format(
            "UPDATE {0} SET unit_id = 1301727777132781568,nick_name = \"鬼岛\" WHERE id = 1412350481430458369",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateHint + sql);

        sql = MessageFormat.format(
            "UPDATE {0} SET unit_id = 1301727777132781568,nick_name = \"白给\",avatar = \"url\" WHERE ( ( unit_id = 1301727777132781568 and id = 1412350481430458369 ) )",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateHint + sql);

        gsiIntegrityCheck(GSI_NAME);

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));
    }

    @Test
    public void testRelocateImplicitCast() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));

        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "        `account_id` bigint(20) UNSIGNED NOT NULL,\n"
                + "        `identifier` varchar(100) NOT NULL DEFAULT \"\",\n"
                + "        `unionid` varchar(255) NOT NULL DEFAULT \"\",\n"
                + "        `credential` varchar(255) NOT NULL DEFAULT \"\",\n"
                + "        `created_at` int(11) NOT NULL DEFAULT \"0\",\n"
                + "        `updated_at` int(11) NOT NULL DEFAULT \"0\",\n"
                + "        PRIMARY KEY USING BTREE (`id`),\n"
                + "        KEY `auto_shard_key_account_id` USING BTREE (`account_id`),\n"
                + "        GLOBAL INDEX `{1}`(`identifier`) COVERING (`id`, `account_id`, `credential`) DBPARTITION BY HASH(`identifier`) TBPARTITION BY HASH(`identifier`) TBPARTITIONS 2\n"
                + ") ENGINE = InnoDB AUTO_INCREMENT = 10235175 DEFAULT CHARSET = utf8mb4 dbpartition by hash(`account_id`) tbpartition by hash(`account_id`) tbpartitions 2;\n",
            TABLE_NAME, GSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String sql = MessageFormat.format(
            "insert into `{0}` values (10184801, 4334075, \"18037241637-1116001\", \"\", \"xxx\", 1595819240, 1608783966), "
                + "(10184802, 4334075, \"18037241637-1116001\", \"\", \"xxx\", 1595819240, 1608783966),"
                + "(10184803, 4334075, \"18037241637-1116001\", \"\", \"xxx\", 1595819240, 1608783966);",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format(
            "UPDATE `{0}` SET identifier = 18037241637",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateHint + sql);

        gsiIntegrityCheck(GSI_NAME);

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(GSI_NAME));
    }

}
