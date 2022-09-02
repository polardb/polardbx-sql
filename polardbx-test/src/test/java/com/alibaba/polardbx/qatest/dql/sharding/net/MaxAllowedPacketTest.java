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

package com.alibaba.polardbx.qatest.dql.sharding.net;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.metaInfoCheckSame;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * 测试前端协议对单行超过 16M 的场景的支持情况
 * 前置要求：MySQL 上的 max_allowed_packet 设置大于 16777215
 */

@Ignore("fix by ???")
public class MaxAllowedPacketTest extends ReadBaseTestCase {
    private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public MaxAllowedPacketTest() {
        this.baseOneTableName = "max_allowed_packet_test";
    }

    @Before
    public void before() {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "DROP TABLE IF EXISTS " + baseOneTableName, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "CREATE TABLE `" + baseOneTableName + "` (\n"
            + "  `pk` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `detail` longtext,\n"
            + "  PRIMARY KEY (`pk`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", null);

        final Random r = new Random();
        byte[] unit8m = new byte[8000000];
        for (int i = 0; i < unit8m.length; i++) {
            unit8m[i] = (byte) AB.charAt(r.nextInt(AB.length()));
        }

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            "INSERT INTO `" + baseOneTableName + "`(pk, detail) values(1, ?)",
            ImmutableList.of(new String(unit8m, StandardCharsets.US_ASCII)));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            "UPDATE `" + baseOneTableName + "` SET detail = concat(detail, detail) WHERE pk = 1",
            null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            "UPDATE `" + baseOneTableName + "` SET detail = concat(detail, detail) WHERE pk = 1",
            null);
    }

    /**
     * 能够正常读取超过 16M 的结果行
     */
    @Test
    public void largeStringColumnTest1() {
        selectContentSameAssert("SELECT `detail` FROM `" + baseOneTableName + "` WHERE pk = 1", null,
            mysqlConnection, tddlConnection);
    }

    /**
     * 能够正常读取超过 16M 的字符串, 且结果与 MySQL 一致
     */
    @Test
    public void largeStringColumnTest2() throws SQLException {
        final String sql = "SELECT `detail` FROM `" + baseOneTableName + "` WHERE pk = 1";
        final ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlConnection);
        final ResultSet mysqlRs = JdbcUtil.executeQuery(sql, mysqlConnection);

        assertResultStringSame(tddlRs, mysqlRs);
    }

    /**
     * 能够正常读取超过 16M 的字符串, 且结果与 MySQL 一致
     * <p>
     * MySQL 协议要求，每个包的最大长度为 16777215
     * 第一列长度为 16777209 时，长度位占用 4 字节，总共占用 16777213，包中留下 2 字节空位
     * 第二列长度位 252 时 长度位占用 3 字节，会被分割到两个包内
     * 验证该场景下能够正确分包
     */
    @Test
    public void largeStringColumnTest3() throws SQLException {
        final String sql =
            "SELECT substr(`detail`, 1, 16777209) as c1, substr(`detail`, 1, 252) as c2 FROM `" + baseOneTableName
                + "` WHERE pk = 1";
        final ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlConnection);
        final ResultSet mysqlRs = JdbcUtil.executeQuery(sql, mysqlConnection);

        assertResultStringSame(tddlRs, mysqlRs);
    }

    /**
     * Binary 协议，能够正常读取超过 16M 的结果行
     */
    @Test
    public void largeStringColumnTest4() throws SQLException {
        selectContentSameAssert("SELECT `detail` FROM `" + baseOneTableName + "` WHERE pk = ?",
            ImmutableList.of(1), mysqlConnection, tddlConnection);
    }

    private void assertResultStringSame(ResultSet tddlRs, ResultSet mysqlRs) throws SQLException {
        metaInfoCheckSame(tddlRs, mysqlRs);

        if (!tddlRs.next() || !mysqlRs.next()) {
            // 不允许为空结果集合
            throw new AssertionError("查询的结果集为空，请修改sql语句，保证有结果集");
        }

        final String tddlResult = tddlRs.getString(1);
        final String mysqlResult = mysqlRs.getString(1);

        assertWithMessage("返回结果不一致").that(tddlResult).isEqualTo(mysqlResult);

//        System.out.println("length: " + tddlResult.length());
    }
}
