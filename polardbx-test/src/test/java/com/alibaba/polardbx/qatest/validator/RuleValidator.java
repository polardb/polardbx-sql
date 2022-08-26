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

package com.alibaba.polardbx.qatest.validator;

import com.alibaba.polardbx.qatest.util.Assembler;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

public class RuleValidator {

    private static final Log log = LogFactory.getLog(RuleValidator.class);

    public static void assertRuleExist(String tableName, String sql, Connection conn) {
        if (sql.contains("tbpartition") && sql.contains("dbpartition")) {
            assertMultiRuleWithBothDbAndTbExist(tableName, conn);
        } else if (sql.contains("tbpartition")) {
            assertMuliRuleOnlyWithTbExist(tableName, conn);
        } else if (sql.contains("dbpartition")) {
            assertMultiRuleExist(tableName, conn);
        } else {
            assertSingelRuleExist(tableName, conn);
        }
    }

    /**
     * 利用show rule 查询，确定规则不存在
     */
    public static void assertRuleNotExist(String tableName, Connection conn) {
        JdbcUtil.executeQueryFaied(conn,
            "show full rule from " + tableName,
            String.format("doesn't exist", getSimpleTableName(tableName)));
    }

    /**
     * 利用show rule 查询，确定存在一个单表规则
     */
    public static void assertSingelRuleExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNull();
            assertThat(rs.getString("TB_RULES_STR")).isNull();
            assertThat(rs.next()).isFalse();
        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * 利用show rule 查询，确定存在一个单表规则
     */
    public static void assertBroadCastRuleExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNull();
            assertThat(rs.getString("TB_RULES_STR")).isNull();
            assertThat(rs.getInt("BROADCAST")).isEqualTo(1);
            assertThat(rs.next()).isFalse();
        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * 利用show rule 查询，确定存在一个只分库的规则
     */
    public static void assertMultiRuleExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNotNull();
            assertThat(rs.getString("TB_RULES_STR")).isNull();
            assertThat(rs.next()).isFalse();

        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * 利用show rule查询，确定存在一个分库和分表的规则
     */
    public static void assertMultiRuleWithBothDbAndTbExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNotNull();
            assertThat(rs.getString("TB_RULES_STR")).isNotNull();
            assertThat(rs.next()).isFalse();
        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * 利用show rule查询，确定存在一个分库和分表的规则
     */
    public static void assertMuliRuleOnlyWithTbExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNull();
            assertThat(rs.getString("TB_RULES_STR")).isNotNull();
            assertThat(rs.next()).isFalse();
        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * 利用show rule查询，确定存在一个分库和分表的规则
     */
    public static void assertMuliRuleOnlyWithDbExist(String tableName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, "show full rule from " + tableName);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(getSimpleTableName(tableName));
            assertThat(rs.getString("DB_RULES_STR")).isNotNull();
            assertThat(rs.getString("TB_RULES_STR")).isNull();
            assertThat(rs.next()).isFalse();
        } catch (Exception e) {
            assertWithMessage(e.getMessage()).fail();
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public static void executeQuery(Connection conn, String sql, Assembler assembler) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                assembler.assemble(rs);
            }
        } catch (Exception e) {
            log.error("Failed to executeSuccess '" + sql + "'", e);
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stmt);
        }
    }

    private static String getSimpleTableName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.split("\\.")[1];
        }
        return tableName;
    }

}
