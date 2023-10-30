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

package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @version 1.0
 */
public class FullTypeTest extends ReadBaseTestCase {

    private static final String PRIMARY_TABLE_NAME = "XPlan_full_type";

    private static final String FULL_TYPE_TABLE =
        ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
            DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        "");

    private static final ImmutableMap<String, List<String>> FULL_TYPE_TEST_INSERTS = GsiConstant
        .buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME);

    private static final String DATA_COLUMN_NO_GEOM =
        "`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`";

    private static final String FULL_INSERT_NO_GEOM = "insert into `" + PRIMARY_TABLE_NAME + "` " +
        "(`id`," + DATA_COLUMN_NO_GEOM + ") values (\n" +
        "null,\n" +
        "1,2,2,2,2,\n" +
        "-1,1,-1,1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-100.003, -100.000,\n" +
        "100.000,100.003,100.003,100.000,100.003,100.003,\n" +
        "'2017-12-12',\n" +
        "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
        "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
        "'01:01:01','01:01:01.1','01:01:01.001','01:01:01.000001',\n" +
        "'2000','2000',\n" +
        "'11','11','11','11',\n" +
        "'11','11','11','11',\n" +
        "'11','11','11','11',\n" +
        "'a','b,a',\n" +
        "'{\"k1\": \"v1\", \"k2\": 10}');";

    @Before
    public void initData() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        // Create customized full type table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);
    }

    @After
    public void cleanup() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
    }

    private int sameResult(Connection tddlConnection, Connection mysqlConnection, String sql) throws Exception {
        int result = 0;

        Pair<String, Exception> failed = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            result = stmt.executeUpdate(sql);
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            failed = Pair.of(sql, e);
        }

        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.executeUpdate(sql);

            if (null != failed) {
                assertWithMessage("DRDS 报错，MySQL 正常, Sql：\n " + failed.left + "\nCause: " + failed.right.getMessage())
                    .fail();
            }
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            if (null != failed) {
                String subError = e.getMessage();
                if (subError.contains(": ")) {
                    subError = subError.substring(subError.indexOf(": ") + 2);
                }
                assertWithMessage("DRDS/MySQL 错误不一致, Sql：\n " + failed.left).that(failed.right.getMessage())
                    .contains(subError);
            } else {
                assertWithMessage("DRDS 正常，MySQL 报错, Sql：" + sql + "\n cause: " + e.getMessage()).fail();
            }
        }
        return result;
    }

    @Test
    public void testDecimalChunk() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        final String tbName = PRIMARY_TABLE_NAME + "_dc";
        final String tb = "CREATE TABLE `" + tbName + "` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c_decimal` decimal(10,2) DEFAULT NULL,\n"
            + "  `c_decimal_pr` decimal(65,10) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT=\"10000000\" ";
        // Create customized full type table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + tbName + "`");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, tb);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + tbName + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, tb + DEFAULT_PARTITIONING_DEFINITION);

        final String sql = "insert into `" + tbName
            + "` values (1, 10.00, 100000000000000.0000000000), (2, 20.00, 20000000000000.0000000000)";
        Assert.assertTrue("must success", sameResult(tddlConnection, mysqlConnection, sql) > 0);
        final String testSql = "select * from `" + tbName + "` limit 10";
        final ResultSet myResult = JdbcUtil.executeQuery(testSql, mysqlConnection);
        final String myStr =
            JdbcUtil.getStringResult(myResult, false).stream().map(l -> String.join(",", l)).collect(
                Collectors.joining(";"));
        final ResultSet xResult = JdbcUtil.executeQuery(testSql, tddlConnection);
        final String xStr =
            JdbcUtil.getStringResult(xResult, false).stream().map(l -> String.join(",", l)).collect(
                Collectors.joining(";"));
        Assert.assertEquals(myStr, xStr);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + tbName + "`");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + tbName + "`");
    }

    @Test
    public void testType() throws Exception {
        if (usingNewPartDb()) {
            return;
        }

        System.out.println("table:\n" + FULL_TYPE_TABLE);
        System.out.println();
        for (Map.Entry<String, List<String>> entry : FULL_TYPE_TEST_INSERTS.entrySet()) {
            for (String sql : entry.getValue()) {
                System.out.println(sql);
            }
        }

        for (Map.Entry<String, List<String>> entry : FULL_TYPE_TEST_INSERTS.entrySet()) {
            for (String sql : entry.getValue()) {
                if (entry.getKey().equals(C_ID)) {
                    continue;
                }
                System.out.println("run sql: " + sql);

                JdbcUtil.executeUpdateSuccess(mysqlConnection, "delete from `" + PRIMARY_TABLE_NAME + "` where 1=1");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from `" + PRIMARY_TABLE_NAME + "` where 1=1");

                if (sameResult(tddlConnection, mysqlConnection, sql) > 0) {
                    try {
                        // ignore when use jdbc protocal
                        if (!useXproto(tddlConnection) && entry.getKey().equalsIgnoreCase("c_bit_64")) {
                            return;
                        }
                        selectContentSameAssert(
                            "select `" + entry.getKey() + "` from `" + PRIMARY_TABLE_NAME + "`", null, mysqlConnection,
                            tddlConnection);
                    } catch (Throwable e) {
                        // System.err.println(e);
                        throw e;
                    }
                } else {
                    System.err.println("ERR: " + sql);
                }
            }
        }
    }

    @Test
    public void allInOneWithoutGeom() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "delete from `" + PRIMARY_TABLE_NAME + "` where 1=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from `" + PRIMARY_TABLE_NAME + "` where 1=1");

        final String sql = FULL_INSERT_NO_GEOM;
        System.out.println("full: " + sql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        selectContentSameAssert("select " + DATA_COLUMN_NO_GEOM + " from `" + PRIMARY_TABLE_NAME + "`", null,
            mysqlConnection, tddlConnection);
    }
}
