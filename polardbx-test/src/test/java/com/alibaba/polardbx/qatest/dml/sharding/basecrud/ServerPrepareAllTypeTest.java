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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;

/**
 * 二进制prepare协议全类型测试
 * 仅在 mysql_connector_v8 版本下测试，避免jdbc驱动的问题
 * 由于v8版本同样存在二进制协议解包问题，因此仅对比PolarX与MySQL在Prepare模式下的回包结果
 */
public class ServerPrepareAllTypeTest extends CrudBasedLockTestCase {

    private static final String TABLE_NAME = "prepare_test_all_type";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s (%s) VALUES(%s)";
    private static final String CREATE_TABLE_SQL;
    private static final String PK_NAME = C_ID;

    static {
        final String CREATE_TEMPLATE = "CREATE TABLE %s (%s %s)";
        final String columnDef = FULL_TYPE_TABLE_COLUMNS.stream().map(
            c -> PK_NAME.equalsIgnoreCase(c) ? PK_COLUMN_DEF_MAP.get(c) : COLUMN_DEF_MAP.get(c)).collect(
            Collectors.joining());
        final String pkDef = PK_DEF_MAP.get(PK_NAME);
        CREATE_TABLE_SQL =
            String.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, pkDef);
    }

    @Before
    public void setUp() {
        destroyTables();
        createTables();
    }

    @After
    public void cleanup() {
        destroyTables();
    }

    private void createTables() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, CREATE_TABLE_SQL);
    }

    private void destroyTables() {
        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
        JdbcUtil.dropTable(mysqlConnection, TABLE_NAME);
    }

    @Test
    public void testAllType() {
        if (JdbcUtil.getDriverMajorVersion(tddlConnection) != 8) {
            // only run tests with mysql-connector-java version higher than v8
            return;
        }
        final ImmutableMap<String, List<String>> COLUMN_VALUES = GsiConstant.buildGsiFullTypeTestValues();
        final List<String> errors = new ArrayList<>();
        for (Map.Entry<String, List<String>> columnAndVals : COLUMN_VALUES.entrySet()) {
            try {
                final String columnName = columnAndVals.getKey();
                if (PK_NAME.equalsIgnoreCase(columnName)) {
                    continue;
                }

                boolean skip = prepareData(columnAndVals.getValue(), PK_NAME, columnName);
                if (skip) {
                    continue;
                }

                final String selectSql = String
                    .format("SELECT %s, %s FROM %s WHERE %s < ?", PK_NAME, columnName, TABLE_NAME, PK_NAME);
                final Map<Integer, String> mysqlPkResultMap = new HashMap<>();
                String expectedError = null;
                try (final Connection mysqlConn = getMysqlConnection();
                    final PreparedStatement stmt = mysqlConn.prepareStatement(selectSql)) {
                    stmt.setInt(1, Integer.MAX_VALUE);
                    final ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        final int index = rs.getInt(PK_NAME);
                        final String c = rs.getString(columnName);
                        mysqlPkResultMap.put(index, c);
                    }
                } catch (Throwable t) {
                    expectedError = t.getMessage();
                }

                try (final Connection conn = getPolardbxConnection();
                    final PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                    stmt.setInt(1, Integer.MAX_VALUE);
                    final ResultSet rs = stmt.executeQuery();
                    int rowCount = 0;
                    while (rs.next()) {
                        final int index = rs.getInt(PK_NAME);
                        final String polarxResult = rs.getString(columnName);
                        final String mysqlResult = mysqlPkResultMap.get(index);
                        Assert.assertEquals("wrong data for column " + columnName + ", id: " + index,
                            mysqlResult,
                            polarxResult);
                        rowCount++;
                    }
                    Assert.assertEquals("wrong row count for column " + columnName, mysqlPkResultMap.size(),  rowCount);
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable t) {
                    // expect the same error as mysql
                    Assert.assertEquals("wrong exception ", expectedError, t.getMessage());
                }
            } catch (Throwable e) {
                errors.add(e.getMessage());
            } finally {
                // clear data after each round
                JdbcUtil.executeUpdateSuccess(tddlConnection, "TRUNCATE " + TABLE_NAME);
                JdbcUtil.executeUpdateSuccess(mysqlConnection, "TRUNCATE " + TABLE_NAME);
            }
        }
        if (!errors.isEmpty()) {
            Assert.fail("Test all type columns failed: \n" + StringUtils.join(errors, ";\n"));
        }
    }

    private boolean prepareData(List<String> values, String pk, String columnName) {
        for (String value : values) {
            final String insertSql = String.format(INSERT_TEMPLATE, TABLE_NAME,
                String.join(",", pk, columnName),
                String.join(",", "null", value));
            try (Statement polarxStmt = tddlConnection.createStatement();
                Statement mysqlStmt = mysqlConnection.createStatement()) {
                polarxStmt.executeUpdate(insertSql);
                mysqlStmt.executeUpdate(insertSql);
            } catch (SQLException e) {
                // If the column does not exist, skip this column.
                if (e.getMessage().contains("Unknown target column")) {
                    return true;
                }
            }
        }
        return false;
    }
}
