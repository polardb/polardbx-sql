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
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 复杂prepare测试用例
 * 测试参数绑定
 */
public class ServerPrepareComplexTest extends CrudBasedLockTestCase {

    private static final String DROP_TABLE = "drop table if exists %s";
    private static final String T1 = "prep_part_order";
    private static final String T2 = "prep_part_orders";
    private static final String T3 = "prep_part_orderss";
    private static final String T4 = "prep_part_ordersss";
    private static final String T5 = "prep_part_contract";

    private static final List<String> TABLES = ImmutableList.of(T1, T2, T3, T4, T5);

    @Before
    public void setUp() throws SQLException {
        destroyTables();
        createTables();
        loadTableData();
    }

    @After
    public void cleanUp() throws SQLException {
        destroyTables();
    }

    private void destroyTables() throws SQLException {
        try (Statement tddlStmt = tddlConnection.createStatement();
            Statement mysqlStmt = mysqlConnection.createStatement()) {
            for (String table : TABLES) {
                tddlStmt.executeUpdate(String.format(DROP_TABLE, table));
                mysqlStmt.executeUpdate(String.format(DROP_TABLE, table));
            }
        }
    }

    private void createTables() throws SQLException {
        String baseSql = "create table %s (\n"
            + "    `id` bigint(20) NOT NULL primary key,\n"
            + "    v1 int(10),\n"
            + "    v2 int(10),\n"
            + "    v3 int(10),\n"
            + "    v4 int(10)\n"
            + ") ";
        String t5Sql = "create table %s (\n"
            + "    `id` bigint(20) NOT NULL primary key,\n"
            + "    v1 int(10),\n"
            + "    v2 int(10)\n"
            + ");";
        try (Statement tddlStmt = tddlConnection.createStatement();
            Statement mysqlStmt = mysqlConnection.createStatement()) {
            for (int i = 0; i < TABLES.size() - 1; i++) {
                tddlStmt.executeUpdate(String.format(baseSql + " dbpartition by hash(`id`) ;", TABLES.get(i)));
                mysqlStmt.executeUpdate(String.format(baseSql, TABLES.get(i)));
            }
            tddlStmt.executeUpdate(String.format(t5Sql, T5));
            mysqlStmt.executeUpdate(String.format(t5Sql, T5));
        }
    }

    private void loadTableData() throws SQLException {
        final int rows = 6;
        try (Statement tddlStmt = tddlConnection.createStatement();
            Statement mysqlStmt = mysqlConnection.createStatement()) {
            for (int i = 0; i < rows; i++) {
                tddlStmt.executeUpdate(String.format("insert into %s values (%d, 1, 2, 6, 4)", T1, i + 1));
                mysqlStmt.executeUpdate(String.format("insert into %s values (%d, 1, 2, 6, 4)", T1, i + 1));
            }
            for (int i = 1; i < TABLES.size() - 1; i++) {
                for (int j = 0; j < rows; j++) {
                    tddlStmt
                        .executeUpdate(String.format("insert into %s values (%d, 1, 2, 3, 4)", TABLES.get(i), j + 1));
                    mysqlStmt
                        .executeUpdate(String.format("insert into %s values (%d, 1, 2, 3, 4)", TABLES.get(i), j + 1));
                }
            }
            for (int i = 0; i < rows; i++) {
                tddlStmt.executeUpdate(String.format("insert into %s values (%d, 1, 1)", T5, i + 1));
                mysqlStmt.executeUpdate(String.format("insert into %s values (%d, 1, 1)", T5, i + 1));
            }
        }

    }

    @Test
    public void complexPrepareTest1() {
        String sql = "SELECT\n"
            + "    a.*,\n"
            + "    b.*,\n"
            + "    c.*,\n"
            + "    d.*,\n"
            + "    (\n"
            + "        SELECT\n"
            + "            MIN(ost.v1)\n"
            + "        FROM\n"
            + "            prep_part_contract ost\n"
            + "        WHERE\n"
            + "            ost.v2 = ?\n"
            + "    ) as packageCode,\n"
            + "    (\n"
            + "        SELECT\n"
            + "            cs.v3\n"
            + "        FROM\n"
            + "            prep_part_orderss cs\n"
            + "        WHERE\n"
            + "            cs.id = d.id\n"
            + "            AND cs.v1 = '0'\n"
            + "            AND cs.v2 IN (\n"
            + "                ?,\n"
            + "                ?,\n"
            + "                ?,\n"
            + "                ?) limit 1\n"
            + "    ) as leafName\n"
            + "FROM\n"
            + "    prep_part_order a\n"
            + "    LEFT JOIN prep_part_orders b ON a.id = b.id\n"
            + "    LEFT JOIN prep_part_orderss c ON b.id = c.id\n"
            + "    LEFT JOIN prep_part_ordersss d ON c.id = d.id\n"
            + "    LEFT JOIN (\n"
            + "        SELECT\n"
            + "            ss.id,\n"
            + "            ss.v2\n"
            + "        FROM\n"
            + "            prep_part_orderss ss\n"
            + "        WHERE\n"
            + "            ss.v2 in ('2', '4')\n"
            + "    ) ss on ss.id = c.id\n"
            + "    and ss.v2 != c.v2\n"
            + "    and c.v2 in ('2', '4')\n"
            + "WHERE\n"
            + "    a.v3 = ?\n"
            + "    and b.v3 = ?";
        List<Object> params = new ArrayList<>();
        params.add(1);
        params.add(1);
        params.add(2);
        params.add(3);
        params.add(4);
        params.add(6);
        params.add(3);
        DataValidator.selectContentSameAssert(sql, params, mysqlConnection, tddlConnection);
    }
}
