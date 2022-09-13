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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class CreateGsiCaseSensitiveTest extends AsyncDDLBaseNewDBTestCase {

    private static final String HINT = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/ ";
    private static final String gsi1 = "g_i_sct_1";
    private static final String gsi2 = "g_i_sct_2";
    private static final String gsi3 = "g_i_sct_3";
    private static final String gsi4 = "g_i_sct_4";
    private static final String gsi5 = "g_i_sct_5";
    private static final String gsi6 = "g_i_sct_6";
    private static final String createTableTemplate = "CREATE TABLE `{0}`( "
        + "    `a` bigint(11) NOT NULL, "
        + "    `B` bigint(11), "
        + "    `c` varchar(20), "
        + "    `D` varchar(20), "
        + "    `eE` varchar(20), "
        + "    `Ff` varchar(20), "
        + "    PRIMARY KEY (`a`), "
        + "    UNIQUE GLOBAL INDEX `" + gsi1 + "`(`{1}`) dbpartition by hash(`{1}`), "
        + "    GLOBAL INDEX `" + gsi2
        + "`(`{2}`,`{3}`) dbpartition by hash(`{2}`) tbpartition by hash(`{3}`) tbpartitions 3, "
        + "    GLOBAL INDEX `" + gsi3 + "`(`{4}`) COVERING(`{5}`) dbpartition by hash(`{4}`)";
    private static final String tail = ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`B`) ";
    private static final String gsiDef4 = "UNIQUE GLOBAL INDEX `" + gsi4 + "`(`{0}`) dbpartition by hash(`{0}`)";
    private static final String gsiDef5 =
        "GLOBAL INDEX `" + gsi5 + "`(`{0}`,`{1}`) dbpartition by hash(`{0}`) tbpartition by hash(`{1}`) tbpartitions 3";
    private static final String gsiDef6 =
        "GLOBAL INDEX `" + gsi6 + "`(`{0}`) COVERING(`{1}`) dbpartition by hash(`{0}`)";
    private static final List<Pair<String, String>> createGsiList = new ArrayList<>();
    private static final List<Pair<String, String>> addGsiList = new ArrayList<>();

    static {
        createGsiList.add(Pair.of(
            "CREATE UNIQUE GLOBAL INDEX `" + gsi4 + "` ON `{0}`(`{1}`) dbpartition by hash(`{1}`)", gsiDef4));
        createGsiList.add(Pair.of(
            "CREATE GLOBAL INDEX `" + gsi5
                + "` ON `{0}`(`{1}`,`{2}`) dbpartition by hash(`{1}`) tbpartition by hash(`{2}`) tbpartitions 3",
            gsiDef5));
        createGsiList.add(Pair.of(
            "CREATE GLOBAL INDEX `" + gsi6 + "` ON `{0}`(`{1}`) COVERING(`{2}`) dbpartition by hash(`{1}`)", gsiDef6));

        addGsiList.add(Pair.of(
            "ALTER TABLE `{0}` ADD UNIQUE GLOBAL INDEX `" + gsi4 + "` (`{1}`) dbpartition by hash(`{1}`)", gsiDef4));
        addGsiList.add(Pair.of(
            "ALTER TABLE `{0}` ADD GLOBAL INDEX `" + gsi5
                + "` (`{1}`,`{2}`) dbpartition by hash(`{1}`) tbpartition by hash(`{2}`) tbpartitions 3",
            gsiDef5));
        addGsiList.add(Pair.of(
            "ALTER TABLE `{0}` ADD GLOBAL INDEX `" + gsi6 + "` (`{1}`) COVERING(`{2}`) dbpartition by hash(`{1}`)",
            gsiDef6));
    }

    private final String tableName;
    private final String c1;
    private final String c2;
    private final String c3;
    private final String c4;
    private final String c5;
    private final String createTable;
    private final List<String> createGsiSqlList = new ArrayList<>();
    private final List<String> addGsiSqlList = new ArrayList<>();
    private String createTableCreateGsi;
    private String createTableAddGsi;

    public CreateGsiCaseSensitiveTest(String tableName, String c1, String c2, String c3, String c4, String c5) {
        this.tableName = tableName;
        this.c1 = c1;
        this.c2 = c2;
        this.c3 = c3;
        this.c4 = c4;
        this.c5 = c5;
        final String createTableHead = MessageFormat.format(createTableTemplate, this.tableName, c1, c2, c3, c4, c5);
        this.createTable = createTableHead + tail;
        this.createTableCreateGsi = createTableHead;
        this.createTableAddGsi = createTableHead;

        Pair<String, String> sqlAndDef = createGsiList.get(0);
        createGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c1));
        createTableCreateGsi += MessageFormat.format("," + sqlAndDef.right, c1);

        sqlAndDef = addGsiList.get(0);
        addGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c1));
        createTableAddGsi += MessageFormat.format("," + sqlAndDef.right, c1);

        sqlAndDef = createGsiList.get(1);
        createGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c2, c3));
        createTableCreateGsi += MessageFormat.format("," + sqlAndDef.right, c2, c3);

        sqlAndDef = addGsiList.get(1);
        addGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c2, c3));
        createTableAddGsi += MessageFormat.format("," + sqlAndDef.right, c2, c3);

        sqlAndDef = createGsiList.get(2);
        createGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c4, c5));
        createTableCreateGsi += MessageFormat.format("," + sqlAndDef.right, c4, c5);

        sqlAndDef = addGsiList.get(2);
        addGsiSqlList.add(MessageFormat.format(sqlAndDef.left, this.tableName, c4, c5));
        createTableAddGsi += MessageFormat.format("," + sqlAndDef.right, c4, c5);

        this.createTableCreateGsi += tail;
        this.createTableAddGsi += tail;
    }

    @Parameterized.Parameters(name = "{index}:tableName={0}, c1={1}, c2={2}, c3={3}, c4={4}, c5={5}")
    public static List<String[]> prepareDate() {
        final String tableName = "sct_gsi_primary";

        final List<String[]> params = new ArrayList<>();
        params.add(new String[] {tableName, "a", "b", "c", "ee", "ff"});
        params.add(new String[] {tableName, "A", "B", "C", "EE", "FF"});
        params.add(new String[] {tableName, "A", "b", "C", "Ee", "fF"});

        return params;
    }

    @Before
    public void before() {

        dropTableWithGsi(tableName, ImmutableList.of(gsi1, gsi2, gsi3));

        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + createTable);
    }

    @After
    public void after() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCheckCreateResult() throws SQLException {
        final String actual =
            JdbcUtil.executeQueryAndGetStringResult("SHOW CREATE TABLE " + tableName, tddlConnection, 2);

        final TableChecker tableChecker = TableChecker.buildTableChecker(actual.toLowerCase());

        tableChecker.identicalTableDefinitionTo(createTable.toLowerCase(), true, Litmus.THROW);
    }

    @Test
    public void testCheckCreateGsi() {
        for (String sql : this.createGsiSqlList) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        final String actual =
            JdbcUtil.executeQueryAndGetStringResult("SHOW CREATE TABLE " + tableName, tddlConnection, 2);

        final TableChecker tableChecker = TableChecker.buildTableChecker(actual.toLowerCase());

        tableChecker.identicalTableDefinitionTo(createTableCreateGsi.toLowerCase(), true, Litmus.THROW);
    }

    @Test
    public void testCheckAlterTableAddGsi() {
        for (String sql : this.addGsiSqlList) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        final String actual =
            JdbcUtil.executeQueryAndGetStringResult("SHOW CREATE TABLE " + tableName, tddlConnection, 2);

        final TableChecker tableChecker = TableChecker.buildTableChecker(actual.toLowerCase());

        tableChecker.identicalTableDefinitionTo(createTableAddGsi.toLowerCase(), true, Litmus.THROW);
    }
}
