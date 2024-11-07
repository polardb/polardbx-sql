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

package com.alibaba.polardbx.qatest.ddl.auto.gsi.group3;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.updateErrorAssert;

public class UpsertTest extends DDLBaseNewDBTestCase {
    private static final String DML_USE_NEW_DUP_CHECKER = "DML_USE_NEW_DUP_CHECKER=TRUE";
    private static final String DML_SKIP_IDENTICAL_ROW_CHECK = "DML_SKIP_IDENTICAL_ROW_CHECK=TRUE";
    private static final String DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK = "DML_SKIP_IDENTICAL_JSON_ROW_CHECK=FALSE";
    private static final String DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE = "DML_CHECK_JSON_BY_STRING_COMPARE=FALSE";

    private boolean useAffectedRows;
    private Connection oldTddl;
    private Connection oldMySql;

    public UpsertTest(boolean useAffectedRows) {
        this.useAffectedRows = useAffectedRows;
    }

    @Parameterized.Parameters(name = "{index}:useAffectedRows={0}")
    public static List<Boolean[]> prepareData() {
        return ImmutableList.of(new Boolean[] {false}, new Boolean[] {true});
    }

    @Before
    public void before() {
        if (useAffectedRows && !useXproto()) {
            useAffectedRows = false;
        }
        if (useAffectedRows) {
            oldTddl = tddlConnection;
            tddlConnection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
            useDb(tddlConnection, tddlDatabase1);
            oldMySql = mysqlConnection;
            mysqlConnection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows();
            useDb(mysqlConnection, mysqlDatabase1);
        }
    }

    @After
    public void after() throws SQLException {
        if (useAffectedRows) {
            tddlConnection.close();
            tddlConnection = oldTddl;
            mysqlConnection.close();
            mysqlConnection = oldMySql;
        }
    }

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * 无 PK 无 UK
     * UPSERT 不支持逻辑执行，直接下发
     */
    @Test
    public void tableNoPkNoUk() {
        final String tableName = "upsert_test_tb_no_pk_no_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint, insert, "select * from " + tableName, Matchers.is(3));
    }

    /**
     * 无 PK 有 UK
     * UPSERT 转 SELECT + 去重 + INSERT + UPDATE
     */
    @Test
    public void tableNoPkWithUk() {
        final String tableName = "upsert_test_tb_no_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id(`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        // equal when first insert
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(topology.size() + 3));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 无 PK 有 UK, UK 列 DEFAULT NULL
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + INSERT
     */
    @Test
    public void tableNoPkWithUk_defaultNull() {
        final String tableName = "upsert_test_tb_no_pk_with_uk_default_null";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) DEFAULT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id(`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c3 = c3 + 1";
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(topology.size() + 3));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 无 PK 有 UK
     * 使用默认值在 VALUES 中补上 UK，VALUES 中有重复
     */
    @Test
    public void tableNoPkWithUk_amendUk() {
        final String tableName = "upsert_test_tb_no_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id(`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        // equal when first insert
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(topology.size() + 1));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(1 + 1));
    }

    /**
     * 无 PK 有 UK
     * 使用默认值在 VALUES 中补上 UK，VALUES 中有重复
     * 由于 UPDATE 新老值相同，跳过 UPDATE
     */
    @Test
    public void tableNoPkWithUk_amendUk2() {
        final String tableName = "upsert_test_tb_no_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id(`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(topology.size()));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select * from " + tableName,
            !useAffectedRows,
            Matchers.is(1));
    }

    /**
     * 无 PK 有 UK, UK 是拆分键
     * 直接下发 UPSERT
     */
    @Test
    public void tableNoPkWithUk_partitionByUk() {
        final String tableName = "upsert_test_tb_no_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id(`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`id`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
                + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint, insert, "select * from " + tableName, Matchers.is(2));
    }

    /**
     * 无 PK 有多个 UK
     * 每个 UK 都包含全部拆分键，直接下发 UPSERT
     */
    @Test
    public void tableNoPkWithMultiUk_partitionByUk() {
        final String tableName = "upsert_test_tb_no_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  UNIQUE KEY u_id_c1(`id`, `c1`),"
            + "  UNIQUE KEY u_id_c2(`c4`, `id`, `c2`),"
            + "  UNIQUE KEY u_id_c3(`c3`, `id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`id`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint, insert, "select * from " + tableName, Matchers.is(2));
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，直接下发 UPSERT
     */
    @Test
    public void tableWithPkNoUk_partitionByPk() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_pk_partition";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint, insert, "select * from " + tableName, Matchers.is(3));
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 UPSERT
     */
    @Test
    public void tableWithPkNoUk_partitionByPk2() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_pk_partition";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint, insert, "select * from " + tableName, Matchers.is(1));
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithUk() {
        final String tableName = "upsert_test_tb_with_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_id(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')on duplicate key update c3 = c3 + 1";
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName,
            !useAffectedRows,
            Matchers.is(topology.size() + 3));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeThriceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName,
            !useAffectedRows,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithUk2() {
        final String tableName = "upsert_test_tb_with_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_id(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')on duplicate key update c3 = c3 + 1";
        // equal when first insert
        executeThriceThenCheckDataAndTraceResult(hint,
            insert,
            "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName,
            true,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUk() {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(1, 3, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c2 = c2 + 1, c5 = values(c5)";
//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
//
//        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
//            tddlConnection);

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8");
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.is(topology.size() + 2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.lessThanOrEqualTo(2 + 2));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUk1() {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(1, 3, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 4, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c2 = values(c2) + 1, c5 = values(c5)";
//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
//
//        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
//            tddlConnection);

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8");
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.is(topology.size() + 2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.lessThanOrEqualTo(2 + 2));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     * 测试变更拆分键
     */
    @Test
    public void tableWithPkWithMultiUk_modifyPartitionKey() throws Exception {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(2, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c1 = c1 + 1, c5 = values(c5)";
//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
//
//        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
//            tddlConnection);

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7");
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnceThenCheckDataAndTraceResultAndRouteCorrectness(
            "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            columnNames,
            tableName,
            Matchers.is(topology.size() + 3));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnceThenCheckDataAndTraceResultAndRouteCorrectness(
            "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            columnNames,
            tableName,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     * 测试变更拆分键
     */
    @Test
    public void tableWithPkWithMultiUk_modifyPartitionKey1() throws Exception {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(3, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 3, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c1 = values(c2) + 1, c5 = values(c5)";
//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
//
//        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
//            tddlConnection);

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7");
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnceThenCheckDataAndTraceResultAndRouteCorrectness(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            columnNames,
            tableName,
            Matchers.is(topology.size() + 2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnceThenCheckDataAndTraceResultAndRouteCorrectness(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            columnNames,
            tableName,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /*
     * 包含 GSI 的测试用例
     */

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 GSI, 主键拆分
     * 唯一键包含全部拆分键，但因为包含GSI，依然检查冲突
     * 同时默忽略不产生效果的 UPDATE , 并不下发 UPDATE 的物理 SQL
     */
    @Test
    public void tableWithPkNoUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 3);
    }

    /**
     * 有 PK 无 UK, 一个 GSI, 主键拆分
     * 唯一键包含全部拆分键，但因为包含GSI，依然检查冲突
     * 同时默忽略不产生效果的 UPDATE , 并不下发 UPDATE 的物理 SQL
     */
    @Test
    public void tableWithPkNoUkWithGsi_partitionByPk2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult("", insert, tableName, gsiName, 3);
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，每张表中都包含全部UK, UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "g_upsert_two_c1";
        final String gsiName2 = "g_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(9);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，每张表中都包含全部UK, UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "g_upsert_two_c1";
        final String gsiName2 = "g_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(9);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有联合 PK 无 UK, 两个 GSI, 主键拆分
     * 唯一键包含全部拆分键，但有 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithCompositedPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "g_upsert_two_c1";
        final String gsiName2 = "g_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`,`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`id`) COVERING(`c4`) PARTITION BY HASH(`id`) PARTITIONS 5\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(8);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有联合 PK 无 UK, 两个 GSI, 主键拆分
     * 唯一键包含全部拆分键，但有 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithCompositedPkNoUkWithMultiGsi_partitionByPk2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "g_upsert_two_c1";
        final String gsiName2 = "g_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`,`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`id`) COVERING(`c4`) PARTITION BY HASH(`id`) PARTITIONS 5\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        // first time, affected rows should equal
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(8);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c2(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        // primary (partition pruning: 1) + gsi(partition pruning: 1) + update(primary + gsi: 2)

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 1 + 1 + 2);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c2(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE, DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert = "insert into " + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, topology.size() + 1 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键包含全部拆分键, 但存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c1(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert = "insert into "
                + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 1 + 1 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键包含全部拆分键, 但存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk22() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c1(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 3);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 由于有 GSI
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 由于存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final String hint =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert = "insert into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 3 + 1 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 由于有 GSI
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 由于存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert = "insert into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 1 + 1 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 由于有 GSI
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 由于存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 3 + 1 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 由于有 GSI
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 由于存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert = "insert into " + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeTwiceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, 3);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 由于有 GSI
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 由于存在 GSI，UPSERT 仍然转换为 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk4() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi4";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` varchar(255) DEFAULT 'd',\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c1_c2_4";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` varchar(255) DEFAULT 'd',\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE CLUSTERED INDEX " + gsiName
            + "(`c1`, `c2`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c6)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(3 + 1);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 有 UK, 一个 GSI
     * 由于有 GSI
     * UK 未包含全部 Partition Key, UPSERT 转 SELECT + UPDATE + INSERT
     * 主表上包含全部 UK，UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY(`c2`,`c4`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_with_uk_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY i_c2_c4(`c2`,`c4`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 12);
    }

    /**
     * 有 PK 有 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 包含全部唯一键，直接下发 UPSERT
     */
    @Test
    public void tableWithPkWithUkWithUgsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_one_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c2_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`, `c1`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 有 PK 有 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 包含全部唯一键，直接下发 UPSERT
     */
    @Test
    public void tableWithPkWithUkWithUgsi2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_one_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_one_c2_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`, `c1`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 由于有 GSI
     * 主表 UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表包含所有UK，UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
            + "on duplicate key update c5 = values(c5)";

        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(3 + 1 + 2 + (1 + 2) * 2 - 1);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 由于有 GSI
     * 主表 UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表包含所有UK，UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
                + "on duplicate key update c5 = values(c5)";

        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(topology.size() + (1 + 2) * 2 - 1);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，
     * 主表包含全部UK，UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表，UPSERT 转 SELECT + DELETE + INSERT，DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_delete_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2_delete_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL:cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(3 + 3 + 1);

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，
     * 主表包含全部UK，UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表，UPSERT 转 SELECT + DELETE + INSERT，DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_delete_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2_delete_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(3 + 3 + 1);

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键
     * 主表 UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表包含全部UK, 但因为是 UPSERT， UPSERT 转 SELECT + UPDATE + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    @CdcIgnore(ignoreReason = "忽略ugsi强行写入，会导致上下游不一致")
    public void tableWithPkWithUkWithUgsi_deleteOnly_usingGsi() {
        final String tableName = "upsert_test_tb_with_pk_with_uk_delete_only_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3_1(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3_delete_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "insert into " + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
                + "on duplicate key update c5 = values(c5)";
        // no hint here, or bad affected rows when replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        // bad affected rows when with hint
        final String hint = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        // when delete only should remove unique on mysql
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "alter table `" + tableName + "` drop index u_c2_c3_1");
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            false);
        checkTraceRowCountIs(7);

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        // 不带hint insert 了 2 次
        Assert.assertThat(allResult.size(), Matchers.is(2));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键
     * 主表 UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表包含全部UK, 但因为是 UPSERT， UPSERT 转 SELECT + UPDATE + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    @CdcIgnore(ignoreReason = "忽略ugsi强行写入，会导致上下游不一致")
    public void tableWithPkWithUkWithUgsi_deleteOnly() {
        final String tableName = "upsert_test_tb_with_pk_with_uk_delete_only_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3_1(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3_delete_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "insert into "
                + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " //
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，replace
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
                + "on duplicate key update c5 = values(c5)";
        // no hint here, or bad affected rows when replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        // bad affected rows when with hint
        final String hint =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        // when delete only should remove unique on mysql
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "alter table `" + tableName + "` drop index u_c2_c3_1");
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(7);

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        // 不带hint insert 了 2 次
        Assert.assertThat(allResult.size(), Matchers.is(2));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 所有UK包含全部拆分键，但有 WRITE_ONLY 阶段的 GSI，
     * 主表包含所有UK, UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表 UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_write_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 所有UK包含全部拆分键，但有 WRITE_ONLY 阶段的 GSI，
     * 主表包含所有UK, UPSERT 转 SELECT + UPDATE + INSERT
     * 索引表 UPSERT 转 SELECT + UPDATE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly2() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_write_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_c2_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeThriceThenCheckGsiDataAndTraceResult(hint, insert, tableName, gsiName, !useAffectedRows, 3 + 3 + 1);
    }

    /**
     * 主表拆分键和gsi拆分键不一样
     * upsert 主表拆分键
     * 主表 UPSERT 转 SELECT + DELETE + INSERT
     * 处于write only 阶段的gsi UPSERT 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly3() throws SQLException {
        final String tableName = "update_test_tb_with_write_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_update_c2_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "insert into " + tableName
                + "(c1, c2, c8) values(4, 5, '2020-06-16 06:49:32'), (2, 3, '2020-06-16 06:49:32'), (3, 4, '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String hint = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String upsertSql = "insert into " + tableName
            + "(c1, c2, c8) values(4, 5, '2020-06-16 06:49:32') on duplicate key update c1 = c1 + 100";
        final String upsertSql2 = "insert into " + tableName
            + "(c1, c2, c8) values(104, 5, '2020-06-16 06:49:32') on duplicate key update c1 = c1 + 100";

        // checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // write only
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + hint + upsertSql, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        org.junit.Assert.assertThat(trace.size(), Matchers.is(1 + 2 + 2));

        // public
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql2, "trace " + upsertSql2, null, true);
        final List<List<String>> trace2 = getTrace(tddlConnection);

        org.junit.Assert.assertThat(trace2.size(), Matchers.is(1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, UPSERT 转 SELECT + UPDATE + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_write_only_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
                + "on duplicate key update c5 = values(c5)";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        checkTraceRowCountIs(3 + 1 + 2 + 5);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, UPSERT 转 SELECT + UPDATE + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_with_uk_write_only_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_c2_c3_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert =
            "insert into "
                + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c2_c3 冲突，replace
                + "on duplicate key update c5 = values(c5)";

        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null, true);
        checkTraceRowCountIs(topology.size() + (2 + 1) * 2 - 1);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE)*/";
        final String upsert = " insert into " + tableName
            + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32') on duplicate key update c5 = values(c5)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, "trace " + hint + upsert, null, true);
        checkTraceRowCountIs(primaryTopology.size() + 1 + 1 + 2);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_SKIP_TRIVIAL_UPDATE=FALSE,DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String upsert =
            "insert into " + tableName
                + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32') on duplicate key update c5 = values(c5)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, "trace " + hint + upsert, null, true);
        checkTraceRowCountIs(topology.size() + 2);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1_usingGsi() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_three_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String upsert =
            "insert into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, "trace " + upsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        // primary (all primary phy table) + gsi(partition pruning: 3 + primary partition pruning: 1) + update(primary + gsi: 8)
        Assert.assertThat(trace.size(), Matchers.is(primaryTopology.size() + 3 + 1 + 8));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * 由于有 GSI
     * UGSI 中未包含主表拆分键， 主表上 UPSERT 转 SELECT + UPDATE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 UPSERT 转 SELECT + UPDATE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1() throws SQLException {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "ug_upsert_three_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_GSI=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String upsert = "insert into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')"
                + "on duplicate key update c5 = values(c5)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, "trace " + hint + upsert, null, true);
        checkTraceRowCountIs(topology.size() + 4 * 2);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 非主键拆分
     * UPSERT 变更 PK
     */
    @Test
    public void tableWithPkNoUk_modifyPk() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_modify_pk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`id`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String insert =
            "insert into " + tableName
                + "(id, c1, c5, c8) values(4, 1, 'a', '2020-06-16 06:49:32'), (5, 2, 'b', '2020-06-16 06:49:32'), (6, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c1 = c1 + 3";

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeTwiceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            "select * from " + tableName,
            Matchers.is(topology.size() + 3));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeTwiceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            "select * from " + tableName,
            Matchers.lessThanOrEqualTo(3 + 3));
    }

    /**
     * 有 PK 无 UK, 非主键拆分
     * UPSERT 变更 PK
     */
    @Test
    public void tableWithPkNoUk_modifyPkToZero() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_modify_pk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`id`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        String insert =
            "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ insert into " + tableName
                + "(id, c1, c5, c8) values(4, 1, 'a', '2020-06-16 06:49:32'), (5, 1, 'b', '2020-06-16 06:49:32'), (6, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c1 = c1 - c1";
        executeErrorAssert(tddlConnection, insert, null,
            "[ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO] Do not support update primary key to null or zero by INSERT ON DUPLICATE KEY UPDATE");

        insert =
            "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE,DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ insert into " + tableName
                + "(id, c1, c5, c8) values(4, 1, 'a', '2020-06-16 06:49:32'), (5, 2, 'b', '2020-06-16 06:49:32'), (6, 3, 'c', '2020-06-16 06:49:32')"
                + "on duplicate key update c1 = c1 - c1, id = id + 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeErrorAssert(tddlConnection, insert, null,
            "[ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO] Do not support update primary key to null or zero by INSERT ON DUPLICATE KEY UPDATE");
    }

    /**
     * 有 PK 有多个 UK
     * UPSERT 变更 UK
     */
    @Test
    public void tableWithPkWithMultiUk_modifyUk() {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ ";
        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(1, 3, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c2 = c2 + 1, c5 = values(c5)";

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8");
        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.is(topology.size() + 2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnceThenCheckDataAndTraceResult(
            hint + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ",
            insert,
            buildSqlCheckData(columnNames, tableName),
            Matchers.lessThanOrEqualTo(2 + 2));
    }

    @Test
    public void tableWithPkNoUk_broadcast() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_broadcast";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable =
            "CREATE TABLE IF NOT EXISTS `" + tableName + "` (a int, b int, k int null,PRIMARY KEY (`a`)) ";
        final String partitionDef = " broadcast";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ ";
        final String insert = "insert into " + tableName
            + "(a,b) values(1+2-2,1) on duplicate key update b=b+20";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final int nodeNum = getNodeNum(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(nodeNum + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void tableWithPkNoUkWithGsi_pushdownUpsert() {
        final String tableName = "upsert_test_tb_with_pk_no_uk_with_gsi_pushdown";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String gsiName = "g_upsert_integer_test";
        final String gsiName2 = "g_upsert_pk";
        final String mysqlCreateTable =
            "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "\t`pk` bigint(11) NOT NULL DEFAULT '1',\n"
                + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
                + "\t`integer_test` int(11) NOT NULL DEFAULT '2',\n"
                + "\t`char_test` char(255) DEFAULT NULL,\n"
                + "\t`tinyint_test` tinyint(4) DEFAULT NULL,\n"
                + "\t`tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
                + "\t`smallint_test` smallint(6) DEFAULT NULL,\n"
                + "\t`mediumint_test` mediumint(9) DEFAULT NULL,\n"
                + "\t`bigint_test` bigint(20) DEFAULT NULL,\n"
                + "\t`double_test` double DEFAULT NULL,\n"
                + "\t`decimal_test` decimal(10, 0) DEFAULT NULL,\n"
                + "\t`date_test` date DEFAULT NULL,\n"
                + "\t`time_test` time DEFAULT NULL,\n"
                + "\t`datetime_test` datetime DEFAULT NULL,\n"
                + "\t`timestamp_test` timestamp NULL DEFAULT NULL,\n"
                + "\t`year_test` year(4) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`, `integer_test`),\n"
                + "\tKEY `auto_shard_key_integer_test` USING BTREE (`integer_test`),\n"
                + "\tINDEX `" + gsiName + "`(`integer_test`),\n"
                + "\tINDEX `" + gsiName2 + "`(`pk`)"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";

        final String createTable =
            "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "\t`pk` bigint(11) NOT NULL DEFAULT '1',\n"
                + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
                + "\t`integer_test` int(11) NOT NULL DEFAULT '2',\n"
                + "\t`char_test` char(255) DEFAULT NULL,\n"
                + "\t`tinyint_test` tinyint(4) DEFAULT NULL,\n"
                + "\t`tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
                + "\t`smallint_test` smallint(6) DEFAULT NULL,\n"
                + "\t`mediumint_test` mediumint(9) DEFAULT NULL,\n"
                + "\t`bigint_test` bigint(20) DEFAULT NULL,\n"
                + "\t`double_test` double DEFAULT NULL,\n"
                + "\t`decimal_test` decimal(10, 0) DEFAULT NULL,\n"
                + "\t`date_test` date DEFAULT NULL,\n"
                + "\t`time_test` time DEFAULT NULL,\n"
                + "\t`datetime_test` datetime DEFAULT NULL,\n"
                + "\t`timestamp_test` timestamp NULL DEFAULT NULL,\n"
                + "\t`year_test` year(4) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`, `integer_test`),\n"
                + "\tKEY `auto_shard_key_integer_test` USING BTREE (`integer_test`),\n"
                + "\tGLOBAL INDEX `" + gsiName
                + "`(`integer_test`) COVERING (`pk`, `double_test`) PARTITION BY HASH(`integer_test`) PARTITIONS 7,\n"
                + "\tGLOBAL INDEX `" + gsiName2
                + "`(`pk`) COVERING (`varchar_test`, `integer_test`) PARTITION BY HASH(`pk`) PARTITIONS 5\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";
        final String partitionDef =
            " partition by hash(`integer_test`) PARTITIONS 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateTable);

        final String insert =
            "insert  into " + tableName
                + "  ( pk , varchar_test,integer_test,char_test,tinyint_test,tinyint_1bit_test,smallint_test,mediumint_test,bigint_test,double_test,decimal_test,date_test,time_test,datetime_test,timestamp_test,year_test)  "
                + "values  ( 16 , 'zhuoxue_yll', 52, 'word23', 43, 0, 78, 66, 45, 21.258, 10, '2014-02-12', '08:02:45', '2012-12-13', '2013-04-05 06:34:12', '2006'), "
                + "( 18 , 'nihaore', 78, 'he343243', 81, 0, 42, 43, 52, 1414.14747, 1000000, '2013-09-02', '08:02:45', '2011-06-22', '2013-03-22 09:17:28', '2018'), "
                + "( 20 , 'safdwe', 62, 'kisfe', 93, 0, 91, 57, 52, 250.4874, 1000000000, '2015-11-23', '12:12:12', '2013-02-05', '2013-02-05 12:27:32', '2005'), "
                + "( 22 , 'feed32feed', 13, 'abdfeed', 18, 0, 74, 85, 85, 21.258, 1000, '2013-02-05', '06:34:12', '2011-06-22', '2013-09-02 14:47:28', '2018')"
                + "ON DUPLICATE KEY UPDATE mediumint_test= double_test  /  49";
        executeTwiceThenCheckDataAndTraceResult("", insert, "select * from " + tableName, Matchers.is(10));
    }

    /**
     * 无 PK 有 UK
     * UPSERT IGNORE 支持下推执行
     */
    @Test
    public void tableWithPkWithUk_upsertIgnore() {
        final String tableName = "upsert_ignore_test_tb_with_pk_with_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_id(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=TRUE)*/ ";
        final String insert =
            "insert ignore into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')on duplicate key update c3 = c3 + 1";
        executeTwiceThenCheckDataAndTraceResult(hint,
            insert,
            "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName,
            Matchers.is(3));
    }

    /**
     * 无 PK 有 UK
     * UPSERT IGNORE 不支持逻辑执行
     */
    @Test
    public void tableWithPkWithUk_upsertIgnoreError() {
        final String tableName = "upsert_ignore_test_tb_with_pk_with_uk_err";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_id(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_PUSH_DUPLICATE_CHECK=FALSE)*/ insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')on duplicate key update c3 = c3 + 1";
        updateErrorAssert(insert, null, tddlConnection, "Do not support insert ignore...on duplicate key update");
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * UPSERT 转 SELECT + UPDATE + INSERT
     * 测试变更拆分键场景下，物理SQL编号是否符合预期
     */
    @Test
    public void tableWithPkWithMultiUk_modifyPartitionKey_testPhySqlId() throws Exception {
        final String tableName = "upsert_test_phy_sql_id";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2(`c1`,`c2`),"
            + "  UNIQUE KEY u_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final String insert = "insert into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, update
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 update, 这行保留
            + "(2, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，update
            + "(1, 2, 4, 'f', '2020-06-16 06:49:32')" // u_c1_c2 冲突，update
            + "on duplicate key update c1 = c1 + 1, c5 = values(c5)";

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            insert,
            "trace " + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ " + insert,
            null,
            true);
        List<List<String>> trace = getTrace(tddlConnection);

        checkPhySqlId(trace);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3));

        final List<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7");
        List<List<Object>> mysqlResult = selectContentSameAssert(buildSqlCheckData(columnNames, tableName),
            null,
            mysqlConnection,
            tddlConnection);

        JdbcUtil.assertRouteCorrectness(hint,
            tableName,
            mysqlResult,
            columnNames,
            ImmutableList.of("c1"),
            tddlConnection);

        // Clear data
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            insert,
            "trace " + "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ " + insert,
            null,
            true);
        trace = getTrace(tddlConnection);

        checkPhySqlId(trace);

        Assert.assertThat(trace.size(), Matchers.lessThanOrEqualTo(3 + 3));

        mysqlResult = selectContentSameAssert(buildSqlCheckData(columnNames, tableName),
            null,
            mysqlConnection,
            tddlConnection);

        JdbcUtil.assertRouteCorrectness(hint,
            tableName,
            mysqlResult,
            columnNames,
            ImmutableList.of("c1"),
            tddlConnection);
    }

    /**
     * 有 PK 无 UK
     * UPSERT 变更 PK
     */
    @Test
    public void tableWithPk() {
        final String tableName = "upsert_test_tb_with_pk_with_multi_uk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`a` int(11) NOT NULL,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "`c` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`a`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`a`) partitions 8";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "insert into " + tableName + "(`a`, `b`, `c`) values (1,2,3)";
        final String upsert =
            "insert into " + tableName + "(`a`, `b`, `c`) values (1,2,3) on duplicate key update `a`=4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);

        selectContentSameAssert("select * from " + tableName + " where a=4", null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 重复执行四次，验证 affected rows 结果符合预期
     * 注意：由于是通过 JDBC 下发，useAffectedRows 默认为 false，也就是 CLIENT_FOUND_ROWS=1 ，
     * 因此返回的是 touched 而非 updated。与此对应的是官方命令行工具仅支持 CLIENT_FOUND_ROWS=0 ，
     * 因此返回的是 updated ，如果更新后取值无变化则返回 0
     */
    @Test
    public void checkAffectedRows() throws SQLException {
        final String tableName = "upsert_test_result_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_upsert_result_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')"
            + "on duplicate key update c5 = 'z'";
        // one
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // two
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // three
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // fore
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * fix https://work.aone.alibaba-inc.com/issue/37662931
     */
    @Test
    public void testUpsertOnShardingKey() {
        String logicalTableName = "checkUpsertOnShardingKey";
        String dropTable = String.format("drop table if exists %s", logicalTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
        String partitionRule = "partition by hash(`c_int_32`) partitions 7";
        String createTableSql = ExecuteTableSelect.getFullTypeTableDef(logicalTableName, partitionRule);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        String upsertSql =
            String.format(
                "insert into %s(c_int_32,id,c_timestamp) values('1010','1010',now()) on duplicate key update c_int_32='1011',id='1011'",
                logicalTableName);
        // first run, insert
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsertSql);
        // second run, update
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsertSql);
    }

    /**
     * 有 PK 无 UK
     * UPSERT 变更 PK
     */
    @Test
    public void tableWithPkMultipleDupValues_1() {
        final String tableName = "upsert_test_tb_with_pk_dup_values";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`a` int(11) NOT NULL,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`a`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`a`) partitions 8";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String upsert =
            "insert into " + tableName + "(`a`, `b`) values (1,2),(1,3),(1,4) on duplicate key update `a`=values(`b`)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 无 UK
     * UPSERT 变更 PK
     */
    @Test
    public void tableWithPkMultipleDupValues_2() {
        final String tableName = "upsert_test_tb_with_pk_dup_values";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`a` int(11) NOT NULL,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`a`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`a`) partitions 8";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "insert into " + tableName + "(`a`, `b`) values (1,2) on duplicate key update `a`=values(`b`)";
        final String upsert =
            "insert into " + tableName + "(`a`, `b`) values (1,3),(1,4) on duplicate key update `a`=values(`b`)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 测试 UPSERT 在回填时的正确性
     */
    @Test
    public void upsertGsiBackfillTest() throws Exception {
        upsertGsiBackfillTestInternal("upsert_gsi_backfill_test_shard_tb", "partition by hash(`b`) partitions 8");
        upsertGsiBackfillTestInternal("upsert_gsi_backfill_test_single_tb", "single");
        upsertGsiBackfillTestInternal("upsert_gsi_backfill_test_broadcast_tb", "broadcast");
    }

    public void upsertGsiBackfillTestInternal(String tableName, String gsiPartitionDef) throws Exception {
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `a` bigint(11) NOT NULL,\n"
            + "  `b` bigint(20) NOT NULL,\n"
            + "  `c` bigint(20) NOT NULL,\n"
            + "  PRIMARY KEY(`a`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`c`) partitions 8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 15; i++) {
            String insert = "insert into " + tableName + "(a,b,c) values(" + i + "," + (i + 1) + "," + (i + 2) + ")";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        }

        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        Callable<Void> backfillTask = () -> {
            Connection connection = null;
            try {
                if (useAffectedRows) {
                    connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                    useDb(connection, tddlDatabase1);
                } else {
                    connection = getPolardbxConnection();
                }
                // Use repartition to check since it can create shard / single / broadcast GSI
                // Rely on GSI checker to find out inconsistency between primary table and GSI
                final String createIndex =
                    "/*+TDDL:CMD_EXTRA(GSI_BACKFILL_BATCH_SIZE=1,GSI_BACKFILL_SPEED_LIMITATION=1,"
                        + "GSI_BACKFILL_SPEED_MIN=1,GSI_BACKFILL_PARALLELISM=4)*/ alter table "
                        + tableName + " " + gsiPartitionDef;
                JdbcUtil.executeUpdateSuccess(connection, createIndex);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            return null;
        };

        Callable<Void> upsertTask = () -> {
            Connection connection = null;
            try {
                if (useAffectedRows) {
                    connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                    useDb(connection, tddlDatabase1);
                } else {
                    connection = getPolardbxConnection();
                }
                // wait to let backfill thread proceed
                Thread.sleep(8 * 1000);
                String upsert = "trace insert into " + tableName + " values(14,15,16) on duplicate key update a=-1";
                JdbcUtil.executeUpdateSuccess(connection, upsert);
                System.out.println(getTrace(connection));
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            return null;
        };

        ArrayList<Future<Void>> results = new ArrayList<>();
        results.add(threadPool.submit(backfillTask));
        results.add(threadPool.submit(upsertTask));

        for (Future<Void> result : results) {
            result.get();
        }
    }

    /**
     * 验证 Replace 在大小写不敏感编码时的正确性
     */
    @Test
    public void checkCaseInsensitive() throws SQLException {
        final String tableName = "upsert_test_case_insensitive";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int(11) primary key,\n"
            + "  `b` varchar(20) unique key\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) partitions 8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "upsert_test_case_insensitive_gsi";
        final String createGsi =
            String.format("create global unique index %s on %s(b) partition by hash(b) PARTITIONS 3", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "insert into " + tableName + " values(1,'QQ')";
        String upsert = "insert into " + tableName + " values(5,'qq') on duplicate key update a=2";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        upsert = "insert into " + tableName + " values(3,'Qq') on duplicate key update a=4";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 测试upsert传递了Uint64参数
     */
    @Test
    public void checkUpsertParamUint64() {
        if (!useXproto()) {
            return;
        }

        final String tableName = "upsert_test_u64_param";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE " + tableName + " (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
            + "\t`integer_test` int(11) NOT NULL DEFAULT '1',\n"
            + "\t`char_test` char(255) DEFAULT NULL,\n"
            + "\t`tinyint_test` tinyint(4) DEFAULT NULL,\n"
            + "\t`tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
            + "\t`smallint_test` smallint(6) DEFAULT NULL,\n"
            + "\t`mediumint_test` mediumint(9) DEFAULT NULL,\n"
            + "\t`bigint_test` bigint(20) DEFAULT NULL,\n"
            + "\t`double_test` double DEFAULT NULL,\n"
            + "\t`decimal_test` decimal(10, 0) DEFAULT NULL,\n"
            + "\t`date_test` date DEFAULT NULL,\n"
            + "\t`time_test` time DEFAULT NULL,\n"
            + "\t`datetime_test` datetime DEFAULT NULL,\n"
            + "\t`timestamp_test` timestamp NULL DEFAULT NULL,\n"
            + "\t`year_test` year(4) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`),\n"
            + "\tUNIQUE KEY `u_upsert_test_u64_param` (`bigint_test`, `integer_test`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        final String partitionDef = " partition by hash(`bigint_test`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "upsert_test_u64_param_gsi";
        final String createGsi =
            String.format(
                "create global unique index %s on %s(`integer_test`, `varchar_test`) partition by hash(`integer_test`) PARTITIONS 3",
                gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String upsert = "insert  into " + tableName
            + "   ( pk , varchar_test,integer_test,char_test,tinyint_test,tinyint_1bit_test,smallint_test,mediumint_test,bigint_test,double_test,decimal_test,date_test,time_test,datetime_test,timestamp_test,year_test)  values  ( 2 , '', 71, '\\'cdefeed\\'', 68, 1, 24, 39, 42, 35.1478, 1000000, '2003-04-05', '15:23:34', '2011-12-23', '2011-06-22 09:12:28', '2008'), ( 4 , 'zhuoxue_yll', 72, '\\'zhuoxue%yll\\'', 71, 0, 56, 14, 10, 2000.23232, 10, '2011-06-22', '11:23:45', '2013-04-05', '2010-02-22 18:35:23', '2017'), ( 6 , 'cdefeed', 78, '\\'hello1234\\'', 5, 0, 76, 44, 54, 800.147, 100000000, '2013-02-05', '15:23:34', '2010-02-22', '2014-02-12 11:23:45', '2012'), ( 8 , 'hellorew', 6, '\\'hello1234\\'', 60, 0, 98, 92, 73, 301.457, 10000, '2013-04-05', '09:17:28', '2012-12-13', '2013-09-02 14:47:28', '2015')ON DUPLICATE KEY UPDATE double_test= smallint_test  &  mediumint_test;";
        String upsert_overflow = "insert  into " + tableName
            + "   ( pk , varchar_test,integer_test,char_test,tinyint_test,tinyint_1bit_test,smallint_test,mediumint_test,bigint_test,double_test,decimal_test,date_test,time_test,datetime_test,timestamp_test,year_test)  values  ( 2 , '', 71, '\\'cdefeed\\'', 68, 1, 24, 39, 42, 35.1478, 1000000, '2003-04-05', '15:23:34', '2011-12-23', '2011-06-22 09:12:28', '2008'), ( 4 , 'zhuoxue_yll', 72, '\\'zhuoxue%yll\\'', 71, 0, 56, 14, 10, 2000.23232, 10, '2011-06-22', '11:23:45', '2013-04-05', '2010-02-22 18:35:23', '2017'), ( 6 , 'cdefeed', 78, '\\'hello1234\\'', 5, 0, 76, 44, 54, 800.147, 100000000, '2013-02-05', '15:23:34', '2010-02-22', '2014-02-12 11:23:45', '2012'), ( 8 , 'hellorew', 6, '\\'hello1234\\'', 60, 0, 98, 92, 73, 301.457, 10000, '2013-04-05', '09:17:28', '2012-12-13', '2013-09-02 14:47:28', '2015')ON DUPLICATE KEY UPDATE double_test= (smallint_test  &  mediumint_test) + 9223372036854775807;";

        // insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // upsert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // upsert with overflow sint64
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert_overflow, upsert_overflow, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void checkHugeBatchUpsertTraceId() throws SQLException {
        final String tableName = "upsert_huge_batch_traceid_test";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` varchar(1024) \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) partitions 8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final int batchSize = 1000;
        String pad = String.join("", Collections.nCopies(1000, "p"));
        StringBuilder sb = new StringBuilder();
        sb.append("insert into " + tableName + " values");
        for (int i = 0; i < batchSize; i++) {
            String value = "(" + i + "," + i + ",'" + pad + "')";
            if (i != batchSize - 1) {
                value += ",";
            }
            sb.append(value);
        }
        sb.append(" on duplicate key update a=values(a)+1001,b=values(b)+1");

        String upsert = sb.toString();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, "trace " + upsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);
        checkPhySqlOrder(trace);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    private static final String SOURCE_TABLE_NAME = "upsert_test_src_tbl";
    private static final String[][] UPSERT_PARAMS = new String[][] {
        new String[] {
            "(id,a,b)", "values (1,2,2),(100,100,100),(101,103,103)", "(id,a,b)",
            "values (1,2,2),(100,100,100),(101,103,103)"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update a=id+2,b=id+2", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=id+2,b=id+2"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update a=values(a),b=values(a)", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=values(a),b=values(a)"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update id=id+10", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update id=id+10"},
        new String[] {
            "(id,a,b)",
            String.format("select * from %s where id=100 ", SOURCE_TABLE_NAME) + "on duplicate key update id=id+10,a=a",
            "(id,a,b)", "values (100,101,101) on duplicate key update id=id+10,a=a"},
        new String[] {
            "(id,a,b)",
            String.format("select * from %s where id>100 order by id ", SOURCE_TABLE_NAME)
                + "on duplicate key update id=id+10,a=b",
            "(id,a,b)", "values (101,102,102),(102,103,103) on duplicate key update id=id+10,a=b"}
    };

    @Test
    public void testLogicalUpsert() throws SQLException {
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=TRUE)*/";

        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", false,
            true, true,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", false, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", false, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", true,
            true, true,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", true, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", true, true, false,
            UPSERT_PARAMS);
    }

    private void testComplexDmlInternal(String hint, String op, String tableName, String partitionDef, boolean withPk,
                                        boolean withUk, boolean withGsi, String[][] params) throws SQLException {
        // Create source table for insert select
        dropTableIfExists(SOURCE_TABLE_NAME);
        String createSourceTableSql =
            String.format("create table if not exists %s (id int primary key, a int, b int)", SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101,101),(101,102,102),(102,103,103)");

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        String primaryDef = withPk ? "primary key" : "";
        String uniqueDef = withUk ? "unique key" : "";
        String createTableSql =
            String.format("create table if not exists %s (id int %s, a int default 1, b int default 0 %s)", tableName,
                primaryDef, uniqueDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        System.out.println("--------------------");
        for (int i = 0; i < params.length; i++) {
            String insert = String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
            String mysqlInsert =
                String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
            System.out.println("mysql: " + mysqlInsert + "\ntddl: " + insert);
            // equal when first insert
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
        if (withGsi) {
            String gsiName1 = tableName + "_gsi_a";
            String gsiName2 = tableName + "_gsi_b";
            String gsiName3 = tableName + "_gsi_ab";
            String gsiName4 = tableName + "_gsi_ba";
            String createGsiSql1 =
                String.format("create global index %s on %s(a) partition by hash(a) PARTITIONS 3", gsiName1, tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(b) partition by hash(b) PARTITIONS 3", gsiName2, tableName);
            String createGsiSql3 =
                String.format("create global index %s on %s(a) covering(id,b) partition by hash(a) PARTITIONS 3",
                    gsiName3,
                    tableName);
            String createGsiSql4 =
                String.format("create global index %s on %s(b) covering(id,a) partition by hash(b) PARTITIONS 3",
                    gsiName4,
                    tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql3);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql4);
            String deleteAll = "delete from " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, true);
            for (int i = 0; i < params.length; i++) {
                String insert =
                    String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
                String mysqlInsert =
                    String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
                System.out.println("mysql: " + mysqlInsert + "\ntddl: " + insert);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null,
                    true);
                selectContentSameAssert("select * from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName3));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName4));
            }
            // delete and try again with hint
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, true);
            for (int i = 0; i < params.length; i++) {
                String insert =
                    String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
                String mysqlInsert =
                    String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
                System.out.println("mysql: " + mysqlInsert + "\ntddl: " + insert);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null,
                    !useAffectedRows);
                selectContentSameAssert("select * from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName3));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName4));
            }
        }
    }

    @Test
    public void testLogicalUpsertUsingIn() throws SQLException {
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_GET_DUP_USING_IN=TRUE,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=TRUE)*/";

        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", false,
            true, true,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", false, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", false, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", true,
            true, true,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", true, true, false,
            UPSERT_PARAMS);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", true, true, false,
            UPSERT_PARAMS);
    }

    private static final String[][] UPSERT_PARAMS_1 = new String[][] {
        new String[] {
            "(id,a,b)", "values (1,2,2),(100,101,101),(101,102,102)", "(id,a,b)",
            "values (1,2,2),(100,101,101),(101,102,102)"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update a=id+2,b=id+2", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=id+2,b=id+2"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update a=values(a),b=values(a)", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=values(a),b=values(a)"},
        new String[] {
            "(id,a,b)", "values (1,5,5),(2,3,3) on duplicate key update id=id+10", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update id=id+10"},
        new String[] {
            "(id,a,b)",
            String.format("select * from %s where id=100 ", SOURCE_TABLE_NAME) + "on duplicate key update id=id+10,a=a",
            "(id,a,b)", "values (100,101,101) on duplicate key update id=id+10,a=a"},
        new String[] {
            "(id,a,b)",
            String.format("select * from %s where id>100 order by id ", SOURCE_TABLE_NAME)
                + "on duplicate key update id=id+10,a=b",
            "(id,a,b)", "values (101,102,102),(102,103,103) on duplicate key update id=id+10,a=b"}
    };

    @Test
    public void testLogicalUpsertWithoutFullTableScan() throws SQLException {
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=FALSE)*/";

        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", false,
            true, true,
            UPSERT_PARAMS_1);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", false, true, false,
            UPSERT_PARAMS_1);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", false, true, false,
            UPSERT_PARAMS_1);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl", " partition by hash(id) PARTITIONS 3", true,
            true, true,
            UPSERT_PARAMS_1);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_brd", " broadcast", true, true, false,
            UPSERT_PARAMS_1);
        testComplexDmlInternal(hint, "insert into", "upsert_test_tbl_single", " single", true, true, false,
            UPSERT_PARAMS_1);
    }

    @Test
    public void testUpsertUGSI() throws SQLException {
        final String tableName = "upsert_ugsi_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);

        String insertSql = "insert into " + tableName + " values (2,3,5),(3,4,-5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String upsertSql = "insert into " + tableName + " values (4,4,-5) on duplicate key update b=11";
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsertSql);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);
        Assert.assertThat(allResult.size(), Matchers.is(2));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testUpsertModifyShardingKey() {
        String tableName = "upsert_test_modify_sk_tbl";
        String createSql =
            String.format("create table %s (a int unsigned primary key) partition by hash(a)", tableName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String upsert = String.format("insert into %s values (1) on duplicate key update a=-1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsert);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertEquals(1, allResult.size());
        JdbcUtil.executeUpdateFailed(tddlConnection, upsert, "");

        resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        allResult = JdbcUtil.getAllResult(resultSet);
        Assert.assertEquals(1, allResult.size());
    }

    @Test
    public void testUpsertSingleShardAfterValue() throws SQLException {
        String tableName = "upsert_test_single_shard_tbl";
        Object[][] param = new Object[][] {
            new Object[] {
                String.format("insert into %s select 1,2+1 on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s select 1,2+1 union select 2,3 on duplicate key update a=values(a),b=4",
                    tableName), String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=3,b=4", tableName),
                String.format("select * from %s where a=3", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s values (1,2),(2,3) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=values(b),b=4", tableName),
                String.format("select * from %s where a=2", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s(b) values (2) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=values(b),b=4", tableName),
                String.format("select * from %s where a=2", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s select * from %s where a=1 on duplicate key update a=values(a),b=4",
                    tableName, tableName), String.format("select * from %s where a=1", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s select a,a from %s where a=1 on duplicate key update a=values(a),b=4",
                    tableName, tableName), String.format("select * from %s where a=1", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=values(b),a=values(a),b=4",
                    tableName), String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=3,a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s set a=1,b=2 on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 4L, true},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=values(a),b=4", tableName),
                String.format("select * from %s where a=1", tableName), 4L, true},
        };

        for (Object[] objects : param) {
            testUpsertSingleShardInternal(tableName, (String) objects[0], (String) objects[1], (Long) objects[2],
                (Boolean) objects[3]);
        }
    }

    @Test
    public void testUpsertSingleShardBeforeValue() throws SQLException {
        String tableName = "upsert_test_single_shard_tbl";

        Object[][] param = new Object[][] {
            new Object[] {
                String.format("insert into %s select 1,2+1 on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s select 1,2+1 union select 2,3 on duplicate key update a=a,b=4",
                    tableName), String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=3,b=4", tableName),
                String.format("select * from %s where a=3", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s values (1,2),(2,3) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 2L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=b,b=4", tableName),
                String.format("select * from %s where a=2", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s(b) values (2) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=b,b=4", tableName),
                String.format("select * from %s where a=2", tableName), 3L, false},
            new Object[] {
                String.format("insert into %s set a=1,b=2 on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 1L, false},
            new Object[] {
                String.format("insert into %s values (1,2) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 4L, true},
            new Object[] {
                String.format("insert into %s(b,a) values (2,1) on duplicate key update a=a,b=4", tableName),
                String.format("select * from %s where a=1", tableName), 4L, true},
        };

        for (Object[] objects : param) {
            testUpsertSingleShardInternal(tableName, (String) objects[0], (String) objects[1], (Long) objects[2],
                (Boolean) objects[3]);
        }
    }

    private void testUpsertSingleShardInternal(String tableName, String upsertSql, String selectSql, Long phySqlCnt,
                                               Boolean withGsi) throws SQLException {
        System.out.println(upsertSql);

        String gsiName = tableName + "_gsi";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createSql = String.format("create table %s (a int primary key default 1, b int)", tableName);
        String partDef = "partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);

        if (withGsi) {
            String gsiSql = String.format("create global index %s on %s(b) partition by hash(b)", gsiName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);
        }

        String insertSql = String.format("insert into %s values (1,2)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + upsertSql, null, true);
        if (withGsi) {
            checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        }

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertEquals(phySqlCnt.longValue(), trace.size());

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // Assert affect rows
        String deleteSql = String.format("delete from %s", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteSql, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, null, true);
    }

    @Test
    public void testUpsertSingleShardMultiSk_fullTableScan() {
        String tableName = "test_upsert_single_shard_pk_tbl";
        String createSql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by hash(a,b) partitions 3";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        final String hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ";

        // all after value, pushdown
        String upsertSql =
            String.format("insert into %s values (1,2,3) on duplicate key update a=values(a),b=values(b),c=c",
                tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(1));

        // all before value, pushdown
        upsertSql = String.format("insert into %s values (1,2,4) on duplicate key update a=a,b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(1));

        // after value and before value, do not pushdown
        upsertSql =
            String.format("insert into %s values (1,2,5) on duplicate key update a=values(a),b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(3));


        // after value with different column, do not pushdown
        upsertSql = String.format("insert into %s values (1,2,6) on duplicate key update a=values(a),b=values(a),c=c",
            tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=1",
            Matchers.is(3 + 2));

        // before value with different column , do not pushdown
        upsertSql = String.format("insert into %s values (1,2,7) on duplicate key update a=a,b=a,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=1",
            Matchers.is(3));

        // part after value, can pushdown
        // a is primary key, if duplicated happens, a.oldValue = a.newValue
        // b is one of partition key, if duplicated happens, b.oldValue and b.newValue are at same partition, because we route upsert using b.newValue
        upsertSql = String.format("insert into %s values (1,3,8) on duplicate key update b=values(b),c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(1));

        // part after value, do not pushdown
        upsertSql = String.format("insert into %s values (1,2,8) on duplicate key update a=values(a),c=c+1", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(3 + 1));

        // part before value, pushdown
        upsertSql = String.format("insert into %s values (1,2,10) on duplicate key update b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(1));
    }

    @Test
    public void testUpsertSingleShardMultiSk() {
        String tableName = "test_upsert_single_shard_pk_tbl";
        String createSql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by hash(a,b) partitions 3";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        final String hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        // all after value, pushdown
        String upsertSql =
            String.format("insert into %s values (1,2,3) on duplicate key update a=values(a),b=values(b),c=c",
                tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(1));

        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 2 | 3 || 1 | 2 | 3 |
         * +-----------++-----------+
         */

        // all before value, pushdown
        upsertSql = String.format("insert into %s values (1,2,4) on duplicate key update a=a,b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(1));

        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 2 | 3 || 1 | 2 | 3 |
         * +-----------++-----------+
         */

        // after value and before value, do not pushdown
        upsertSql =
            String.format("insert into %s values (1,2,5) on duplicate key update a=values(a),b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=2",
            Matchers.is(1));

        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 2 | 3 || 1 | 2 | 3 |
         * +-----------++-----------+
         */

        // after value with different column, do not pushdown
        upsertSql = String.format("insert into %s values (1,2,6) on duplicate key update a=values(a),b=values(a),c=c",
            tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=1",
            Matchers.is(1 + 2));

        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 1 | 3 || 1 | 1 | 3 |
         * +-----------++-----------+
         */

        // before value with different column , do not pushdown
        upsertSql = String.format("insert into %s values (1,2,7) on duplicate key update a=a,b=a,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=1",
            Matchers.is(1));

        // partitioning results of "a=1 and b=1" and "a=1 and b=2" are same;
        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 1 | 3 || 1 | 1 | 3 |
         * +-----------++-----------+
         */

        // part after value, can pushdown
        // a is primary key, if duplicated happens, a.oldValue = a.newValue
        // b is one of partition key, if duplicated happens, b.oldValue and b.newValue are at same partition, because we route upsert using b.newValue
        upsertSql = String.format("insert into %s values (1,3,8) on duplicate key update b=values(b),c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(1));

        // partitioning results of "a=1 and b=1" and "a=1 and b=3" are same;
        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 3 | 3 || 1 | 3 | 3 |
         * +-----------++-----------+
         */

        // part after value, do not pushdown
        upsertSql = String.format("insert into %s values (1,2,8) on duplicate key update a=values(a),c=c+1", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(1 + 1));

        // partitioning results of "a=1 and b=3" and "a=1 and b=2" are same;
        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 3 | 4 || 1 | 3 | 4 |
         * +-----------++-----------+
         */

        // part before value, pushdown
        upsertSql = String.format("insert into %s values (1,2,10) on duplicate key update b=b,c=c", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where a=1 and b=3",
            Matchers.is(1));

        // partitioning results of "a=1 and b=2" and "a=1 and b=3" are same;
        /*
         * +-----------++-----------+
         * | PolarDB-X ||   MySQL   |
         * |-----------||-----------|
         * | a | b | c || a | b | c |
         * |---|---|---||---|---|---|
         * | 1 | 3 | 4 || 1 | 3 | 4 |
         * +-----------++-----------+
         */
    }

    @Test
    public void testUpsertSingleShardMultiSk1() {
        String tableName = "test_upsert_single_shard_pk_tbl1";
        String createSql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by hash(b,c) partitions 2";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);

        String upsertSql = String.format("insert into %s values (1,2,3)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + upsertSql, null, true);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        String hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ";
        upsertSql = String.format("insert into %s values (1,10,11) on duplicate key update b=values(b)", tableName);
        executeOnceThenCheckDataAndTraceResult(hint,
            upsertSql,
            "select * from " + tableName + " where b=10 and c=3",
            Matchers.is(4));

//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + tableName + " where 1=1", null, false);

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = false
        hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";
        upsertSql = String.format("insert into %s values (1,13,4) on duplicate key update b=values(b)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + hint + upsertSql, null, false);
        checkTraceRowCount(Matchers.lessThanOrEqualTo(3));
        selectContentSameAssert("select * from " + tableName + " where b=13 and c=3", null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void testUpsertSingleShardWithGsi() {
        String tableName = "test_upsert_single_shard_gsi_tbl";
        String gsiName = tableName + "_gsi";
        String createSql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by hash(a) partitions 2";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);

        String createGsi = String.format("create global index %s on %s(a) partition by hash(a)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String upsertSql = String.format("insert into %s values (1,2,3)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + upsertSql, null, true);

        upsertSql = String.format("insert into %s values (1,10,11) on duplicate key update b=b,c=values(c)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + upsertSql, null, true);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertEquals(2, trace.size());
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        upsertSql =
            String.format("insert into %s values (1,10,11) on duplicate key update b=values(b),c=values(c)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, "trace " + upsertSql, null, true);

        trace = getTrace(tddlConnection);
        Assert.assertEquals(2, trace.size());
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testUpsertPartFieldChecker_1() throws SQLException {
        final String tableName = "upsert_part_field_1";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b varchar(4), "
                + "global unique index %s(b) partition by hash(b)"
                + ") CHARACTER SET utf8 COLLATE utf8_bin partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            String sql = String.format("insert into %s values (1, 'fdsa')", tableName);
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert into %s values (2, 'fdsafdsafdas') on duplicate key update a=3",
                tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("3"));
            Assert.assertTrue(allResult.get(0).get(1).equals("fdsa"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 'fd')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // space at the end of the string should be ignored
            sql = hint + String.format("insert into %s values (2, 'fd  ') on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("3"));
            Assert.assertTrue(allResult.get(0).get(1).equals("fd"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 'fd')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // case sensitive
            sql = hint + String.format("insert into %s values (2, 'FD') on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(2));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUpsertPartFieldChecker_2() throws SQLException {
        final String tableName = "upsert_part_field_2";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b varchar(4), "
                + "global unique index %s(b) partition by hash(b)"
                + ") CHARACTER SET utf8 COLLATE utf8_general_ci partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            String sql = String.format("insert into %s values (1, 'fd')", tableName);
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // case insensitive
            sql = hint + String.format("insert into %s values (2, 'FD') on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("3"));
            Assert.assertTrue(allResult.get(0).get(1).equals("fd"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUpsertPartFieldChecker_3() throws SQLException {
        final String tableName = "upsert_part_field_3";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b int unsigned, "
                + "global unique index %s(b) partition by hash(b)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            String sql = String.format("insert into %s values (1, 0)", tableName);
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert into %s values (2, -1) on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("3"));
            Assert.assertTrue(allResult.get(0).get(1).equals("0"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 4294967295)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert into %s values (2, -1) on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(2));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 4294967295)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert into %s values (2, 4294967296) on duplicate key update a=3", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("3"));
            Assert.assertTrue(allResult.get(0).get(1).equals("4294967295"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUpsertPartFieldChecker_4() throws SQLException {
        final String tableName = "upsert_part_field_4";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b varchar(4) unique key, "
                + "global index %s(a) partition by hash(a)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            String sql = String.format("insert into %s values (1,'fdsa'),(2,'rew')", tableName);
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert into %s values (3, 0) on duplicate key update a=13", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(3));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert into %s values (4, 0) on duplicate key update a=14", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(3));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert into %s values (5, 123) on duplicate key update a=15", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(4));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUpsertPartFieldChecker_5() throws SQLException {
        final String tableName = "upsert_part_field_5";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b varchar(4) unique key, "
                + "global index %s(a) partition by hash(a)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            String sql = String.format("insert into %s values (1,'fdsa'),(2,'rew')", tableName);
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = hint + String.format("insert into %s values (3, 0),(4, 'dsa') on duplicate key update a=a+1",
                tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(4));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert into %s values (5, 'fdas'),(6, 234) on duplicate key update a=a+1",
                tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(6));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUpsertPartFieldChecker_type1() throws SQLException {
        final String tableName = "upsert_part_field_type_1";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b timestamp(6), "
                + "global unique index %s(b) partition by hash(b)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String sql = String.format("replace %s values (1,'2018-01-01 00:00:01.33333')", tableName);
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint + String.format(
            "insert into %s values (2,'2018-01-01 00:00:01.33333') on duplicate key update a=12,b=b", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("12"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 00:00:01.33333"));

        final String timeZone = JdbcUtil.getTimeZone(tddlConnection);

        JdbcUtil.setTimeZone(tddlConnection, "+9:00");

        sql = hint + String.format(
            "insert into %s values (3,'2018-01-01 01:00:01.33333') on duplicate key update a=13,b=b", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("13"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 01:00:01.33333"));

        // reset timezone
        JdbcUtil.setTimeZone(tddlConnection, timeZone);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testUpsertPartFieldChecker_type2() throws SQLException {
        final String tableName = "upsert_part_field_type_2";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b date, "
                + "global unique index %s(b) partition by hash(b)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String sql = String.format("insert into %s values (1,'2018-01-01')", tableName);
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            hint + String.format("insert into %s values (2,'2018-01-01') on duplicate key update a=12,b=b", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("12"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testUpsertPartFieldChecker_type3() throws SQLException {
        final String tableName = "upsert_part_field_type_3";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b datetime(3), "
                + "global unique index %s(b) partition by hash(b)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String sql = String.format("insert into %s values (1,'2018-01-01 00:00:01.123')", tableName);
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            hint + String.format("insert into %s values (2,'2018-01-01 00:00:01.123') on duplicate key update a=12,b=b",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("12"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 00:00:01.123"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testUpsertWithUnorderedUpdatePart() throws SQLException {
        final String tableName = "upsert_unordered_update_part";
        final String indexName1 = "g_unordered_update_idx1";
        final String indexName2 = "g_unordered_update_idx2";
        final String indexName3 = "g_unordered_update_idx3";
        final String createTableTmpl = "CREATE TABLE %s (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "        `local_date` varchar(10) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `local_time` varchar(6) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `app_id` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `belong_bm` varchar(16) COLLATE utf8mb4_bin NOT NULL DEFAULT '1' ,\n"
            + "        `access_md` varchar(3) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `access_id` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `app_name` varchar(64) COLLATE utf8mb4_bin NOT NULL ,\n"
            + "        `prod_cd` varchar(32) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `sett_md` varchar(4) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `tran_cd` varchar(32) COLLATE utf8mb4_bin NOT NULL ,\n"
            + "        `tran_nm` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `host_date` date NOT NULL ,\n"
            + "        `order_id` varchar(64) COLLATE utf8mb4_bin NOT NULL ,\n"
            + "        `order_st` varchar(10) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `trade_id` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' ,\n"
            + "        `created_at` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            + "        `updated_at` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "        `dept_id` bigint(20) DEFAULT NULL ,\n"
            + "        `del_flag` char(1) COLLATE utf8mb4_bin DEFAULT '0' ,\n"
            + "        `create_id` bigint(20) DEFAULT NULL ,\n"
            + "        `create_by` varchar(64) COLLATE utf8mb4_bin DEFAULT '' ,\n"
            + "        `create_time` varchar(20) COLLATE utf8mb4_bin DEFAULT '',\n"
            + "        `update_id` int(11) DEFAULT NULL ,\n"
            + "        `update_by` varchar(64) COLLATE utf8mb4_bin DEFAULT '' ,\n"
            + "        `update_time` varchar(20) COLLATE utf8mb4_bin DEFAULT '',\n"
            + "        `version` int(255) DEFAULT '1' ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE GLOBAL INDEX %s (`order_id`, `app_id`) COVERING (`id`, `host_date`) PARTITION BY HASH(`order_id`),\n"
            + "        UNIQUE GLOBAL INDEX %s (`id`) COVERING (`host_date`) PARTITION BY HASH(`id`),\n"
            + "        UNIQUE GLOBAL INDEX %s (`trade_id`) COVERING (`id`, `host_date`) PARTITION BY HASH(`trade_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 9850480 DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_bin PARTITION BY HASH(`host_date`)\n";
        String createSql = String.format(createTableTmpl, tableName, indexName1, indexName2, indexName3);

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        final String initDataTmpl =
            "INSERT INTO %s (`id` , `local_date` , `local_time` , `app_id` , `belong_bm` , `access_md` , `access_id` , `app_name`    , `prod_cd` , `sett_md` , `tran_cd` , `tran_nm`       , `host_date` , `order_id` , `order_st`   , `trade_id`              , `created_at`                 , `updated_at`                 , `dept_id`  , `del_flag` , `create_id` , `create_by` , `create_time` , `update_id` , `update_by` , `update_time` , `version`) VALUES "
                + "('9445941'         , '20220924'   , '204306'     , 'QY0003' , '4'         , '001'       , 'QY0003'    , '鏈夐檺鍏徃'    , 'SM102'   , 'T1'      , 'P2013'   , '寰紬鍙锋敮浠�'   , '20220924'  , '1'        , '2000000000' , 'OCG010924204305659713' , '2022-09-24 20:43:06.051757' , '2022-09-24 20:53:44.696521' , '20000088' , '0'        , null        , ''          , ''            , null        , ''          , ''            , '1')";

        String sql = String.format(initDataTmpl, tableName);
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint + String.format(
            "INSERT INTO %s (`id` , `local_date` , `local_time` , `app_id` , `belong_bm` , `access_md` , `access_id` , `app_name`                          , `prod_cd` , `sett_md` , `tran_cd` , `tran_nm`             , `host_date` , `order_id`                  , `order_st`   , `trade_id`              , `created_at`                 , `updated_at`                 , `dept_id`  , `del_flag` , `create_id` , `create_by` , `create_time` , `update_id` , `update_by` , `update_time` , `version`)\n"
                + "VALUES (\"9445941\"                      , \"20220924\"   , \"204306\"     , \"QY0003\" , \"4\"         , \"001\"       , \"QY0003\"    , \"鏉\uE15E窞浼楁嫇缃戦�氱\uE756鎶�鏈夐檺鍏\uE100徃\" , \"SM102\"   , \"T1\"      , \"P2013\"   , \"寰\uE1BB俊鍏\uE0FF紬鍙锋敮浠�\" , \"20220924\"  , \"20291924204305824818P2013\" , \"2000000000\" , \"OCG010022004305659713\" , \"2022-09-24 20:43:06.051757\" , \"2022-09-24 20:53:44.696521\" , \"20000088\" , \"0\"        , null        , \"\"          , \"\"            , null        , \"\"          , \"\"            , \"1\")\n"
                + "ON DUPLICATE KEY UPDATE `order_id`=VALUES(`order_id`) , `app_id`=VALUES(`app_id`) , `access_id`=VALUES(`access_id`) , `created_at`=VALUES(`created_at`) , `update_id`=VALUES(`update_id`) , `del_flag`=VALUES(`del_flag`) , `id`=VALUES(`id`) , `host_date`=VALUES(`host_date`)     , `create_id`=VALUES(`create_id`) , `version`=VALUES(`version`) , `access_md`=VALUES(`access_md`) , `tran_cd`=VALUES(`tran_cd`) , `belong_bm`=VALUES(`belong_bm`) , `update_time`=VALUES(`update_time`) , `prod_cd`=VALUES(`prod_cd`) , `local_date`=VALUES(`local_date`) , `app_name`=VALUES(`app_name`) , `update_by`=VALUES(`update_by`) , `trade_id`=VALUES(`trade_id`) , `updated_at`=VALUES(`updated_at`) , `dept_id`=VALUES(`dept_id`) , `local_time`=VALUES(`local_time`) , `create_time`=VALUES(`create_time`) , `order_st`=VALUES(`order_st`) , `create_by`=VALUES(`create_by`);\n",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select order_id from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("20291924204305824818P2013"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName2));
    }

    @Test
    public void testUpsertWithDupColumns() {
        String tableName = "test_upsert_with_dup_col_tbl";
        String createSql = String.format("create table %s (id int primary key, a int, b int)", tableName);
        String partDef = "partition by hash(id)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);

        String insert =
            String.format("insert into %s values (1,2,2) on duplicate key update a=1,a=a+10,b=a,id=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testUpsertValueColumnOrder() {
        String tableName = "test_upsert_value_column_order";
        String createSql =
            String.format("create table %s (a int primary key, b int default 2, c int default 4)", tableName);
        String partDef = "partition by hash(c)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);

        String insert = String.format("insert into %s(a,b,c) values (b,b+1,b+2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        ResultSet rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));
        Assert.assertTrue(objects.get(0).get(2).toString().equals("4"));

        String delete = "delete from " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, delete);

        String hint = buildCmdExtra("DML_REF_PRIOR_COL_IN_VALUE=TRUE");
        insert = String.format("insert into %s(b,a,c) values (b+1,b+2,3)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, hint + insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert =
            String.format("insert into %s(b,a,c) values (3,5,3) on duplicate key update a=a+1,b=a+1,b=b+1,a=b+1,c=4",
                tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, hint + insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testUpsertAutoUpdateShardingKey() throws SQLException {
        String tableName = "upsert_auto_update_shard";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String create = String.format(
            "create table %s (a int primary key, b datetime default '2022-10-10 10:10:10' on update current_timestamp()) ",
            tableName);
        String partDef = "partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String insert = String.format("insert into %s(a) values (1) on duplicate key update a=a+1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testUpsertJson() {
        final String tableName = "replace_json_tbl";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) partition by hash(b)) partition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert =
            String.format("insert into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        insert = String.format(
            "insert into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}') on duplicate key update a=1,b=2,c='{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}'",
            tableName);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + insert, "");

        hint = buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DML_SKIP_IDENTICAL_ROW_CHECK);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + insert);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    @Test
    public void testUpsertUnpushableFunc() {
        final String tableName = "upsert_unpush_func_tbl";
        String[] partDefs = new String[] {"single", "broadcast", "partition by hash(a)"};

        for (String partDef : partDefs) {
            dropTableIfExists(tableName);
            String create = String.format("create table %s (a int primary key, b text) " + partDef, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, create);

            String insert =
                String.format("insert into %s values (1,2) on duplicate key update b=current_user()", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

            String select =
                String.format("/*+TDDL:enable_mpp=false*/select * from %s where b != current_user()", tableName);
            ResultSet resultSet = JdbcUtil.executeQuery(select, tddlConnection);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertEquals(0, allResult.size());
        }
    }

    @Test
    public void testUpsertJson1() {
        final String tableName = "replace_json_tbl1";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) partition by hash(b)) partition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert =
            String.format("insert into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        insert = String.format(
            "insert into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}') on duplicate key update a=1,b=2,c='{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}'",
            tableName);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + insert, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    @Test
    public void testUpsertGsiStatus() {
        final String tableName = "upsert_status_tbl";
        final String gsiName1 = tableName + "_gsi1";
        final String gsiName2 = tableName + "_gsi2";

        String[] status = new String[] {"DELETE_ONLY", "WRITE_ONLY", "PUBLIC"};

        for (String s : status) {
            dropTableIfExists(tableName);

            String create =
                String.format("create table %s (a int primary key, b int, c int) partition by hash(a)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, create);

            String hint = String.format("/*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=%s)*/", s);
            String createGsi =
                String.format("create global index %s on %s(b) partition by hash(b)", gsiName1, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + createGsi);

            createGsi = String.format("create global index %s on %s(c) partition by hash(c)", gsiName2, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + createGsi);

            String insert = String.format("insert into %s values (1,2,3) on duplicate key update c=10", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
            Assert.assertTrue(allResult.get(0).get(1).equals("2"));
            Assert.assertTrue(allResult.get(0).get(2).equals("10"));
        }
    }

    @Test
    public void testBinaryFunc1() {
        String tableName = "test_binary_func_tbl1";
        String create = String.format("create table %s (a int primary key, b varbinary(16) unique)", tableName);
        String partDef = "partition by key(b)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234"};

        for (int i = 0; i < binaryValues.length; i++) {
            String insert = String.format("insert into %s values (%d,%s)", tableName, i, binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String insert =
                String.format("insert into %s values (%d,%s) on duplicate key update b=%s", tableName, i + 10,
                    binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testBinaryFunc2() {
        String tableName = "test_binary_func_tbl2";
        String create = String.format("create table %s (a int primary key, b varbinary(16) unique)", tableName);
        String partDef = "partition by key(a)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234"};

        for (int i = 0; i < binaryValues.length; i++) {
            String insert = String.format("insert into %s values (%d,%s)", tableName, i, binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }

        // DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        final String hint = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ";
        for (int i = 0; i < binaryValues.length; i++) {
            String insert =
                String.format("insert into %s values (%d,%s) on duplicate key update a=a+10, b=%s", tableName, i + 10,
                    binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testBinaryFunc3() {
        String tableName = "test_binary_func_tbl3";
        String create =
            String.format("create table %s (a int primary key auto_increment, b varbinary(16))", tableName);
        String partDef = "partition by key(a)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234"};

        for (int i = 0; i < binaryValues.length; i++) {
            String insert = String.format("insert into %s(b) values (%s)", tableName, binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234", "b"};
        for (int i = 0; i < binaryValues.length; i++) {
            String insert = String.format("insert into %s(b) select %s from %s", tableName, binaryValues[i], tableName);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testMultipleSKChecker1() {
        String tableName = "test_multi_sk_checker_1";
        String gsiName = tableName + "_gsi";
        String create =
            String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by key(a,b) partitions 3";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String createGsi = String.format("create global index %s on %s(a,c) partition by key(a,c)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert =
            String.format("insert into %s values (1,3,6) on duplicate key update a=values(a),c=values(c)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert =
            String.format("insert into %s values (1,3,7) on duplicate key update a=values(a),c=values(c)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);

        checkTraceRowCountIs(6);
    }

    @Test
    public void testMultipleSKChecker2() {
        String tableName = "test_multi_sk_checker_2";
        String gsiName = tableName + "_gsi";
        String create =
            String.format("create table %s (a int primary key, b int, c int)", tableName);
        String partDef = "partition by key(a,b) partitions 3";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String createGsi = String.format("create global index %s on %s(a,c) partition by key(a,c)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert =
            String.format("insert into %s values (1,3,6) on duplicate key update a=values(a),c=values(c)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert =
            String.format("insert into %s values ('1','3','7') on duplicate key update a=values(a),c=values(c)",
                tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);

        checkTraceRowCountIs(6);
    }

    @Test
    public void testUpsertWithUgsiAndJson() throws SQLException {
        final String tableName = "test_tb_update_with_json";
        dropTableIfExists(tableName);

        final String gsiName = tableName + "_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  `c3` bigint(20) DEFAULT NULL ,\n"
            + "  `c4` json DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`), \n"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c1`) covering(`c2`) PARTITION BY HASH(`c1`) PARTITIONS 3, \n"
            + "  UNIQUE INDEX l1 on g1(`c2`) "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c3`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String sql = String.format("insert into %s values (1,1,1,1,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values (1,2,3,4,'{\"a\":\"b\"}') on duplicate key update pk=values(pk),c1=values(c1),c2=values(c2),c3=values(c3),c4=values(c4)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertEquals("2", allResult.get(0).get(1).toString());
        Assert.assertEquals("3", allResult.get(0).get(2).toString());
        Assert.assertEquals("4", allResult.get(0).get(3).toString());
        Assert.assertEquals("{\"a\": \"b\"}", allResult.get(0).get(4).toString());
    }
}
