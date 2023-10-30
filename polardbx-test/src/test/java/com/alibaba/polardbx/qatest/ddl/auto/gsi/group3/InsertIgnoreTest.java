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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.core.Is.is;

public class InsertIgnoreTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final String DISABLE_GET_DUP_USING_GSI = "DML_GET_DUP_USING_GSI=FALSE";
    private static final String DISABLE_RETURNING = "DML_USE_RETURNING=FALSE";
    private static final String DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK = "DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE";
    private static final String DML_EXECUTION_STRATEGY_LOGICAL = "DML_EXECUTION_STRATEGY=LOGICAL";
    private static final String DML_WRITE_ONLY = "GSI_DEBUG=\"GsiStatus2\"";
    private static final String DML_USE_NEW_DUP_CHECKER = "DML_USE_NEW_DUP_CHECKER=TRUE";
    private boolean supportReturning = false;

    private boolean useAffectedRows;
    private Connection oldTddl;
    private Connection oldMySql;

    public InsertIgnoreTest(boolean useAffectedRows) {
        this.useAffectedRows = useAffectedRows;
    }

    @Parameterized.Parameters(name = "{index}:useAffectedRows={0}")
    public static List<Boolean[]> prepareData() {
        return ImmutableList.of(new Boolean[] {false}, new Boolean[] {true});
    }

    @Before
    public void before() {
        this.supportReturning = useXproto()
            && Optional.ofNullable(getStorageProperties(tddlConnection).get("supportsReturning"))
            .map(Boolean::parseBoolean).orElse(false);
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

    /**
     * 无 PK 无 UK
     * INSERT IGNORE 转 INSERT 直接下发
     */
    @Test
    public void tableNoPkNoUk() {
        final String tableName = "test_tb_no_pk_no_uk";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableNoPkWithUk() {
        final String tableName = "test_tb_no_pk_with_uk";
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

        final String hint = buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL, DISABLE_RETURNING);
        final String insert = hint + " insert ignore into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size()));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableNoPkWithUk_returning() {
        if (!supportReturning) {
            return;
        }
        final String tableName = "test_tb_no_pk_with_uk_returning";
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

        final String hint = buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL);
        final String insert = hint + " insert ignore into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK, UK 列 DEFAULT NULL
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableNoPkWithUk_defaultNull() {
        final String tableName = "test_tb_no_pk_with_uk_default_null";
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

        final String insert = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size() + 3));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK
     * 使用默认值在 VALUES 中补上 UK，VALUES 中有重复
     */
    @Test
    public void tableNoPkWithUk_amendUk() {
        final String tableName = "test_tb_no_pk_with_uk";
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

        final String insert = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ insert ignore into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size()));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK, UK 是拆分键
     * 直接下发 INSERT IGNORE
     */
    @Test
    public void tableNoPkWithUk_partitionByUk() {
        final String tableName = "test_tb_no_pk_with_uk";
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

        final String insert = "insert ignore into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(2));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有多个 UK
     * 每个 UK 都包含全部拆分键，直接下发 INSERT IGNORE
     */
    @Test
    public void tableNoPkWithMultiUk_partitionByUk() {
        final String tableName = "test_tb_no_pk_with_multi_uk";
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

        final String insert = "insert ignore into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(2));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithPkNoUk_partitionByPk() {
        final String tableName = "test_tb_with_pk_no_uk_pk_partition";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithPkNoUk_partitionByPk2() {
        final String tableName = "test_tb_with_pk_no_uk_pk_partition";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithUk() {
        final String tableName = "test_tb_with_pk_with_uk";
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

        final String hint =
            buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL, DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK, DISABLE_RETURNING);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size() + 1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * LOCAL UK 有 NULL，走 Returning
     */
    @Test
    public void tableWithPkWithUk_returning() {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_with_uk_returning";
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
        final String partitionDef = " partition by hash(`c2`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL, DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c2, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (null, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithUk2() {
        final String tableName = "test_tb_with_pk_with_uk";
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

        final String hint = buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL, DISABLE_RETURNING);
        final String insert = hint + " insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3 + 1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * LOCAL UK 有 NULL，走 Returning
     */
    @Test
    public void tableWithPkWithUk2_returning() {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_with_uk_returning";
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
        final String partitionDef = " partition by hash(`c2`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String hint = buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL);
        final String insert = hint + " insert ignore into " + tableName
            + "(c1, c2, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (null, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithMultiUk() {
        final String tableName = "test_tb_with_pk_with_multi_uk";
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

        final String insert = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ insert ignore into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, ignore
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 冲突, ignore
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，ignore
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size() + 1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /*
     * 包含 GSI 的测试用例
     */

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_i_c2";
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

        final String hint = buildCmdExtra(DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK, DISABLE_RETURNING);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // Primary(PK is SK, partition pruning: 3)
        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     * returning 优化
     */
    @Test
    public void tableWithPkNoUkWithGsi_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_no_uk_one_gsi_returning";
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

        final String gsiName = "g_i_c2_returning";
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

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/insert ignore into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi2() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_i_c2";
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

        final String hint = buildCmdExtra(DISABLE_RETURNING);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi2_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_i_c2_returning2";
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

        final String hint = buildCmdExtra();
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI, 主键拆分
     * 唯一键包含全部拆分键，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithPkNoUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_i_c2";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), is(5));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_two_c1";
        final String gsiName2 = "g_two_c2";
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

        final String hint = buildCmdExtra(DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK, DISABLE_RETURNING);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // Primary(partition pruning: 3)
        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }
        final String tableName = "test_tb_with_pk_no_uk_two_gsi_returning";
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

        final String gsiName1 = "g_two_c1_returning";
        final String gsiName2 = "g_two_c2_returning";
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

        final String hint = buildCmdExtra(DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK);
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk2() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_two_c1";
        final String gsiName2 = "g_two_c2";
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

        final String hint = buildCmdExtra(DISABLE_RETURNING);
        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        // equal when first insert
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            !useAffectedRows);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk2_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }
        final String tableName = "test_tb_with_pk_no_uk_two_gsi_returning";
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

        final String gsiName1 = "g_two_c1_returning";
        final String gsiName2 = "g_two_c2_returning";
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

        final String hint = buildCmdExtra();
        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有联合 PK 无 UK, 两个 GSI, 主键拆分
     * 唯一键包含全部拆分键，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithCompositedPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_two_c1";
        final String gsiName2 = "g_two_c2";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi";
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
            + "  UNIQUE KEY u_c1(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "ug_one_c2";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String insert = hint + "insert ignore into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size()));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi";
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
            + "  UNIQUE KEY u_c1(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName1 = "ug_one_c2";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        // primary (partition pruning: 1)  +  gsi(partition pruning: 1)
        Assert.assertThat(trace.size(), is(1 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk2() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi";
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

        final String gsiName1 = "ug_one_c1";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c5`) PARTITION BY HASH(`c1`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName1 = "ug_c1_c2_3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_GET_DUP_USING_GSI=false)*/insert ignore into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        // Primary (Partition Pruning: 1)
        Assert.assertThat(trace.size(), is(1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName1 = "ug_c1_c2_3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/insert ignore into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        // primary (partition pruning: 1)  +  gsi(partition pruning: 1)
        Assert.assertThat(trace.size(), is(2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName1 = "ug_c1_c2_3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String insert = hint + "insert ignore into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        Assert.assertThat(trace.size(), is(1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName1 = "ug_c1_c2_3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c1`, `c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert ignore into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        // primary (partition pruning: 1)  +  gsi(partition pruning: 1)
        Assert.assertThat(trace.size(), is(1 + 1));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 有 UK, 一个 GSI
     * UK 未包含全部 Partition Key, INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_one_gsi";
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

        final String gsiName = "g_with_uk_c2";
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

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size()));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 有 UK, 一个 UGSI
     * UGSI 包含全部唯一键，直接下发 INSERT IGNORE
     */
    @Test
    public void tableWithPkWithUkWithUgsi() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_one_ugsi";
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

        final String gsiName1 = "ug_one_c2_c1";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`c2`, `c1`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_with_ugsi";
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

        final String gsiName1 = "u_g_c2_c3";
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
            + "  UNIQUE GLOBAL INDEX u_g_c2_c3(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String insert = hint + "insert ignore into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_g_c2_c3 冲突, ignore
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_g_c2_c3 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size() + 2));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_with_ugsi";
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

        final String gsiName1 = "u_g_c2_c3";
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
            + "  UNIQUE GLOBAL INDEX u_g_c2_c3(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert ignore into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_g_c2_c3 冲突, ignore
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_g_c2_c3 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        // gsi(partition pruning: 2) + gsi (partition pruning : 2) + insert(primary + gsi: 2)
        Assert.assertThat(trace.size(), is(2 + 2 + 2));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    @Test
    public void tableWithPkWithMultiUkWithUgsi1_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_with_ugsi";
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

        final String gsiName1 = "u_g_c2_c3";
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
            + "  UNIQUE GLOBAL INDEX u_g_c2_c3(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        String insert = "insert ignore into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        insert = "insert ignore into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(2, 2, 3, 'a', '2020-06-16 06:49:32'), " // ignore
            + "(2, 2, 4, 'b', '2020-06-16 06:49:32')"; // u_c1_c2_1 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     * DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_delete_only_gsi";
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

        final String gsiName = "g_i_c2_delete_only";
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

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ insert ignore into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // Primary (Partition Pruning: 3)
        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，INSERT IGNORE 转 SELECT + 去重 + INSERT
     * DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly2() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_delete_only_gsi";
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

        final String gsiName = "g_i_c2_delete_only";
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

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\")*/ insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), is(0));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, INSERT IGNORE 转 SELECT + 去重 + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    public void tableWithPkWithUkWithUgsi_deleteOnly() {
        final String tableName = "test_tb_with_pk_with_uk_delete_only_ugsi";
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
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "u_g_c2_c3_delete_only";
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
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\", DML_GET_DUP_USING_GSI=FALSE)*/ insert ignore into "
                + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), "
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // primary (partition pruning: 2)  + insert(primary: 2)
        Assert.assertThat(trace.size(), is(2 + 2));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), is(0));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, INSERT IGNORE 转 SELECT + 去重 + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    public void tableWithPkWithUkWithUgsi_deleteOnly_usingGsi() {
        final String tableName = "test_tb_with_pk_with_uk_delete_only_ugsi";
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
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "u_g_c2_c3_delete_only";
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

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\")*/ insert ignore into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), "
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        // primary (partition pruning: 3)  + insert(primary: 2)
        Assert.assertThat(trace.size(), is(2 + 2));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + getRealGsiName(tddlConnection, tableName, gsiName),
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键包含全部拆分键，但由于有 WRITE_ONLY 阶段的 GSI，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_write_only_gsi";
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

        final String gsiName = "g_i_c2_write_only";
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

        final String hint = buildCmdExtra(DML_WRITE_ONLY, DISABLE_SKIP_DUPLICATE_CHECK_FOR_PK, DISABLE_RETURNING);
        final String insert = hint + " insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // Primary(Partition pruning: 3)
        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键包含全部拆分键，但由于有 WRITE_ONLY 阶段的 GSI，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_no_uk_write_only_gsi_returning";
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

        final String gsiName = "g_i_c2_write_only_returning";
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

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/ insert ignore into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键包含全部拆分键，但由于有 WRITE_ONLY 阶段的 GSI，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly2() throws SQLException {
        final String tableName = "test_tb_with_pk_no_uk_write_only_gsi";
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

        final String gsiName = "g_i_c2_write_only";
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

        final String hint = buildCmdExtra(DML_WRITE_ONLY, DISABLE_RETURNING);
        final String insert = hint + " insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键包含全部拆分键，但由于有 WRITE_ONLY 阶段的 GSI，INSERT IGNORE 转 SELECT + 去重 + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly2_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "test_tb_with_pk_no_uk_write_only_gsi_returning";
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

        final String gsiName = "g_i_c2_write_only_returning";
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

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\")*/ insert ignore into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, INSERT IGNORE 转 SELECT + 去重 + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_write_only_ugsi";
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

        final String gsiName1 = "u_g_c2_c3_write_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1 + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_GET_DUP_USING_GSI=FALSE)*/ insert ignore into " + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_g_c2_c3 冲突, ignore
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_g_c2_c3 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), is(topology.size() + 2));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, INSERT IGNORE 转 SELECT + 去重 + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly_usingGsi() throws SQLException {
        final String tableName = "test_tb_with_pk_with_uk_write_only_ugsi";
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

        final String gsiName1 = "u_g_c2_c3_write_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName1 + "(`c2`,`c3`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\")*/ insert ignore into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_g_c2_c3 冲突, ignore
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突, ignore
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2_1 冲突，ignore
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_g_c2_c3 上与上面一行冲突，但是上面一个行被 ignore，这行保留
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        // primary (partition pruning: 3)  +  gsi(partition pruning: 1) + insert(primary + gsi: 2)
        Assert.assertThat(trace.size(), is(3 + 1 + 2));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有自增 PK
     * 验证是否会跳过对自增 PK 的检查
     */
    @Test
    public void tableWithAutoIncPk() throws SQLException {
        final String tableName = "test_tb_with_auto_inc_pk";
        dropTableIfExists(tableName);

        final String gsiName1 = "ug_c2_pk";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName1 + "(`c2`) PARTITION BY HASH(`c2`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));

        final String insert1 =
            "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName + "(`c1`,`c2`) values (1,2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert1);
        List<List<String>> trace = getTrace(tddlConnection);
        // 跳过 PK，只查找 UK
        Assert.assertThat(trace.size(), is(1 + 2));

        final String insert2 =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ insert ignore into "
                + tableName + "(`c1`,`c2`) values (2,3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert2);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(primaryTopology.size() + 1 + 2));

        final String insert3 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (null,3,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert3);
        trace = getTrace(tddlConnection);
        // 跳过 PK，只查找 UK
        Assert.assertThat(trace.size(), is(1 + 2));

        final String insert4 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (null,4,5),(100000,5,6)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert4);
        trace = getTrace(tddlConnection);
        // 手动指定了自增列，不跳过 PK
        Assert.assertThat(trace.size(), is(primaryTopology.size() + 2 + 4));

        final String insert5 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (null,6,7),(null,7,8)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert5);
        trace = getTrace(tddlConnection);
        // 跳过 PK
        Assert.assertThat(trace.size(), is(2 + 4));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有自增 Uk
     * 验证是否会跳过对自增 Uk 的检查
     */
    @Test
    public void tableWithAutoIncUk() throws SQLException {
        final String tableName = "test_tb_with_auto_inc_uk";
        dropTableIfExists(tableName);

        final String gsiName1 = "ug_c2_uk";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL AUTO_INCREMENT,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY (`c2`),"
            + "  GLOBAL INDEX " + gsiName1 + "(`c1`) PARTITION BY HASH(`c1`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`pk`) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String insert1 =
            "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName + "(`pk`,`c1`) values (1,2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert1);
        List<List<String>> trace = getTrace(tddlConnection);
        // 跳过 UK
        Assert.assertThat(trace.size(), is(1 + 2));

        final String insert2 =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ insert ignore into "
                + tableName + "(`pk`,`c1`) values (2,3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert2);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(primaryTopology.size() + 2));

        final String insert3 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (3,4,null)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert3);
        trace = getTrace(tddlConnection);
        // 跳过 UK
        Assert.assertThat(trace.size(), is(1 + 2));

        final String insert4 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (4,5,null),(5,6,100000)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert4);
        trace = getTrace(tddlConnection);
        // 手动指定了自增列，不跳过 PK
        Assert.assertThat(trace.size(), is(primaryTopology.size() + 4));

        final String insert5 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (6,7,null),(7,8,null)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert5);
        trace = getTrace(tddlConnection);
        // 跳过 UK
        Assert.assertThat(trace.size(), is(2 + 4));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 有自增 Column
     * 不应该跳过任何检查
     */
    @Test
    public void tableWithAutoIncCol() throws SQLException {
        final String tableName = "test_tb_with_auto_inc_col";
        dropTableIfExists(tableName);

        final String gsiName1 = "ug_c2_col";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL AUTO_INCREMENT,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  LOCAL KEY (`c2`),"
            + "  GLOBAL UNIQUE INDEX " + gsiName1 + "(`c1`) PARTITION BY HASH(`c1`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`pk`) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);

        final String insert1 =
            "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName + "(`pk`,`c1`) values (1,2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert1);
        List<List<String>> trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(1 + 1 + 2));

        final String insert2 =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ insert ignore into "
                + tableName + "(`pk`,`c1`) values (2,3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert2);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(1 + 1 + 2));

        final String insert3 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (3,4,null)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert3);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(1 + 1 + 2));

        final String insert4 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (4,5,null),(5,6,100000)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert4);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(2 + 2 + 4));

        final String insert5 = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName
            + "(`pk`,`c1`,`c2`) values (6,7,null),(7,8,null)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + insert5);
        trace = getTrace(tddlConnection);
        // 查找 PK, UK
        Assert.assertThat(trace.size(), is(2 + 2 + 4));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
    }

    /**
     * 检查是否限制了物理 SQL 的最大物理数量
     */
    @Test
    public void testMaxUnionCount() throws SQLException {
        final String tableName = "test_tb_union_count";
        dropTableIfExists(tableName);

        final String gsiName = "test_tb_union_count_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  LOCAL UNIQUE KEY (`c2`),"
            + "  GLOBAL UNIQUE INDEX " + gsiName + "(`c1`) PARTITION BY HASH(`c1`) PARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`pk`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology =
            JdbcUtil.getTopology(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        String insert = "insert ignore into " + tableName
            + " values(1,2,3),(2,3,4),(3,4,5),(4,5,6),(5,6,7),(6,7,8),(7,8,9),(8,9,10),(9,10,11),(10,11,12),(11,12,13),(12,13,14)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // no limit, no partition pruning on primary table
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE, DML_GET_DUP_UNION_SIZE=0)*/" + insert);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() + gsiTopology.size()));

        // limit 1
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE, DML_GET_DUP_UNION_SIZE=1)*/" + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(84));

        // limit 3
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE, DML_GET_DUP_UNION_SIZE=3)*/" + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is((29)));

        // limit 5
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE, DML_GET_DUP_UNION_SIZE=5)*/" + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(18));

        // limit -1, same as no limit
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE, DML_GET_DUP_UNION_SIZE=-1)*/" + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() + gsiTopology.size()));
    }

    /**
     * 检查 Insert Ignore 的 Column Mapping 是否正确
     */
    @Test
    public void testUkColumnMapping() throws SQLException {
        final String tableName = "test_uk_column_mapping";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        final String gsiName = "test_uk_column_mapping_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`pk`),\n"
            + "  UNIQUE KEY (`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`pk`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createGsi =
            "create global unique index " + gsiName + " on " + tableName + "(c2) partition by hash(c2) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
        final String insert = "insert into " + tableName + " values(0,1,1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        final String hint = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE,DML_EXECUTION_STRATEGY=LOGICAL)*/";
        final String insertIgnore = "insert ignore into " + tableName + " values(1,1,1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, hint + insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 验证 Insert Ignore 在大小写不敏感编码时的正确性
     */
    @Test
    public void checkCaseInsensitive() throws SQLException {
        final String tableName = "insert_ignore_test_case_insensitive";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int(11) primary key,\n"
            + "  `b` varchar(20) unique key\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "insert_ignore_test_case_insensitive_gsi";
        final String createGsi =
            String.format("create global unique index %s on %s(b) partition by hash(b) PARTITIONS 3", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "insert into " + tableName + " values(1,'QQ')";
        final String hint = "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE,DML_EXECUTION_STRATEGY=LOGICAL)*/";
        String insertIgnore = hint + "insert ignore into " + tableName + " values(5,'qq')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insertIgnore = hint + "insert ignore into " + tableName + " values(2,'Qq')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insertIgnore = hint + "insert ignore into " + tableName + " values(11,'tt'),(12,'TT')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 验证 Insert Ignore 在大小写不敏感编码时的正确性
     */
    @Test
    public void checkCaseInsensitivePk() throws SQLException {
        final String tableName = "insert_ignore_test_case_insensitive_pk";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` varchar(20) primary key\n,"
            + "  `b` int\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "insert_ignore_test_case_insensitive_pk_gsi";
        final String createGsi =
            String.format("create global unique index %s on %s(b) partition by hash(b) PARTITIONS 3", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/";
        String insertIgnore = hint + "insert ignore into " + tableName + " values ('A',1),('a',2)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insertIgnore = hint + "insert ignore into " + tableName + " values ('B',3),('b ',4)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, insertIgnore, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void checkHugeBatchInsertIgnoreTraceId_returing() throws SQLException {
        final String tableName = "insert_ignore_huge_batch_traceid_test";
        final String indexName = "insert_ignore_huge_batch_traceid_test_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` varchar(1024) \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by hash(`b`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final int batchSize = 4000;
        String pad = String.join("", Collections.nCopies(1000, "p"));
        StringBuilder sb = new StringBuilder();
        sb.append("insert ignore into " + tableName + " values");
        for (int i = 0; i < batchSize; i += 3) {
            String value = "(" + i + "," + i + 1 + ",'" + pad + "')";
            if (i + 3 < batchSize) {
                value += ",";
            }
            sb.append(value);
        }
        String insertIgnorePart = sb.toString();

        sb = new StringBuilder();
        sb.append("insert ignore into " + tableName + " values");
        for (int i = 0; i < batchSize; i++) {
            String value = "(" + i + "," + i + ",'" + pad + "')";
            if (i != batchSize - 1) {
                value += ",";
            }
            sb.append(value);
        }

        String insertIgnore = sb.toString();

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnorePart, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, "trace " + insertIgnore, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);
        checkPhySqlOrder(trace);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void checkHugeBatchInsertIgnoreTraceId() throws SQLException {
        final String tableName = "insert_ignore_huge_batch_traceid_test";
        final String indexName = "insert_ignore_huge_batch_traceid_test_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` varchar(1024) \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " partition by hash(`a`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by hash(`b`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final int batchSize = 4000;
        String pad = String.join("", Collections.nCopies(1000, "p"));
        StringBuilder sb = new StringBuilder();
        sb.append("/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName + " values");
        for (int i = 0; i < batchSize; i += 3) {
            String value = "(" + i + "," + i + 1 + ",'" + pad + "')";
            if (i + 3 < batchSize) {
                value += ",";
            }
            sb.append(value);
        }
        String insertIgnorePart = sb.toString();

        sb = new StringBuilder();
        sb.append("/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/ insert ignore into " + tableName + " values");
        for (int i = 0; i < batchSize; i++) {
            String value = "(" + i + "," + i + ",'" + pad + "')";
            if (i != batchSize - 1) {
                value += ",";
            }
            sb.append(value);
        }

        String insertIgnore = sb.toString();

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnorePart, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnore, "trace " + insertIgnore, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);
        checkPhySqlOrder(trace);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    private static final String SOURCE_TABLE_NAME = "insert_ignore_test_src_tbl";
    private static final String[][] REPLACE_PARAMS = new String[][] {
        new String[] {
            "(id,a,b)", "values (0,1,1),(1,2,2),(2,3,3),(100,100,100),(101,103,103)", "(id,a,b)",
            "values (0,1,1),(1,2,2),(2,3,3),(100,100,100),(101,103,103)"},
        new String[] {"(id)", "values (1)", "(id)", "values (1)"},
        new String[] {"(id,a,b)", "values (3,0+2,0+2)", "(id,a,b)", "values (3,2,2)"},
        new String[] {"(id,a,b)", "values (1,2,2),(2,3,3)", "(id,a,b)", "values (1,2,2),(2,3,3)"},
        new String[] {
            "(id,a,b)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (100,101,101)"},
        new String[] {
            "(id,a,b)", String.format("select * from %s where id>100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (101,102,102),(102,103,103)"}
    };

    @Test
    public void testLogicalInsertIgnore() throws SQLException {
        String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE)*/";

        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl",
            " partition by hash(id) PARTITIONS 3", false,
            true, true, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_brd", " broadcast", false, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_single", " single", false, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl",
            " partition by hash(id) PARTITIONS 3", true,
            true, true, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_brd", " broadcast", true, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_single", " single", true, true,
            false, REPLACE_PARAMS);
    }

    private void testComplexDmlInternal(String op, String tableName, String partitionDef, boolean withPk,
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

        for (int i = 0; i < params.length; i++) {
            String insert = String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
            String mysqlInsert =
                String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null, true);
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
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null,
                    true);
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
    public void testLogicalInsertIgnoreUsingIn() throws SQLException {
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE,DML_GET_DUP_USING_IN=TRUE)*/";

        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl",
            " partition by hash(id) PARTITIONS 3", false, true, true, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_brd", " broadcast", false, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_single", " single", false, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl",
            " partition by hash(id) PARTITIONS 3", true, true, true, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_brd", " broadcast", true, true,
            false, REPLACE_PARAMS);
        testComplexDmlInternal(hint + "insert ignore into", "insert_ignore_test_tbl_single", " single", true, true,
            false, REPLACE_PARAMS);
    }

    @Test
    public void testCrossSchemaInsertIgnore() throws SQLException {
        final String dbName = "test_db_cross_schema_dml";
        final String tableName = "t1";
        final String gsiName = "g1";

        Connection conn = null;
        try {
            if (useAffectedRows) {
                conn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
                useDb(conn, tddlDatabase1);
            } else {
                conn = getPolardbxConnection();
            }

            String sql = String.format("drop database if exists %s", dbName);
            JdbcUtil.executeUpdateSuccess(conn, sql);
            sql = String.format("create database %s mode='auto'", dbName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("use %s", dbName);
            JdbcUtil.executeUpdateSuccess(conn, sql);
            sql = String.format(
                "create table %s (a int primary key, b int, global index %s(b) partition by hash(b) PARTITIONS 3) partition by hash(a) PARTITIONS 3",
                tableName, gsiName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert ignore into %s.%s values (1,2)", dbName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format("replace into %s.%s values (1,2)", dbName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format("insert into %s.%s values (1,2) on duplicate key update a=2,b=3", dbName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("drop database %s", dbName);
            JdbcUtil.executeUpdateSuccess(conn, sql);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testInsertIgnoreUGSI_returning() throws SQLException {
        final String tableName = "insert_ignore_returing_ugsi_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,3,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String insertIgnoreSql = "insert ignore into " + tableName + " values (3,2,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnoreUGSI1_returning() throws SQLException {
        final String tableName = "insert_ignore_returing_ugsi1_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String insertIgnoreSql = "insert ignore into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, "trace " + insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnoreUGSI2_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "insert_ignore_returing_ugsi2_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int unique,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String insertIgnoreSql = "insert ignore into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, "trace " + insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnoreUGSI3_returning() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "insert_ignore_returing_ugsi3_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int unique,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create unique global index " + indexName + " on " + tableName
                + "(`c`) partition by range(`c`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`c`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String insertIgnoreSql = "insert ignore into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, "trace " + insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnoreUGSI() throws SQLException {
        final String tableName = "insert_ignore_ugsi_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,3,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String hint = buildCmdExtra(DISABLE_RETURNING);
        String insertIgnoreSql = hint + "insert ignore into " + tableName + " values (3,2,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnoreUGSI1() throws SQLException {
        final String tableName = "insert_ignore_ugsi1_tbl";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY RANGE (c) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName
                + "(`b`) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertSql = "insert into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String hint = buildCmdExtra(DISABLE_RETURNING);
        String insertIgnoreSql = hint + "insert ignore into " + tableName + " values (3,null,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnorePartFieldChecker_1() throws SQLException {
        final String tableName = "insert_ignore_part_field_1";
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
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert ignore into %s values (2, 'fdsafdsafdas')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
            Assert.assertTrue(allResult.get(0).get(1).equals("fdsa"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 'fd')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // space at the end of the string should be ignored
            sql = hint + String.format("insert ignore into %s values (2, 'fd  ')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
            Assert.assertTrue(allResult.get(0).get(1).equals("fd"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 'fd')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // case sensitive
            sql = hint + String.format("insert ignore into %s values (2, 'FD')", tableName);
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
    public void testInsertIgnorePartFieldChecker_2() throws SQLException {
        final String tableName = "insert_ignore_part_field_2";
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
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // case insensitive
            sql = hint + String.format("insert ignore into %s values (2, 'FD')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
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
    public void testInsertIgnorePartFieldChecker_3() throws SQLException {
        final String tableName = "insert_ignore_part_field_3";
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
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert ignore into %s values (2, -1)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
            Assert.assertTrue(allResult.get(0).get(1).equals("0"));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            // delete all
            sql = String.format("delete from %s where 1=1", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = String.format("insert into %s values (1, 4294967295)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert ignore into %s values (2, -1)", tableName);
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
            sql = hint + String.format("insert ignore into %s values (2, 4294967296)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(1));
            Assert.assertTrue(allResult.get(0).get(0).equals("1"));
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
    public void testInsertIgnorePartFieldChecker_4() throws SQLException {
        final String tableName = "insert_ignore_part_field_4";
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
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            // data should be truncated
            sql = hint + String.format("insert ignore into %s values (3, 0)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(3));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert ignore into %s values (4, 0)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(3));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert ignore into %s values (5, 123)", tableName);
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
    public void testInsertIgnorePartFieldChecker_5() throws SQLException {
        final String tableName = "insert_ignore_part_field_5";
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
            String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            sql = hint + String.format("insert ignore into %s values (3, 0),(4, 'dsa')", tableName);
            JdbcUtil.executeUpdateSuccess(conn, sql);

            ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, conn);
            List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
            Assert.assertThat(allResult.size(), Matchers.is(4));
            checkGsi(conn, getRealGsiName(conn, tableName, indexName));

            sql = hint + String.format("insert ignore into %s values (5, 'fdas'),(6, 234)", tableName);
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
    public void testInsertIgnorePartFieldChecker_type1() throws SQLException {
        final String tableName = "insert_ignore_part_field_type_1";
        final String indexName = tableName + "_gsi";
        String createSql =
            String.format("create table %s ("
                + "a int primary key, "
                + "b timestamp(6), "
                + "global unique index %s(b) partition by hash(b)"
                + ") partition by hash(a)", tableName, indexName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String sql = String.format("insert into %s values (1,'2018-01-01 00:00:01.33333')", tableName);
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint + String.format("insert ignore into %s values (2,'2018-01-01 00:00:01.33333')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 00:00:01.33333"));

        final String timeZone = JdbcUtil.getTimeZone(tddlConnection);

        JdbcUtil.setTimeZone(tddlConnection, "+9:00");

        sql = hint + String.format("insert ignore into %s values (3,'2018-01-01 01:00:01.33333')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 01:00:01.33333"));

        // reset timezone
        JdbcUtil.setTimeZone(tddlConnection, timeZone);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void testInsertIgnorePartFieldChecker_type2() throws SQLException {
        final String tableName = "insert_ignore_part_field_type_2";
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
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint + String.format("insert ignore into %s values (2,'2018-01-01')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void tesInsertIgnorePartFieldChecker_type3() throws SQLException {
        final String tableName = "insert_ignore_part_field_type_3";
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
        String hint = buildCmdExtra(DML_USE_NEW_DUP_CHECKER, DISABLE_RETURNING);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint + String.format("insert ignore into %s values (2,'2018-01-01 00:00:01.123')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2018-01-01 00:00:01.123"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void autoPartitionImplicitKeyTest() throws SQLException {
        String tableName = "auto_part_impl_key_tbl";
        String create = String.format("create table %s (a int)", tableName);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        Connection tddlConn;
        if (useAffectedRows) {
            tddlConn = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
            useDb(tddlConn, tddlDatabase1);
        } else {
            tddlConn = getPolardbxConnection();
        }
        Connection mysqlConn = getMysqlConnection();
        try {
            String[] sqlModes = {"NO_AUTO_VALUE_ON_ZERO", ""};

            for (String sqlMode : sqlModes) {
                String tddlSqlMode = JdbcUtil.getSqlMode(tddlConn);
                setSqlMode(sqlMode, tddlConn);

                String mysqlSqlMode = JdbcUtil.getSqlMode(mysqlConn);
                setSqlMode(sqlMode, mysqlConn);

                try {
                    String sql = String.format("insert into %s values (1),(2)", tableName);
                    executeOnMysqlAndTddl(mysqlConn, tddlConn, sql, null, true);
                    selectContentSameAssert("select * from " + tableName, null, mysqlConn, tddlConn);
                } finally {
                    // Reset sql mode
                    setSqlMode(tddlSqlMode, tddlConn);
                    setSqlMode(mysqlSqlMode, mysqlConn);
                }
            }
        } finally {
            tddlConn.close();
            mysqlConn.close();
        }
    }

    @Test
    public void testInsertIgnoreUGSI_returning1() throws SQLException {
        if (!supportReturning) {
            return;
        }

        final String tableName = "insert_ignore_returing_ugsi_tbl1";
        final String indexName = tableName + "_gsi";

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` int \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef =
            " PARTITION BY HASH(a)";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global unique index " + indexName + " on " + tableName + "(`b`) partition by hash(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName + " on " + tableName + "(`b`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        String insertIgnoreSql = "insert ignore into " + tableName + " values (3,2,5)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertIgnoreSql, "trace " + insertIgnoreSql, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        List<List<String>> traces = getTrace(tddlConnection);
        Assert.assertThat(traces.size(), is(2));
        for (List<String> trace : traces) {
            Assert.assertTrue(trace.get(11).contains("INSERT IGNORE INTO"));
        }

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexName));
    }

    @Test
    public void insertIgnoreSelectSubqueryTest() throws Exception {
        final String tableName1 = "insert_ignore_select_subquery_test1";
        final String tableName2 = "insert_ignore_select_subquery_test2";
        final String indexName1 = tableName1 + "_gsi";
        final String tableBody = "  `id` int(11) NOT NULL auto_increment,\n"
            + "  `name` varchar(30) DEFAULT NULL,\n"
            + "  `create_time` datetime DEFAULT NULL,\n"
            + "  `int_col1` int DEFAULT NULL,\n"
            + "  `int_col2` int DEFAULT NULL,\n"
            + "  `int_col3` int DEFAULT NULL,\n"
            + "  `int_col4` int DEFAULT NULL,\n"
            + "  `int_col5` int DEFAULT NULL,\n"
            + "  `int_col6` int DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + "  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 \n";

        // Create table
        final String createTable1 = "create table " + tableName1 + " (\n" + tableBody;
        final String createTable2 = "create table " + tableName2 + " (\n" + tableBody;
        final String partitionDef = " PARTITION BY HASH(id)";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExistsInMySql(tableName1);
        dropTableIfExistsInMySql(tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable1 + partitionDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable2 + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable1);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable2);

        // Create index
        final String createIndex = "create global unique index " + indexName1 + " on " + tableName1
            + "(`int_col4`) partition by hash(int_col4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
        final String createIndexMysql =
            "create unique index " + indexName1 + " on " + tableName1 + "(`int_col4`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createIndexMysql);

        // Init data
        String sql = String.format("insert into %s(name, create_time, int_col2, int_col3) values"
            + "(\"a\", \"2013-04-05 06:34:12\", 0, 1), "
            + "(\"b\", \"2013-04-05 06:34:12\", 0, 2), "
            + "(\"c\", \"2013-04-05 06:34:12\", 0, 3)", tableName2);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        sql = String.format("insert ignore into %s select * from %s", tableName1, tableName2);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        // Test correlate subquery
        sql = String.format("insert ignore into %s (int_col1, int_col2, int_col3, name, create_time, int_col4)\n"
            + "select distinct (ar.id), 1500000039, NULL, 'test0', '2020-03-03 18:20:17', null\n"
            + "  from %s ar, %s arpr\n"
            + "  where not exists (\n"
            + "    select *\n"
            + "    from %s\n"
            + "    where int_col3    = ar.int_col3\n"
            + "      and ar.int_col2 = 0\n"
            + "    )\n", tableName1, tableName1, tableName2, tableName2);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select name, create_time, int_col1, int_col2, int_col3, int_col4, int_col5, int_col6 from "
            + tableName1 + " order by id";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName1, indexName1));
    }
}
