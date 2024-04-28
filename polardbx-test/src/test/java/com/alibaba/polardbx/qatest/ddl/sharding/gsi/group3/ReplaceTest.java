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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.Matchers.is;

/**
 * @author chenmo.cm
 */
public class ReplaceTest extends DDLBaseNewDBTestCase {
    private static final String DISABLE_GET_DUP_USING_GSI = "DML_GET_DUP_USING_GSI=FALSE";
    private static final String FORCE_PUSHDOWN_RC_REPLACE = "DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE";
    private static final String DML_SKIP_IDENTICAL_ROW_CHECK = "DML_SKIP_IDENTICAL_ROW_CHECK=TRUE";
    private static final String DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK = "DML_SKIP_IDENTICAL_JSON_ROW_CHECK=FALSE";
    private static final String DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE = "DML_CHECK_JSON_BY_STRING_COMPARE=FALSE";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    /**
     * 无 PK 无 UK
     * REPLACE 转 INSERT 直接下发
     */
    @Test
    public void tableNoPkNoUk() {
        final String tableName = "replace_test_tb_no_pk_no_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK
     * REPLACE 转 SELECT + 去重 + DELETE + INSERT
     */
    @Test
    public void tableNoPkWithUk() {
        final String tableName = "replace_test_tb_no_pk_with_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK, UK 列 DEFAULT NULL
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableNoPkWithUk_defaultNull() {
        final String tableName = "replace_test_tb_no_pk_with_uk_default_null";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK
     * 使用默认值在 VALUES 中补上 UK，VALUES 中有重复
     */
    @Test
    public void tableNoPkWithUk_amendUk() {
        final String tableName = "replace_test_tb_no_pk_with_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // VALUES 中有重复，affected rows 可能会比 MySQL 返回的小 1
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有 UK, UK 是拆分键
     * 直接下发 REPLACE
     */
    @Test
    public void tableNoPkWithUk_partitionByUk() {
        final String tableName = "replace_test_tb_no_pk_with_uk";
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
        final String partitionDef = " dbpartition by hash(`id`) tbpartition by hash(`id`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "replace into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(2));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 无 PK 有多个 UK
     * 每个 UK 都包含全部拆分键，直接下发 REPLACE
     */
    @Test
    public void tableNoPkWithMultiUk_partitionByUk() {
        final String tableName = "replace_test_tb_no_pk_with_multi_uk";
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
        final String partitionDef = " dbpartition by hash(`id`) tbpartition by hash(`id`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "replace into " + tableName
            + "(id, c1, c5, c8) values(1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 'b', '2020-06-16 06:49:32'), (1, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(2));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，直接下发 REPLACE
     */
    @Test
    public void tableWithPkNoUk_partitionByPk() {
        final String tableName = "replace_test_tb_with_pk_no_uk_pk_partition";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 无 UK, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 REPLACE
     */
    @Test
    public void tableWithPkNoUk_partitionByPk2() {
        final String tableName = "replace_test_tb_with_pk_no_uk_pk_partition";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkWithUk() {
        final String tableName = "replace_test_tb_with_pk_with_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkWithUk2() {
        final String tableName = "replace_test_tb_with_pk_with_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (null, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUk() {
        final String tableName = "replace_test_tb_with_pk_with_multi_uk";
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
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);
    }

    /*
     * 包含 GSI 的测试用例
     */

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_replace_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi2() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_replace_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI, 主键拆分
     * 唯一键包含全部拆分键，直接下发 REPLACE
     */
    @Test
    public void tableWithPkNoUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_replace_c2";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，每张表中都包含全部UK, REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_replace_two_c1";
        final String gsiName2 = "g_replace_two_c2";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 3 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，每张表中都包含全部UK, REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk2() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_replace_two_c1";
        final String gsiName2 = "g_replace_two_c2";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 3 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 有联合 PK 无 UK, 两个 GSI, 主键拆分
     * 唯一键包含全部拆分键，直接下发 REPLACE
     */
    @Test
    public void tableWithCompositedPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_two_gsi";
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

        final String gsiName1 = "g_replace_two_c1";
        final String gsiName2 = "g_replace_two_c2";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`id`) COVERING(`c4`) DBPARTITION BY HASH(`id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 5\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 * 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi";
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

        final String gsiName = "ug_replace_one_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (partition pruning: 1) + gsi(partition pruning: 1) + replace(primary + gsi: 3)
        Assert.assertThat(trace.size(), Matchers.is(1 + 1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi";
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

        final String gsiName = "ug_replace_one_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 REPLACE
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk2() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi";
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

        final String gsiName = "ug_replace_one_c1";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 主表 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName = "ug_replace_c1_c2_3";
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
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (1) + gsi(1 + 1) + replace(primary + gsi: 3)
        Assert.assertThat(trace.size(), Matchers.is(3 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 主表 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName = "ug_replace_c1_c2_3";
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
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE,DML_GET_DUP_USING_GSI=FALSE)*/replace into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        Assert.assertThat(trace.size(), Matchers.is(1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 主表 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName = "ug_replace_c1_c2_3";
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
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (partition pruning: 1) + gsi(partition pruning: 1 + 1) + replace(primary + gsi: 3)
        Assert.assertThat(trace.size(), Matchers.is(1 + 1 + 1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 主表 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi3";
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

        final String gsiName = "ug_replace_c1_c2_3";
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
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        Assert.assertThat(trace.size(), Matchers.is(1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 主表 REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk4() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi4";
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

        final String gsiName = "ug_replace_c1_c2_4";
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
            + "  UNIQUE CLUSTERED INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        String hint = buildCmdExtra(FORCE_PUSHDOWN_RC_REPLACE);
        final String insert = hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (partition pruning: 1) + cgsi(partition pruning: 1) + replace(primary + gsi: 3)
        Assert.assertThat(trace.size(), Matchers.is(1 + 1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK, 一个 GSI
     * UK 未包含全部 Partition Key, REPLACE 转 SELECT + DELETE + INSERT
     * 主表上包含全部 UK，REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkWithUkWithGsi_partitionByPk() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_one_gsi";
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

        final String gsiName = "g_replace_with_uk_c2";
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
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 3 + 3 * 2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK, 一个 UGSI
     * UGSI 包含全部唯一键，直接下发 REPLACE
     */
    @Test
    public void tableWithPkWithUkWithUgsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_one_ugsi";
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

        final String gsiName = "ug_replace_one_c2_c1";
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
            + "(`c2`, `c1`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(4));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 索引表包含所有UK，REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_with_ugsi";
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

        final String gsiName = "ug_replace_c2_c3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (partition pruning: 3) + gsi(partition pruning: 1 + primary partition pruning: 2) + replace(primary + gsi: 7)
        Assert.assertThat(trace.size(), Matchers.is(3 + 1 + 2 + (2 * 2 + 1) + (1 + 1)));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 索引表包含所有UK，REPLACE 转 SELECT + REPLACE + INSERT
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_with_ugsi";
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

        final String gsiName = "ug_replace_c2_c3";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + (2 * 2 + 1) + (1 + 1)));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，
     * 主表包含全部UK，REPLACE 转 SELECT + REPLACE + INSERT
     * 索引表，REPLACE 转 SELECT + DELETE + INSERT，DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_delete_only_gsi";
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

        final String gsiName = "g_replace_c2_delete_only";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + gsiName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，
     * 主表包含全部UK，REPLACE 转 SELECT + REPLACE + INSERT
     * 索引表，REPLACE 转 SELECT + DELETE + INSERT，DELETE_ONLY 模式默认忽略 INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_deleteOnly2() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_delete_only_gsi";
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

        final String gsiName = "g_replace_c2_delete_only";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + gsiName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 索引表包含全部UK, 但因为是 DELETE_ONLY 模式， REPLACE 转 SELECT + DELETE + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    @CdcIgnore(ignoreReason = "忽略ugsi强行写入，会导致上下游不一致")
    public void tableWithPkWithUkWithUgsi_deleteOnly() {
        final String tableName = "replace_test_tb_with_pk_with_uk_delete_only_ugsi";
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

        final String gsiName = "ug_replace_c2_c3_delete_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\", DML_GET_DUP_USING_GSI=FALSE)*/ replace into " + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " //
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // 不冲突
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // primary (partition pruning: 3) + replace(primary: 6)
        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 3));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + gsiName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键
     * 主表 REPLACE 转 SELECT + DELETE + INSERT
     * 索引表包含全部UK, 但因为是 DELETE_ONLY 模式， REPLACE 转 SELECT + DELETE + INSERT
     * 验证 DELETE_ONLY 模式符合预期
     * 在 DELETE_ONLY 模式下，该 UK 视为不存在
     */
    @Test
    @CdcIgnore(ignoreReason = "忽略ugsi强行写入，会导致上下游不一致")
    public void tableWithPkWithUkWithUgsi_deleteOnly_usingGsi() {
        final String tableName = "replace_test_tb_with_pk_with_uk_delete_only_ugsi";
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

        final String gsiName = "ug_replace_c2_c3_delete_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\")*/ replace into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String checkSql = "select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // primary (partition pruning: 3) + replace(primary: 6)
        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 3));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + gsiName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 所有UK包含全部拆分键，但有 WRITE_ONLY 阶段的 GSI，
     * 主表包含所有UK, REPLACE 转 SELECT + REPLACE + INSERT
     * 索引表 REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_write_only_gsi";
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

        final String gsiName = "g_replace_c2_write_only";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 所有UK包含全部拆分键，但有 WRITE_ONLY 阶段的 GSI，
     * 主表包含所有UK, REPLACE 转 SELECT + REPLACE + INSERT
     * 索引表 REPLACE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly2() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_write_only_gsi";
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

        final String gsiName = "g_replace_c2_write_only";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 2));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, REPLACE 转 SELECT + DELETE + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_write_only_ugsi";
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

        final String gsiName = "ug_replace_c2_c3_write_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\")*/ replace into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (partition pruning: 3) + gsi(partition pruning: 1 + primary partition pruning: 2) + replace(primary + gsi: 3 + 3 + 1)
        Assert.assertThat(trace.size(), Matchers.is(3 + 1 + 2 + 7));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有多个 UK
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * 唯一键没有包含全部拆分键, REPLACE 转 SELECT + DELETE + INSERT
     * 校验 WRITE_ONLY 状态下结果符合预期
     */
    @Test
    public void tableWithPkWithUkWithUgsi_writeOnly() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_with_uk_write_only_ugsi";
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

        final String gsiName = "ug_replace_c2_c3_write_only";
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
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\", DML_GET_DUP_USING_GSI=FALSE)*/ replace into "
                + tableName
                + "(c1, c2, c3, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
                + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
                + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
                + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
                + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        // Lookup (all primary table) + primary(2 + 3) + gsi(1 + 1)
        Assert.assertThat(trace.size(), Matchers.is(topology.size() + (2 + 3) + (1 + 1)));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
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

        final String gsiName = "ug_replace_two_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String replace =
            "replace into " + tableName + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (all primary phy table) + gsi(partition pruning: 1 + primary partition pruning: 1) + replace(primary + gsi: 6)
        Assert.assertThat(trace.size(), Matchers.is(primaryTopology.size() + 1 + 1 + 6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
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

        final String gsiName = "ug_replace_two_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String replace =
            hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 6));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1_usingGsi() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
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

        final String gsiName = "ug_replace_three_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String replace =
            "replace into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        // final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);
        // primary (all primary phy table) + gsi(partition pruning: 3 + primary partition pruning: 1) + replace(primary + gsi: 11)
        Assert.assertThat(trace.size(), Matchers.is(primaryTopology.size() + 3 + 1 + 6 + 2 + 3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上 REPLACE 转 SELECT + DELETE + INSERT
     * UGSI 中包含全部唯一键，UGSI 上 REPLACE 转 SELECT + REPLACE + INSERT
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1() throws SQLException {
        final String tableName = "replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
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

        final String gsiName = "ug_replace_three_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String replace =
            hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(topology.size() + 6 + 2 + 3));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 重复执行四次，验证 affected rows 结果符合预期
     * 注意：由于是通过 JDBC 下发，useAffectedRows 默认为 false，也就是 CLIENT_FOUND_ROWS=1 ，
     * 因此返回的是 touched 而非 updated。与此对应的是官方命令行工具仅支持 CLIENT_FOUND_ROWS=0 ，
     * 因此返回的是 updated ，如果更新后取值无变化则返回 0
     */
    @Test
    public void checkAffectedRows() throws SQLException {
        final String tableName = "replace_test_result_tb_with_pk_no_uk_one_gsi";
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

        final String gsiName = "g_replace_result_c2";
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
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        // one
        final String insert = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        // two
        final String insert1 = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:33'), (2, 'b', '2020-06-16 06:49:33'), (3, 'c', '2020-06-16 06:49:33')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        // three
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // four
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 验证在 RC 的隔离级别下不会下推 REPLACE
     */
    @Test
    public void checkIsolationLevelRC() throws SQLException {
        final String tableName = "replace_test_isolation_level";
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE " + tableName + " (\n"
            + "  `a` int(11) NOT NULL,\n"
            + "  `b` int(11) DEFAULT NULL,\n"
            + "  `c` int(11) DEFAULT NULL,\n"
            + "  `d` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE KEY `b` (`b`),\n"
            + "  UNIQUE KEY `c` (`c`),\n"
            + "  KEY `auto_shard_key_d` USING BTREE (`d`),\n"
            + "  GLOBAL INDEX `g`(`c`) COVERING (`a`, `d`) DBPARTITION BY HASH(`c`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`d`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insert = "insert into " + tableName + " values (1,2,3,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            String replace = "trace replace into " + tableName + " values (1,3,3,4)";
            JdbcUtil.executeUpdateSuccess(conn, replace);
            List<List<String>> trace = getTrace(conn);

            for (List<String> row : trace) {
                String phySql = row.get(row.size() - 4);
                Assert.assertFalse(phySql.contains("REPLACE"));
            }

            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    /**
     * 验证在 RR 的隔离级别下会下推 REPLACE
     */
    @Test
    public void checkIsolationLevelRR() throws SQLException {
        final String tableName = "replace_test_isolation_level";
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE " + tableName + " (\n"
            + "  `a` int(11) NOT NULL,\n"
            + "  `b` int(11) DEFAULT NULL,\n"
            + "  `c` int(11) DEFAULT NULL,\n"
            + "  `d` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE KEY `b` (`b`),\n"
            + "  UNIQUE KEY `c` (`c`),\n"
            + "  KEY `auto_shard_key_d` USING BTREE (`d`),\n"
            + "  GLOBAL INDEX `g`(`c`) COVERING (`a`, `d`) DBPARTITION BY HASH(`c`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`d`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insert = "insert into " + tableName + " values (1,2,3,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            String replace = "trace replace into " + tableName + " values (1,3,3,4)";
            JdbcUtil.executeUpdateSuccess(conn, replace);
            List<List<String>> trace = getTrace(conn);
            boolean hasReplace = false;

            for (List<String> row : trace) {
                String phySql = row.get(row.size() - 4);
                hasReplace |= phySql.contains("REPLACE");
            }
            Assert.assertTrue(hasReplace);

            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    /**
     * 验证 Replace 在大小写不敏感编码时的正确性
     */
    @Test
    public void checkCaseInsensitive() throws SQLException {
        final String tableName = "replace_test_case_insensitive";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int(11) primary key,\n"
            + "  `b` varchar(20) unique key\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " dbpartition by hash(`a`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "replace_test_case_insensitive_gsi";
        final String createGsi =
            String.format("create global unique index %s on %s(b) dbpartition by hash(b)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "insert into " + tableName + " values(1,'QQ')";
        String replace = "replace into " + tableName + " values(5,'qq')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, replace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        replace = "replace into " + tableName + " values(2,'Qq')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, replace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        replace = "replace into " + tableName + " values(11,'tt'),(12,'TT')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, replace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void checkHugeBatchReplaceTraceId() throws SQLException {
        final String tableName = "replace_huge_batch_traceid_test";
        final String indexName = "replace_huge_batch_traceid_test_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` varchar(1024) \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        final String partitionDef = " dbpartition by hash(`a`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global index " + indexName + " on " + tableName + "(`b`) dbpartition by hash(`b`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final int batchSize = 1000;
        String pad = String.join("", Collections.nCopies(1000, "p"));
        StringBuilder sb = new StringBuilder();
        sb.append("replace into " + tableName + " values");
        for (int i = 0; i < batchSize; i++) {
            String value = "(" + i + "," + i + ",'" + pad + "')";
            if (i != batchSize - 1) {
                value += ",";
            }
            sb.append(value);
        }

        String replace = sb.toString();
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + replace);
        final List<List<String>> trace = getTrace(tddlConnection);
        checkPhySqlOrder(trace);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, replace);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, replace);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    private static final String SOURCE_TABLE_NAME = "replace_test_src_tbl";
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
    public void testLogicalReplace() throws SQLException {
        String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE)*/";

        testComplexDmlInternal(hint + "replace into", "replace_test_tbl", " dbpartition by hash(id)", false, true, true,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_brd", " broadcast", false, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_single", " single", false, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl", " dbpartition by hash(id)", true, true, true,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_brd", " broadcast", true, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_single", " single", true, true, false,
            REPLACE_PARAMS);
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
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null, false);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
        if (withGsi) {
            String gsiName1 = tableName + "_gsi_a";
            String gsiName2 = tableName + "_gsi_b";
            String gsiName3 = tableName + "_gsi_ab";
            String gsiName4 = tableName + "_gsi_ba";
            String createGsiSql1 =
                String.format("create global index %s on %s(a) dbpartition by hash(a)", gsiName1, tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(b) dbpartition by hash(b)", gsiName2, tableName);
            String createGsiSql3 =
                String.format("create global index %s on %s(a) covering(id,b) dbpartition by hash(a)", gsiName3,
                    tableName);
            String createGsiSql4 =
                String.format("create global index %s on %s(b) covering(id,a) dbpartition by hash(b)", gsiName4,
                    tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql3);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql4);
            String deleteAll = "delete from " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, false);
            for (int i = 0; i < params.length; i++) {
                String insert =
                    String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
                String mysqlInsert =
                    String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null,
                    false);
                selectContentSameAssert("select * from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, gsiName1);
                checkGsi(tddlConnection, gsiName2);
                checkGsi(tddlConnection, gsiName3);
                checkGsi(tddlConnection, gsiName4);
            }
        }
    }

    /**
     * 检查使用 IN 代替 UNION 的 HINT 是否生效
     */
    @Test
    public void testSelectUseInHint() throws SQLException {
        final String tableName = "test_tb_use_in";
        dropTableIfExists(tableName);

        final String gsiName = "test_tb_use_in_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  GLOBAL INDEX " + gsiName + "(`c1`) DBPARTITION BY HASH(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String replace = "replace into " + tableName + " values (1,2,3),(2,2,3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + buildCmdExtra("DML_GET_DUP_USING_IN=TRUE") + replace);
        List<List<String>> trace = getTrace(tddlConnection);
        String phySql = trace.get(0).get(trace.get(0).size() - 4);
        Assert.assertFalse(phySql.contains("UNION"));

        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + replace);
        trace = getTrace(tddlConnection);
        phySql = trace.get(0).get(trace.get(0).size() - 4);
        Assert.assertTrue(phySql.contains("UNION"));
    }

    /**
     * 检查是否限制了物理 SQL 的 IN 数量
     */
    @Test
    public void testMaxInCount() throws SQLException {
        final String tableName = "test_tb_in_count";
        dropTableIfExists(tableName);

        final String gsiName = "test_tb_in_count_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  LOCAL UNIQUE KEY (`c2`),"
            + "  GLOBAL INDEX " + gsiName + "(`c1`) DBPARTITION BY HASH(`c1`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final List<Pair<String, String>> primaryTopology = JdbcUtil.getTopology(tddlConnection, tableName);
        final List<Pair<String, String>> gsiTopology = JdbcUtil.getTopology(tddlConnection, gsiName);

        String insert = "replace into " + tableName
            + " values(1,2,3),(2,3,4),(3,4,5),(4,5,6),(5,6,7),(6,7,8),(7,8,9),(8,9,10),(9,10,11),(10,11,12),(11,12,13),(12,13,14)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // no limit, no partition pruning on primary table
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_IN=TRUE, DML_GET_DUP_IN_SIZE=0, DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/"
                + insert);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() + gsiTopology.size() * 3));

        // limit 1
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_IN=TRUE, DML_GET_DUP_IN_SIZE=1, DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/"
                + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() * 12 * 2 + gsiTopology.size() * 3));

        // limit 3
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_IN=TRUE, DML_GET_DUP_IN_SIZE=3, DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/"
                + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() * 4 * 2 + gsiTopology.size() * 3));

        // limit 5
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_IN=TRUE, DML_GET_DUP_IN_SIZE=5, DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/"
                + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() * 5 + gsiTopology.size() * 3));

        // limit -1, same as no limit
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace /*+TDDL:CMD_EXTRA(DML_GET_DUP_USING_IN=TRUE, DML_GET_DUP_IN_SIZE=-1, DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/"
                + insert);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(primaryTopology.size() + gsiTopology.size() * 3));
    }

    @Test
    public void testLogicalReplaceUsingIn() throws SQLException {
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE,DML_GET_DUP_USING_IN=TRUE)*/";

        testComplexDmlInternal(hint + "replace into", "replace_test_tbl", " dbpartition by hash(id)", false, true, true,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_brd", " broadcast", false, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_single", " single", false, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl", " dbpartition by hash(id)", true, true, true,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_brd", " broadcast", true, true, false,
            REPLACE_PARAMS);
        testComplexDmlInternal(hint + "replace into", "replace_test_tbl_single", " single", true, true, false,
            REPLACE_PARAMS);
    }

    @Test
    public void testReplaceMultipleUkConflict() throws SQLException {
        final String tableName = "test_tb_replace_multiple_uk_conflict";
        dropTableIfExists(tableName);

        final String gsiName = tableName + "_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  `c3` bigint(20) DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`), \n"
            + "  GLOBAL INDEX " + gsiName + "(`c1`) covering(`c2`) DBPARTITION BY HASH(`c1`), \n"
            + "  UNIQUE INDEX l1 on g1(`c2`) "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c3`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String sql = String.format("insert into %s values (1,1,1,1), (1,2,1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("replace into %s values (3,1,1,1), (4,2,1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, gsiName);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(1));
    }

    @Test
    public void testReplaceJson() {
        final String tableName = "replace_json_tbl";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) dbpartition by hash(b)) dbpartition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String replace =
            String.format("replace into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);
        replace = String.format("replace into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}')", tableName);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + replace, "");
        hint = buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DML_SKIP_IDENTICAL_ROW_CHECK);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + replace);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    @Test
    public void testReplaceJson1() {
        final String tableName = "replace_json_tbl1";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) dbpartition by hash(b)) dbpartition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String replace =
            String.format("replace into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);
        replace = String.format("replace into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}')", tableName);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + replace, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    @Test
    public void testReplacePushdown() {
        final String tableName1 = "replace_pushdown_tbl1";
        final String tableName2 = "replace_pushdown_tbl2";
        final String tableName3 = "replace_pushdown_tbl3";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExists(tableName3);

        String create = String.format("create table %s(e int primary key) dbpartition by hash(e)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        create = String.format("CREATE TABLE %s (\n"
            + "  a bigint(20) NOT NULL, \n"
            + "  b bigint(20) NOT NULL, \n"
            + "  c bigint(20) NOT NULL, \n"
            + "  d int(11) NOT NULL, \n"
            + "  PRIMARY KEY (`a`)\n"
            + ") DBPARTITION BY hash(`d`)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        create = String.format("CREATE TABLE %s (\n"
            + "  b bigint(20) NOT NULL, \n"
            + "  d int(11) NOT NULL, \n"
            + "  PRIMARY KEY (`b`)\n"
            + ") DBPARTITION BY hash(`d`);", tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert = String.format("insert into %s values (1,1,1,1)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        insert = String.format("insert into %s values (1,1)", tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String replace = MessageFormat.format(
            "REPLACE INTO {0}(e) SELECT 1 FROM {1} INNER JOIN {2} ON {1}.b = {2}.b AND {2}.d = 1 WHERE {1}.d = 1 AND {1}.c = 1;\n",
            tableName1, tableName2, tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);

        String select = String.format("select * from %s", tableName1);
        ResultSet rs = JdbcUtil.executeQuery(select, tddlConnection);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);

        Assert.assertEquals(1, objects.size());
        Assert.assertEquals("1", objects.get(0).get(0).toString());
    }

    @Test
    public void testReplaceWithUgsiAndJson() throws SQLException {
        final String tableName = "test_tb_replace_with_json";
        dropTableIfExists(tableName);

        final String gsiName = tableName + "_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  `c3` bigint(20) DEFAULT NULL ,\n"
            + "  `c4` json DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`), \n"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c1`) covering(`c2`) DBPARTITION BY HASH(`c1`), \n"
            + "  UNIQUE INDEX l1 on g1(`c2`) "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c3`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String sql = String.format("insert into %s values (1,1,1,1,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("replace into %s values (1,2,3,4,'{\"a\":\"b\"}')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, gsiName);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertEquals("2", allResult.get(0).get(1).toString());
        Assert.assertEquals("3", allResult.get(0).get(2).toString());
        Assert.assertEquals("4", allResult.get(0).get(3).toString());
        Assert.assertEquals("{\"a\": \"b\"}", allResult.get(0).get(4).toString());
    }
}
