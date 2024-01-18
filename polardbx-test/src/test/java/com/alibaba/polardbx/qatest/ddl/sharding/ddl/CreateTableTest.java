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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.RuleValidator;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertNotExistsBroadcastTable;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardDbTableExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardDbTableNotExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.isIndexExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 建表测试
 *
 * @author simiao
 * @since 14-11-27
 */

public class CreateTableTest extends AsyncDDLBaseNewDBTestCase {

    private String testTableName = "gxw_test";
    private String newSuffix = "_new";
    private List<Connection> phyConnectionList;

    public CreateTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{true}});
    }

    @Before
    public void cleanEnv() {

        this.phyConnectionList =
            StringUtils.isBlank(tddlDatabase2) ? getMySQLPhysicalConnectionList(tddlDatabase1) :
                getMySQLPhysicalConnectionList(tddlDatabase2);
    }

    @After
    public void clean() {

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTableNoPartition() {
        String simpleTableName = "simiao_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + tableName + " (id int,name varchar(30),primary key(id))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("partition"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));
        Assert.assertTrue(showCreateTableString.contains("`id` int"));
        Assert.assertTrue(showCreateTableString.contains("`name` varchar(30)"));

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikeNoPartition() {
        String simpleTableName = "simiao_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + simpleTableName + newSuffix;
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = "create table if not exists " + tableName
            + " (id int,name varchar(30),primary key(id)) CHARSET=utf8mb4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("partition"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));
        Assert.assertTrue(showCreateTableString.contains("`id` int"));
        Assert.assertTrue(showCreateTableString.contains("`name` varchar(30)"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertFalse(showCreateTableString.contains("partition"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "_new`"));
        Assert.assertTrue(showCreateTableString.contains("`id` int"));
        Assert.assertTrue(showCreateTableString.contains("`name` varchar(30)"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTableNoPartitionWithKeyWord() {
        String simpleTableName = "tunan1_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + tableName + " (`key` int,name varchar(30),primary key(`key`))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableName, "`key`");

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("partition"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));
        Assert.assertTrue(showCreateTableString.contains("`key` int"));
        Assert.assertTrue(showCreateTableString.contains("`name` varchar(30)"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTableNoPartitionWithFromWord() {
        String simpleTableName = "tunan2_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + tableName + " (`from` int,name varchar(30),primary key(`from`))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableName, "`from`");

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("partition"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));
        Assert.assertTrue(showCreateTableString.contains("`from` int"));
        Assert.assertTrue(showCreateTableString.contains("`name` varchar(30)"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTablePartitionWithKey() {
        String simpleTableName = "simiao3_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.3.13
     */
    @Test
    public void testCreateTablePartitionSpecialKey() {
        String simpleTableName = "`simiao3-test`";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE if not exists "
            + tableName
            + " (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `name-name` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(`name-name`) tbpartition by hash(`name-name`) tbpartitions 2;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("dbpartition by hash(`name-name`) tbpartition by hash(`name-name`) tbpartitions 2"));
        Assert.assertTrue(showCreateTableString.contains(simpleTableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikePartitionWithKey() {
        String simpleTableName = "simiao3_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + simpleTableName + newSuffix;

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) CHARSET=utf8mb4 dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableNewName);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + newSuffix + "`"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    @Test
    public void testCreateTableLikePartitionFromDifferentSchema() {
        String simpleTableName = "simiao3_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + simpleTableName + newSuffix;

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) CHARSET=utf8mb4 dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableNewName);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + newSuffix + "`"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    @Test
    public void testCreateTableLikePartitionToDifferentSchema() {
        String simpleTableName = "simiao3_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + simpleTableName + newSuffix;

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) CHARSET=utf8mb4 dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableNewName);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + newSuffix + "`"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    @Test
    public void testCreateTableLikeTableWithGSI() {
        String simpleTableName = "simiao3_test_gsi";
        String tableName = simpleTableName;
        String tableNewName = simpleTableName + newSuffix;

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = HINT_CREATE_GSI
            + " CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`),\n"
            + "  GLOBAL INDEX g_i_seller(`seller_id`) COVERING (`id`, `order_id`) DBPARTITION BY hash(`seller_id`) TBPARTITION BY hash(`seller_id`) TBPARTITIONS 4,\n"
            + "  UNIQUE GLOBAL g_i_buyer (`buyer_id`) COVERING (`id`, `order_id`, `order_snapshot`) DBPARTITION BY hash(`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`order_id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`order_id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + newSuffix + "`"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    @Test
    public void testCreateTableLikePartitionWithKeyWithSpecialName() {
        String simpleTableName = "simiao3_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + "`create`";

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) CHARSET=utf8mb4 dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableNewName);
        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + "create" + "`"));
        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTablePartitionWithKeyAndKeyWord() {
        String simpleTableName = "tunan3_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + tableName
            + " (`key` int, name varchar(30), primary key(`key`)) dbpartition by hash(`key`) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableName, "`key`");

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`key`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikePartitionWithKeyAndKeyWord() {
        String simpleTableName = "tunan3_test";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = schemaPrefix + simpleTableName + newSuffix;

        dropTableIfExists(tableName);

        String sql = "create table if not exists "
            + tableName
            + " (`key` int, name varchar(30), primary key(`key`)) CHARSET=utf8mb4 dbpartition by hash(`key`) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableName, "`key`");

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`key`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        sql = "create table if not exists " + tableNewName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableNewName, "`key`");

        showCreateTableString = showCreateTable(tddlConnection, tableNewName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`key`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + newSuffix + "`"));

        dropTableIfExists(tableName);
        dropTableIfExists(tableNewName);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testCreateTablePartitionWithKeyAndFromWord() {
        String simpleTableName = "tunan4_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists "
            + tableName
            + " (`from` int, name varchar(30), primary key(`from`)) dbpartition by hash(`from`) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTableWithKeyName(tableName, "`from`");

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`from`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableSubpartitionWithKey() {
        String simpleTableName = "simiao4_test";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by hash(`id`) tbpartitions 4"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableIfNotExist() {
        String simpleTableName = testTableName + "_1";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists " + tableName + " (id int,name varchar(30),primary key(id)) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);

        String phyTableName = getTbNamePattern(tableName);
        assertExistsTable(phyTableName, phyConnectionList.get(1));

        sql = "create table if not exists " + tableName
            + " (idd int,name varchar(30),primary key(idd)) dbpartition by hash(idd) dbpartitions 2";

        // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccessWithWarning(tddlConnection, sql, "Table '" + simpleTableName
                + "' already exists");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Rule already exists");
        }

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableIfNotExistMulti() {

        String simpleTableName = testTableName + "_2";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists " + tableName
            + " (id int, name varchar(30),primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        sql = "create table if not exists " + tableName
            + " (id int, name2 date,primary key(id)) dbpartition by hash(id) dbpartitions 2 ";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);

        // 用户判断表结构没有变化
        runInsertOneForTable(tableName);

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikeIfNotExistMulti() {
        String simpleTableName = testTableName + "_2";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists " + tableName
            + " (id int, name varchar(30),primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        sql = "create table if not exists " + tableName + " like " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Not unique table/alias");
        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);
        // 用户判断表结构没有变化
        runInsertOneForTable(tableName);

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikeExist() {
        String simpleTableName = testTableName + "_3";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists " + tableName
            + " (id int, name varchar(30),primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        sql = "create table " + tableName + " like " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Not unique table/alias");
        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);
        // 用户判断表结构没有变化
        runInsertOneForTable(tableName);

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikeExist2() {
        String simpleTableName = testTableName + "_3";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists " + tableName
            + " (id int, name varchar(30),primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String simpleTableName2 = testTableName + "_4";
        String tableName2 = schemaPrefix + simpleTableName2;
        dropTableIfExists(tableName2);
        String sql2 = "create table if not exists " + tableName2
            + " (id int, name varchar(30),primary key(id)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        sql = "create table " + tableName2 + " like " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "already exists");
        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName);

        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);
        // 用户判断表结构没有变化
        runInsertOneForTable(tableName);

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLikeExist3() {
        String simpleTableName = testTableName + "_4";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + "tb123" + " like " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "doesn't exist");
    }

    @Test
    public void testCreateTableLikeIfNotExist2() {
        String simpleTableName = testTableName + "_5";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table if not exists " + "tb123" + " like " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "doesn't exist");
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableAllDataType() {
        String simpleTableName = testTableName + "_3";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + " ("
            + "pk int not null auto_increment primary key,"
            + "tint tinyint(10) unsigned zerofill,"
            + "sint smallint default 1000,"
            + " mint mediumint unique key,   "
            + " bint bigint(20) comment \"bigint\","
            + "rint real(10, 2) references tt1 (rint) match full on delete restrict,"
            + "dble double(10,2) references tt1 (dble) match partial on delete cascade,"
            + " fl float(10,2) references tt1 (fl) match simple on delete set null,"
            + " dc decimal(10,2) references tt1(dc) match simple on update no action,"
            + "num numeric(10,2),"
            + "dt date null,"
            + "ti time,"
            + "tis timestamp,"
            + "dti  datetime,"
            // + "c char ascii,"
            // + "c2 char unicode,"
            + "vc varchar(100) binary,"
            + "vc2 varchar(100) character set utf8mb4 collate utf8mb4_bin not null,"
            + "tb tinyblob,"
            + "bl blob,"
            + " mb mediumblob,"
            + "lb longblob,"
            + "tt tinytext,"
            + "mt mediumtext,"
            + "lt longtext,"
            + "en enum(\"1\",\"2\"),"
            + "st set(\"5\",\"6\"),"
            + "id1 int,"
            + "id2 int,"
            + "id3 varchar(100),"
            + "vc1 varchar(100),"
            + "vc3 varchar(100),"
            + "index idx1 using btree(id1),"
            + "key idx2 using btree(id2),"
            + "FULLTEXT KEY idx4(id3(20)),"
            + "constraint c1 unique key idx3 using btree(vc1(20))  "
            + ")engine=InnoDB auto_increment=2 avg_row_length=100 default character set utf8mb4 collate utf8mb4_bin checksum=0 comment=\"abcd\" dbpartition by hash(id1) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id1`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableWithDbName() {
        String simpleTableName = testTableName + "_4";
        String tableName = schemaPrefix + simpleTableName;
        if (StringUtils.isBlank(tableName)) {
            tableName = tddlDatabase1 + "." + simpleTableName;
        }
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table if not exists %s (id int, name varchar(30), primary key(id)) dbpartition by hash(id) dbpartitions 2",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);

        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);

        runInsertOneForTable(tableName);

        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`id`)"));
        Assert.assertTrue(showCreateTableString.contains("`" + simpleTableName + "`"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTablePartitionWithNotExistDbName() {
        String simpleTableName = testTableName + "_5";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists abc." + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";

        // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown database");
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableWithNotExistDbName() {
        String simpleTableName = testTableName + "_5";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists abc." + tableName + " (id int, name varchar(30), primary key(id))";

        //
        // 目前用户在DRDS输入的所有SQL, 如果表名带了错误的或不存在的dbName( 如, notExistDb.tbl),
        // drds都会忽略DbName而只取它表名来执行SQL
        // 而mysql同步的样的行为应该报Unknown Database 之类的语法错误
        // , 又因为drds执行DDL时，是先推规则，再推执行DDL
        // 这时碰到当ddl的表名带上错误或不存在的表名时，则物理mysql已经报错，但表的规则已经推送
        // 从而showTable查到对应的表名

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown database");
        // 建表不成功但是规则会发布
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableWithAutoIncrement() throws SQLException {
        String simpleTableName = testTableName + "_6";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + "(id INT NOT NULL AUTO_INCREMENT,name CHAR(30) NOT NULL,PRIMARY KEY (id))ENGINE=INNODB AUTO_INCREMENT = 200000 dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "INSERT INTO " + tableName + " (name) VALUES ('dog')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(getLastInsertId(tddlConnection) > 0);
        String createTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            createTableString.contains("AUTO_INCREMENT=200000")
                || createTableString.contains("AUTO_INCREMENT = 200000")
                || createTableString.contains("AUTO_INCREMENT = 200001")
                || createTableString.contains("AUTO_INCREMENT=200001"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    @Ignore("需要改进的case,暂时忽略")
    public void testCreateTableWithAutoIncrementAllType() throws SQLException {
        String simpleTableName = testTableName + "_6";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + "(id INT NOT NULL AUTO_INCREMENT ,name CHAR(30) NOT NULL,PRIMARY KEY (id))ENGINE=INNODB AUTO_INCREMENT = 200000 dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "INSERT INTO " + tableName + " (name) VALUES ('dog')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(getLastInsertId(tddlConnection) > 0);
        String createTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            createTableString.contains("AUTO_INCREMENT=200000")
                || createTableString.contains("AUTO_INCREMENT = 200000")
                || createTableString.contains("AUTO_INCREMENT = 200001")
                || createTableString.contains("AUTO_INCREMENT=200001"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableWithAutoIncrementInMyISAM() throws SQLException {
        String simpleTableName = testTableName + "_7";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + "(grp ENUM('fish','mammal','bird') NOT NULL,id INT NOT NULL AUTO_INCREMENT,name CHAR(30) NOT NULL,PRIMARY KEY (grp,id)) ENGINE=MyISAM AUTO_INCREMENT = 200000 dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "INSERT INTO " + tableName + " (grp,name) VALUES('mammal','dog')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(getLastInsertId(tddlConnection) > 0);
        String createTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            createTableString.contains("AUTO_INCREMENT=200000")
                || createTableString.contains("AUTO_INCREMENT = 200000")
                || createTableString.contains("AUTO_INCREMENT = 200001")
                || createTableString.contains("AUTO_INCREMENT=200001"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateTableWithMultiTableOption() {
        String simpleTableName = testTableName + "_19";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // String sql = "create table if not exists " + tableName + " (id int)"
        // +
        // "DEFAULT CHARACTER SET=utf8 CHECKSUM=0 COLLATE=utf8_bin
        // COMMENT=\"table_option\" "
        // +
        // "CONNECTION=\"default\" DELAY_KEY_WRITE=1 DATA DIRECTORY=\"/tmp\"
        // INDEX DIRECTORY=\"/tmp\" "
        // +
        // "INSERT_METHOD=NO MAX_ROWS=100000 MIN_ROWS=0 PACK_KEYS=default
        // PASSWORD='wenjun866_' "
        // +
        // "dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id)
        // tbpartitions 2";

        /**
         * <pre>
         * 暂时去掉 DATA DIRECTORY 与 INDEX DIRECTORY两个键表选项，
         * 因为不同的回归实验室，可能是不同的系统，路径可能是不一样 ,
         * 例如，window 下就没有 /tmp 的目录
         * , 这里虽然已经去 DATA DIRECTORY 与 INDEX DIRECTORY，但也足够测试建表选项的功能
         * </pre>
         */
        String sql = "create table if not exists " + tableName + " (id int)"
            + "DEFAULT CHARACTER SET=utf8mb4 CHECKSUM=0 COLLATE=utf8mb4_bin COMMENT=\"table_option\" "
            + "CONNECTION=\"default\" DELAY_KEY_WRITE=1 "
            + "INSERT_METHOD=NO MAX_ROWS=100000 MIN_ROWS=0 PACK_KEYS=default PASSWORD='wenjun866_' "
            + "dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testPartitionLessThan0() {
        String simpleTableName = testTableName + "_24";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int) dbpartition by hash(id) dbpartitions -1 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "dbpartitions should > 0");

        sql = "create table " + tableName
            + "(id int) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions -1 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "tbpartitions should > 0");
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testPartitionKeyNotFound() {

        String simpleTableName = testTableName + "_25";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int) dbpartition by hash(id1) dbpartitions 2 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found partition column");

        sql = "create table " + tableName
            + "(id int) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id1) tbpartitions 2 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found partition column");

        sql = "create table " + tableName + "(id int) tbpartition by hash(id1) tbpartitions 2 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found partition column");
    }

    // /**
    // * @TCDescription : 建表的dbName中包含特殊字符 v
    // * @TestStep : 表名带注释，表名带各种特殊字符，空格开头
    // * @ExpectResult :不支持{}组合，不支持中文
    // * @author xiaowen.guoxw
    // * @since 5.1.20
    // */
    // @Test
    // public void testTableNameUnNormal() {
    // String[] unNoramlTableNames = { "<!--abcd-->", "~!@#$%^&*()
    // _+-=[]}{|\\;:\",?/", " $ $$$", "我是中文" };
    // for (String tableName : unNoramlTableNames) {
    // String sql = "create table if not exists `" + tableName
    // + "`(id int) dbpartition by hash(id) dbpartitions 2 tbpartition by
    // hash(id) tbpartitions 2 ";
    //
    // dropTableIfExists(tableName);
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName,
    // tddlConnection);
    // Assert.assertEquals(8, getShardNum(tableName));
    // dropTableIfExists(tableName);
    // }
    // }

    // /**
    // * @TCDescription : 建表的拆分字段包含特殊字符
    // * @TestStep : 拆分字段包含xml注释
    // * @ExpectResult :
    // * @author xiaowen.guoxw
    // * @since 5.1.20
    // */
    // @Test
    // public void testPartitionKeyUnNormal() {
    // String simpleTableName =testTableName + "_26";
    // String[] unNoramlColumnNames = { "<!--c-->", "~!@%^&*() _+-=[]|:?/",
    // "我是中文" };
    // for (String columnName : unNoramlColumnNames) {
    // dropTableIfExists(tableName);
    // String sql = String.format("create table if not exists " + tableName
    // + "(`%s` int) dbpartition by hash(`%s`) dbpartitions 2 tbpartition by
    // hash(`%s`) tbpartitions 2 ",
    // columnName,
    // columnName,
    // columnName);
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName,
    // tddlConnection);
    // Assert.assertTrue(getShardNum(tableName) == 8);
    // }
    //
    // dropTableIfExists(tableName);
    // }

    /**
     * @since 5.1.20
     */
    @Test
    public void testMultiCreateTable() {
        String simpleTb1 = "tb1";
        String simpleTb2 = "tb2";
        String simpleTb3 = "tb3";
        String tb1 = schemaPrefix + simpleTb1;
        String tb2 = schemaPrefix + simpleTb2;
        String tb3 = schemaPrefix + simpleTb3;
        dropTableIfExists(tb1);
        dropTableIfExists(tb2);
        dropTableIfExists(tb3);

        String sql = String.format("create table %s (id int, name varchar(20));"
                + "create table %s (id int, name varchar(20)) dbpartition by hash(id) dbpartitions 2;"
                + "create table %s (id int, name varchar(20)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(name) tbpartitions 2",
            tb1,
            tb2,
            tb3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertSingelRuleExist(tb1, tddlConnection);

        RuleValidator.assertMultiRuleExist(tb2, tddlConnection);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tb3, tddlConnection);
        dropTableIfExists(tb1);
        dropTableIfExists(tb2);
        dropTableIfExists(tb3);

    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testMultiStmtsWithEmptyStmtOrTailingWhiteSpace() {
        String tb1 = schemaPrefix + "test_m1";
        String tb2 = schemaPrefix + "test_m2";
        String tb3 = schemaPrefix + "test_m3";

        String dropSql =
            String.format("drop table if exists %s;drop table if exists %s;;;drop table if exists %s;", tb1, tb2, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        String sql = String.format("create table %s (id int, name varchar(20));\n"
                + "create table %s (id int, name varchar(20)) dbpartition by hash(id) dbpartitions 2;;;\n"
                + "create table %s (id int, name varchar(20)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(name) tbpartitions 2; \n",
            tb1,
            tb2,
            tb3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertSingelRuleExist(tb1, tddlConnection);

        RuleValidator.assertMultiRuleExist(tb2, tddlConnection);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tb3, tddlConnection);

        dropSql = String
            .format("drop table if exists %s;drop table if exists %s;;;drop table if exists %s;  \n", tb1, tb2, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
    }

    /**
     * @since 5.1.21
     */
    @Ignore("现在已经支持")
    @Test
    public void testCreateTableWithTbParWithoutDBPar() {
        String simpleTableName = testTableName + "_27";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, name varchar(20)) tbpartition by hash(id) tbpartitions 4";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "error in your SQL syntax");

        // sql = "create table "
        // + tableName
        // +
        // " (id int, name varchar(20)) dbpartition by hash() tbpartition by
        // hash(id) tbpartitions 4";
        // runTddlUpdateFaild(sql, "Required");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionPattern() {
        String simpleTableName = testTableName + "_33";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by mm(gmt) tbpartitions 12 (dbpartition \"andor_mysql_group_{0}\" (tbpartition \""
            + "ab" + "_{0000}_{000}\"))";

        System.out.print(sql);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        dropTableIfExists(tableName);

    }

    // /**
    // * @TCDescription : 普通类型，显示指定pattern
    // * @TestStep : 分多个库, 多个分表
    // * @ExpectResult : TODO expect results
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testPartitionPatternById() {
    // String simpleTableName =testTableName + "_34";
    // dropTableIfExists(tableName);
    //
    // String sql = String.format(
    // "create table %s(id int, gmt date) dbpartition by hash(id) dbpartitions 2
    // tbpartition by hash(id) tbpartitions 2 (dbpartition \"%s\" (tbpartition
    // \""
    // + "abc" + "_{00}\"))",
    // tableName,
    // groupPattern);
    //
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName,
    // tddlConnection);
    // Assert.assertTrue(getShardNum(tableName) == 8);
    //
    // dropTableIfExists(tableName);
    //
    // }

    // /**
    // * @TCDescription : 普通类型，显示指定pattern
    // * @TestStep : 分多个库, 没有分表
    // * @ExpectResult : TODO expect results
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testPartitionPatternByIdNoTBPartition() {
    // String simpleTableName =testTableName + "_35";
    //
    // dropTableIfExists(tableName);
    //
    // String sql = String.format(
    // "create table %s (id int, gmt date) dbpartition by hash(id) dbpartitions
    // 4 (dbpartition \"%s\")",
    // tableName,
    // groupPattern);
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
    // Assert.assertTrue(getShardNum(tableName) == 4);
    //
    // dropTableIfExists(tableName);
    // }

    // /**
    // * @TCDescription : 普通类型，显示指定pattern,但pattern与实际分库名不一致
    // * @TestStep : 分多个库, 没有分表
    // * @ExpectResult : 报错
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testPartitionPatternWrongById() {
    // String simpleTableName =testTableName + "_36";
    //
    // dropTableIfExists(tableName);
    //
    // String sql = "create table " + tableName
    // + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2
    // (dbpartition \"aa_{0}\")";
    // JdbcUtil.executeUpdateFailed(tddlConnection, sql, "group list is
    // invalid");
    //
    // }

    // /**
    // * @TCDescription :分库，未指定分库键，默认用主键作为分库键
    // * @TestStep :
    // * @ExpectResult :执行成功
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testPartitionDefaultByPK() {
    // String simpleTableName =testTableName + "_46";
    // dropTableIfExists(tableName);
    //
    // String sql = "create table " + tableName + " (id int, gmt date, primary
    // key (id)) dbpartition by hash()";
    //
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
    // Assert.assertTrue(getShardNum(tableName) == 4);
    // dropTableIfExists(tableName);
    //
    // }

    /**
     * @since 5.1.21
     */
    @Test
    public void testTbPartitionDefaultByPK() {
        String simpleTableName = testTableName + "_47";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date, primary key (id)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash()";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "key to empty");
        dropTableIfExists(tableName);
    }

    // /**
    // * @TCDescription :分库分表，未指定分库键，默认用主键作为分库键
    // * @TestStep :
    // * @ExpectResult :语法报错
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testTbPartitionDefaultByPKInDate() {
    // String simpleTableName =testTableName + "_47";
    // dropTableIfExists(tableName);
    //
    // String sql = "create table " + tableName
    // + " (id int, gmt date, primary key (id)) dbpartition by hash(id)
    // dbpartitions 2 tbpartition by week()";
    //
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
    // Assert.assertTrue(getShardNum(tableName) == 4);
    // dropTableIfExists(tableName);
    // }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDBTbPartitionDefaultByPK() {
        String simpleTableName = testTableName + "_47";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName
            + " (id int, gmt date, primary key (id)) dbpartition by hash() tbpartition by hash()";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testTbPartition0() {
        String simpleTableName = testTableName + "_48";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date, primary key (id)) dbpartition by hash() tbpartition by hash(id) tbpartitions 0";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "tbpartitions should > 0");
    }

    // /**
    // * @TCDescription : 有主键的情况下，使用主键为分库键，
    // * @TestStep : TODO Test steps
    // * @ExpectResult : 报错
    // * @author xiaowen.guoxw
    // * @since 5.1.21
    // */
    // @Test
    // public void testDefaultPartitionWithoutPK() {
    // String simpleTableName =testTableName + "_49";
    // dropTableIfExists(tableName);
    //
    // String sql = "create table " + tableName
    // + " (id int, gmt date, primary key(id)) dbpartition by hash() tbpartition
    // by hash(id) tbpartitions 1";
    //
    // JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    // dropTableIfExists(tableName);
    // }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDefaultPartitionWithComplexPK() {
        String simpleTableName = testTableName + "_50";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id1 int, id2 varchar(20), gmt date, primary key (id1, id2)) dbpartition by hash() tbpartition by hash(id1) tbpartitions 1";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can't set dbpartition");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateBroadcastTable() {
        String simpleTableName = testTableName + "_51";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, id2 varchar(20), gmt date) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertBroadCastRuleExist(tableName, tddlConnection);

        String phyTableName = getTbNamePattern(tableName);
        assertShardDbTableExist(phyConnectionList, phyTableName);

        Assert.assertTrue(getShardNum(tableName) == 1);

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateBroadcastLikeTable() {
        String simpleTableName = testTableName + "_51";
        String tableName = schemaPrefix + simpleTableName;
        String tableNewName = tableName + newSuffix;
        dropTableIfExists(tableNewName);
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, id2 varchar(20), gmt date) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertBroadCastRuleExist(tableName, tddlConnection);

        String phyTableName = getTbNamePattern(tableName);

        assertShardDbTableExist(phyConnectionList, phyTableName);
        Assert.assertTrue(getShardNum(tableName) == 1);

        sql = "create table " + tableNewName + " like " + tableName;

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertBroadCastRuleExist(tableNewName, tddlConnection);

        phyTableName = getTbNamePattern(tableNewName);

        assertShardDbTableExist(phyConnectionList, phyTableName);
        Assert.assertTrue(getShardNum(tableNewName) == 1);

        dropTableIfExists(tableNewName);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateBroadcastTableAndShardDb() {
        String simpleTableName = testTableName + "_52";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName
            + " (id int, id2 varchar(20), gmt date) dbpartition by hash(id) dbpartitions 2 broadcast";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "broadcast and sharding are exclusive", "broadcast and dbpartition are exclusive");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testBatchCreateAndDrop() {
        String simpleTableName = testTableName + "_70";
        String tableName = schemaPrefix + simpleTableName;
        String sql = "drop table if exists " + tableName + ";create table " + tableName
            + " (name varchar(20), sum int) dbpartition by hash(sum) dbpartitions 2 ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        assertExistsTable(tableName, tddlConnection);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testBatchCreateTable() {
        String simpleTableName1 = testTableName + "_71";
        String simpleTableName2 = testTableName + "_72";
        String tableName1 = schemaPrefix + simpleTableName1;
        String tableName2 = schemaPrefix + simpleTableName2;
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);

        String sql = "create table "
            + tableName1
            + " (name varchar(20), sum int) dbpartition by hash(sum) dbpartitions 2; create table "
            + tableName2
            + " (name varchar(20), sum int, gmt date) dbpartition by hash(sum) dbpartitions 2 tbpartition by week(gmt) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertMultiRuleExist(tableName1, tddlConnection);
        assertExistsTable(tableName1, tddlConnection);

        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName2, tddlConnection);
        assertExistsTable(tableName2, tddlConnection);

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreateFailed() {
        String simpleTableName = testTableName + "_74";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (PropertiesUtil.enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        sql = "  CREATE TABLE " + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreateFailedShard() {
        String simpleTableName = testTableName + "_75";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB dbpartition by hash(id) dbpartitions 2 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (PropertiesUtil.enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreateFailedShardTb() {
        String simpleTableName = testTableName + "_76";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (PropertiesUtil.enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2 ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreatePartlyFailed() {
        String simpleTableName = testTableName + "_77";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql + " broadcast");
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);

        ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show topology from " + tableName);
        String physicalSimpleTableName = simpleTableName;
        try {
            resultSet.next();
            physicalSimpleTableName = (String) JdbcUtil.getObject(resultSet, 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        this.dropOneDbInMysql(phyConnectionList, physicalSimpleTableName, 2);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql + " dbpartition by hash(id) dbpartitions 2", "");
        assertNotExistsBroadcastTable(tableName, tddlConnection);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
            "exists");
        assertNotExistsBroadcastTable(tableName, tddlConnection);

        // 虽然sql执行会出错,但是表已经建立成功了
        if (PropertiesUtil.enableAsyncDDL) {
            String tempSql = "  CREATE TABLE "
                + physicalSimpleTableName
                + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB";
            JdbcUtil.executeUpdateSuccess(phyConnectionList.get(2), tempSql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "exists");
        }
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);
        assertShardDbTableExist(phyConnectionList, physicalSimpleTableName);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreatePartlyFailedShard() {
        String simpleTableName = testTableName + "_78";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql + "dbpartition by hash(id) dbpartitions 2 ");

        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName, 1);
        this.dropOneDbInMysql(phyConnectionList, physicalTableName, 1);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "exists");
        assertNotExistsTable(tableName, tddlConnection);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql + "dbpartition by hash(id) dbpartitions 2  tbpartition by hash(id) tbpartitions 2",
            "exists");
        assertNotExistsTable(tableName, tddlConnection);

        if (PropertiesUtil.enableAsyncDDL) {
            String tempSql = "  CREATE TABLE "
                + physicalTableName
                + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
            JdbcUtil.executeUpdateSuccess(phyConnectionList.get(1), tempSql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql + "dbpartition by hash(id) dbpartitions 2 ", "");
        }
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);

        assertShardDbTableExist(phyConnectionList, physicalTableName, true, 2);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreatePartlyFailedShardTb() {
        String simpleTableName = testTableName + "_79";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            sql + "dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2");

        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName, "_3", 1);

        this.dropOneDbInMysql(phyConnectionList, physicalTableName, 1);
        assertNotExistsTable(tableName, tddlConnection);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "exists");
        assertNotExistsTable(tableName, tddlConnection);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql + "dbpartition by hash(id) dbpartitions 2 ", "exists");
        assertNotExistsTable(tableName, tddlConnection);

        if (PropertiesUtil.enableAsyncDDL) {
            String tempSql = "  CREATE TABLE "
                + physicalTableName
                + " (`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
            JdbcUtil.executeUpdateSuccess(phyConnectionList.get(1), tempSql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                sql + "dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2",
                "");
        }
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableCreateSuccess() {
        String simpleTableName = testTableName + "_80";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "already exists");

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableOnlyWithTbPartition() {
        String simpleTableName = testTableName + "_81";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB tbpartition by hash(`id`) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMuliRuleOnlyWithTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableWithDbPartitionOne() {
        String simpleTableName = testTableName + "_82";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL, `gmt` date, PRIMARY KEY (`id`)) ENGINE=InnoDB dbpartition by hash(id) dbpartitions 1 tbpartition by week(`gmt`) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMuliRuleOnlyWithTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("dbpartition by hash(`id`) dbpartitions 1"));
        Assert.assertTrue(showCreateTableString.contains("tbpartition by WEEK(`gmt`) tbpartitions 2"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableWithDbPartitionOne2() {
        String simpleTableName = testTableName + "_83";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL, `gmt` date, PRIMARY KEY (`id`)) ENGINE=InnoDB dbpartitions 1 tbpartition by week(`gmt`) tbpartitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertMuliRuleOnlyWithTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 3);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("dbpartitions 1"));
        Assert.assertTrue(showCreateTableString.contains("tbpartition by WEEK(`gmt`) tbpartitions 3"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableWithDbPartitionOneAndTbPartitionOne() {
        String simpleTableName = testTableName + "_84";
        String tableName = schemaPrefix + simpleTableName;
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL, `gmt` date, PRIMARY KEY (`id`)) ENGINE=InnoDB";
        dropTableIfExists(tableName);

        // 只有dbpartitions，且dbpartitions=2
        String runSql = sql + " dbpartitions 1 tbpartition by week(`gmt`) tbpartitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, runSql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 1);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("tbpartitions 1"));
        Assert.assertFalse(showCreateTableString.contains("dbpartition"));

        dropTableIfExists(tableName);
        runSql = sql + " dbpartition by hash(id) dbpartitions 1 tbpartition by week(`gmt`) tbpartitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, runSql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 1);
        showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("tbpartitions 1"));
        Assert.assertFalse(showCreateTableString.contains("dbpartition"));

        dropTableIfExists(tableName);

        runSql = sql + " tbpartition by week(`gmt`) tbpartitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, runSql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        assertExistsTable(tableName, tddlConnection);
        RuleValidator.assertSingelRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 1);
        showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains("tbpartitions 1"));
        Assert.assertFalse(showCreateTableString.contains("dbpartition"));

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCreateTableOnlyWithTbPartitionWrong() {
        String simpleTableName = testTableName + "_83";
        String tableName = schemaPrefix + simpleTableName;
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL, `gmt` date, PRIMARY KEY (`id`)) ENGINE=InnoDB";
        dropTableIfExists(tableName);

        // 只有dbpartitions，且dbpartitions=2
        String runSql = sql + " dbpartitions 3 tbpartition by week(`gmt`) tbpartitions 2";
        JdbcUtil.executeUpdateFailed(tddlConnection, runSql, "Required group count:3 exceed physical group count:2");
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testCreateTableIfNotExistShardTb() {

        String simpleTableName = testTableName + "_84";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table if not exists "
            + tableName
            + " (idd int, name varchar(30),primary key(idd)) dbpartition by hash(idd) dbpartitions 2 tbpartition by hash(idd) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);

        sql = "create table if not exists " + tableName
            + " (idd int, name2 varchar(30),primary key(idd)) dbpartition by hash(idd) dbpartitions 2 ";

        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccessWithWarning(tddlConnection, sql, "Table '" + simpleTableName
                + "' already exists");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Rule already exists");
        }

        sql = "create table if not exists " + tableName + " (idd int, name2 varchar(30),primary key(idd)) ";

        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccessWithWarning(tddlConnection, sql, "Table '" + simpleTableName
                + "' already exists");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Rule already exists");
        }

        sql = "create table if not exists "
            + tableName
            + " (idd int, name2 varchar(30),primary key(idd)) dbpartition by hash(idd) dbpartitions 2 tbpartition by hash(idd) tbpartitions 4";

        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccessWithWarning(tddlConnection, sql, "Table '" + simpleTableName
                + "' already exists");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Rule already exists");
        }

        sql = "create table if not exists " + tableName
            + " (idd int, name2 varchar(30),primary key(idd)) tbpartition by hash(idd) tbpartitions 4";

        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccessWithWarning(tddlConnection, sql, "Table '" + simpleTableName
                + "' already exists");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Rule already exists");
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDdlWhenNoRuleExist() {
        String simpleTableName = testTableName + "_91";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int, name varchar(20)) ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
//        Assert.assertEquals(1, getExplainNum(sql));

        sql = "alter table " + tableName + " add index name_index(name) ";
//        Assert.assertEquals(1, getExplainNum(sql));

        sql = "create index id_index on " + tableName + "(id)";
//        Assert.assertEquals(1, getExplainNum(sql));

        sql = "drop index id_index on " + tableName;
//        Assert.assertEquals(1, getExplainNum(sql));

        sql = "drop table " + tableName;
//        Assert.assertEquals(1, getExplainNum(sql));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.21
     */
    public void testCreateSingleTable() {
        String simpleTableName = testTableName + "_91";
        String tableName = schemaPrefix + simpleTableName;
        String sql = "create table " + tableName + "(id int, name varchar(20)) ";
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertShardDbTableExist(phyConnectionList, tableName);

        sql = "alter table " + tableName + " add index name_index(name) ";
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "index_name"));

        sql = "create index id_index on " + tableName + "(id)";
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "index_id"));

        sql = "drop index id_index on " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "index_id"));

        sql = "drop table " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertShardDbTableNotExist(phyConnectionList, tableName);

    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testShardKeyAutoIndexSingle() {
        String simpleTableName = testTableName + "_86";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // 创建单表，不会自动创建任何索引
        String sql = "create table " + tableName + " (id  int, name varchar(20), gmt date)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, ""));
        dropTableIfExists(tableName);

        // 创建广播表，不会自动创建任何索引
        sql = "create table " + tableName + " (id  int, name varchar(20), gmt date) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, ""));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testShardKeyAutoIndexShardTb() {
        String simpleTableName = testTableName + "_86";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // 创建单库分表, int型拆分键
        String sql = "create table " + tableName
            + " (id  int, name varchar(20), gmt date) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // 创建单库分表，varchar型拆分键
        sql = "create table " + tableName
            + " (id  int, name varchar(20), gmt date) tbpartition by hash(name) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_name"));
        dropTableIfExists(tableName);

        // 创建单库分表，text型拆分键，注意不会自动建索引
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), descrip text, pass varbinary(10), gmt date) tbpartition by hash(descrip) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_descrip"));
        dropTableIfExists(tableName);

        // 创建单库分表，varbinary型拆分键
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), descrip text, pass varbinary(10), gmt date) tbpartition by hash(pass) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_pass"));
        dropTableIfExists(tableName);

        // 创建单库分表，date型拆分键
        sql = "create table " + tableName
            + " (id  int, name varchar(20), gmt date) tbpartition by week(gmt) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_gmt"));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testShardKeyAutoIndexShardDb() {
        String simpleTableName = testTableName + "_86";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // 创建分库单表, int型拆分键
        String sql = "create table " + tableName + " (id  int, name varchar(20), gmt date) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // 创建分库单表，varchar型拆分键
        sql = "create table " + tableName + " (id  int, name varchar(20), gmt date) dbpartition by hash(name)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_name"));
        dropTableIfExists(tableName);

        // 创建分库单表，text型拆分键，注意，不会自动建索引
        sql = "create table " + tableName
            + " (id  int, name varchar(20), descrip text, gmt date) dbpartition by hash(descrip)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_descrip"));
        dropTableIfExists(tableName);

        // 创建分库单表，varbianry型拆分键
        sql = "create table " + tableName
            + " (id  int, name varchar(20), descrip text, pass varbinary(10), gmt date) dbpartition by hash(pass)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getIndexNumInAllDb(tableName, "auto_shard_key_pass"));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testShardKeyAutoIndexShardDbTb() {
        String simpleTableName = testTableName + "_86";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // 创建分库分表，int型为拆分键
        String sql = "create table "
            + tableName
            + " (id  int, name varchar(20), gmt date) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // 创建分库分表，varchar型为拆分键
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), gmt date) dbpartition by hash(name) tbpartition by hash(name) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "auto_shard_key_name"));
        dropTableIfExists(tableName);

        // 创建分库分表，text型为拆分键, 注意，这种类型的拆分键不会自动键索引
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), descrip text, pass varbinary(10), gmt date) dbpartition by hash(descrip) tbpartition by hash(descrip) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_descrip"));
        dropTableIfExists(tableName);

        // 创建分库分表，varbinary型为拆分键
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), descrip text, pass varbinary(10), gmt date) dbpartition by hash(pass) tbpartition by hash(pass) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "auto_shard_key_pass"));
        dropTableIfExists(tableName);

        // 创建分库分表，日期与int为拆分键
        sql = "create table "
            + tableName
            + " (id  int, name varchar(20), gmt date) dbpartition by hash(name) tbpartition by week(gmt) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "auto_shard_key_gmt"));
        Assert.assertEquals(4, getIndexNumInAllDb(tableName, "auto_shard_key_name"));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.21
     */
    @Test
    @Ignore("Only single table can create foreign keys or be referenced as foreign keys in PolarDB-X 1.0")
    public void testShardKeyIndexExistsForeginKey() {

        String simpleTableName = testTableName + "_85_20170728";
        String tableName = schemaPrefix + simpleTableName;
        String parentTableName = schemaPrefix + "parent";
        dropTableIfExists(tableName);
        clearForeignTable(parentTableName);

        String sql = "CREATE TABLE if not exists " + parentTableName
            + "(id INT NOT NULL, id1 int, PRIMARY KEY (id, id1)) ENGINE=INNODB broadcast";

        // 创建parent表
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 分库不分表
        sql = "create table if not exists "
            + tableName
            + "(id INT, parent_id INT,FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE) ENGINE=INNODB dbpartition by hash(parent_id)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support foreign key");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testShardKeyExistUniqueKey() {
        String simpleTableName = testTableName + "_88";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // unique key为拆分键，不会自动创建索引
        String sql = "create table " + tableName + " (id int unique key) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // unique key为拆分键，不会自动创建索引
        sql = "create table " + tableName + " (id int unique) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // unique key为拆分字段
        sql = "create table " + tableName
            + " (id int, name varchar(20),  unique key unique_id using hash(id))dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testShardKeyExistNormalKey() {
        String simpleTableName = testTableName + "_89";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        // key为拆分键，不会自动创建索引
        String sql = "create table " + tableName + " (id int, key(id)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        dropTableIfExists(tableName);

        // key为拆分键，不会自动创建索引
        sql = "create table " + tableName + " (id int,  index index_id (id)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_id"));
        dropTableIfExists(tableName);

        // key为拆分字段
        sql = "create table " + tableName
            + " (id int, name varchar(20),  fulltext index unique_name(name))dbpartition by hash(name)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_name", tddlConnection));
        Assert.assertEquals(0, getIndexNumInAllDb(tableName, "auto_shard_key_name"));
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testShardKeyTooLong() {
        String simpleTableName = testTableName + "_89";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id varchar(256), name varchar(20)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        //Assert.assertTrue(showCreateTableString.contains("255"));
        dropTableIfExists(tableName);

        sql = "create table " + tableName
            + " (id varchar(256), name varchar(20)) dbpartition by hash(name) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        showCreateTableString = showCreateTable(tddlConnection, tableName);
        //Assert.assertTrue(showCreateTableString.contains("255"));
        dropTableIfExists(tableName);

        sql = "create table " + tableName
            + " (id varchar(256), name varchar(20)) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isIndexExist(tableName, "auto_shard_key_id", tddlConnection));
        showCreateTableString = showCreateTable(tddlConnection, tableName);
        //Assert.assertTrue(showCreateTableString.contains("255"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testCreateSameTable() {
        String simpleTableName = testTableName + "_90";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int, name varchar(20))";
        // 拼接在sql后面，用于产生各种各样的分表
        String[] subSqls = {
            "", "broadcast", "dbpartition by hash(id)", "tbpartition by hash(id) tbpartitions 2",
            "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2"};

        String testSql = "";
        for (String subSql : subSqls) {
            testSql = sql + subSql;
            // 先创建一个表
            JdbcUtil.executeUpdateSuccess(tddlConnection, testSql);
            for (String subSql2 : subSqls) {
                // 然后创建同名表，同名表的采用各种各样的拓扑结构
                testSql = sql + subSql2;

                if (PropertiesUtil.enableAsyncDDL) {
                    JdbcUtil.executeUpdateFailed(tddlConnection, testSql, "Table '" + simpleTableName
                        + "' already exists");
                } else {
                    if (subSql.equals(subSql2)) {
                        // 如果表结构完全相同，则不会报错
                        JdbcUtil.executeUpdateFailed(tddlConnection, testSql, "exists");
                    } else {
                        JdbcUtil.executeUpdateFailed(tddlConnection, testSql, "Rule already exists");
                    }
                }

            }
            dropTableIfExists(tableName);
        }
    }

    /**
     * @since 5.1.28-SNAPSHOT
     */
    @Test
    public void testCreateGeomTable() {
        String simpleTableName = testTableName + "_91";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String[] partitions = {
            "", "broadcast", "dbpartition by hash(id)",
            "dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2",
            "tbpartition by hash(id) tbpartitions 2"};
        for (String partition : partitions) {
            // String sql = String
            // .format("create table %s (id int not null auto_increment primary
            // key, geom geometry, pot point, line linestring, poly polygon,
            // multipot multipoint, multiline multilinestring, multipoly
            // multipolygon, geomlist geometrycollection) %s",
            // tableName, partition);
            String sql = String.format("create table %s (id int not null auto_increment primary key, geom geometry) %s",
                tableName,
                partition);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
            RuleValidator.assertRuleExist(tableName, sql, tddlConnection);

            dropTableIfExists(tableName);
        }

    }

    /**
     * @since 5.1.28-SNAPSHOT
     */
    @Test
    public void testShardKeyIsGeom() {
        String simpleTableName = testTableName + "_92";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String[] geomTypes = {
            "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring",
            "multipolygon", "geometrycollection"};
        for (String geomtype : geomTypes) {
            String sql = String.format("create table %s (id %s) dbpartition by hash(id)", tableName, geomtype);
            dropTableIfExists(tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Invalid type for a sharding key");
            RuleValidator.assertRuleNotExist(tableName, tddlConnection);
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        }
    }

    /**
     * @since 5.1.28-SNAPSHOT
     */
    @Test
    public void testShardKeyWithQuato() {
        String simpleTableName = testTableName + "_93";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table  %s(id int, name varchar(20)) dbpartition by hash(`id`) tbpartition by hash(`id`) tbpartitions 2",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.26
     */
    // @Test
    public void testCreateTableWithCaseSensitiveKeys() {
        String simpleTableName = testTableName + "_94";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` int NOT NULL, `consCode` int, flag int, PRIMARY KEY (`id`)) ENGINE=InnoDB dbpartition by hash(consCode) tbpartition by hash(conscode) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        isShowTableExist(tableName, tddlDatabase2);
        assertExistsTable(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 8);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains("dbpartition by hash(`consCode`)"));
        Assert.assertTrue(showCreateTableString.contains("tbpartition by hash(`conscode`) tbpartitions 2"));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    private void runInsertOneForTable(String tableName) {
        String sql = "insert into " + tableName + " values(1, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName + " where id > 0";
        boolean id = false;
        boolean name = false;
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                if (rs.getInt("id") == 1) {
                    id = true;
                }
                if (rs.getString("name").equals("simiao_test")) {
                }
                {
                    name = true;
                }
            }
            Assert.assertTrue(id);
            Assert.assertTrue(name);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "delete from " + tableName + " where id > 0";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * @since 5.1.21
     */
    private void runInsertOneForTableWithKeyName(String tableName, String keyName) {
        String sql = "insert into " + tableName + " values(1, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName + " where " + keyName + " > 0";
        boolean id = false;
        boolean name = false;
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                String pureKeyName = keyName;
                if (keyName.startsWith("`")) {
                    pureKeyName = keyName.substring(1, keyName.length() - 1);
                }
                if (rs.getInt(pureKeyName) == 1) {
                    id = true;
                }
                if (rs.getString("name").equals("simiao_test")) {
                }
                {
                    name = true;
                }
            }
            Assert.assertTrue(id);
            Assert.assertTrue(name);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "delete from " + tableName + " where " + keyName + " > 0";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void createTableTimestampNullDefaultNullCheck() {
        String tableName = schemaPrefix + "wcm_test1";
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + tableName);
        String sql = "CREATE TABLE " + tableName + " (" + "  `order_seq` bigint(22) NOT NULL COMMENT '订单编号',"
            + "  `create_time` timestamp(3) NULL DEFAULT NULL COMMENT '创建订单时间',"
            + "  PRIMARY KEY (`order_seq`)" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * @since 5.1.25
     */
    @Test
    public void testShardKeyInMultiColumnIndex() {
        String simpleTableName = testTableName + "_95";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName
            + " (id int, name varchar(10), age int, primary key (id), key name_index (name, age)) "
            + "dbpartition by hash(name) tbpartition by hash(age) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, "name_index", tddlConnection));
        Assert.assertTrue(isIndexExist(tableName, "auto_shard_key_age", tddlConnection));
        Assert.assertFalse(isIndexExist(tableName, "auto_shard_key_name", tddlConnection));

        dropTableIfExists(tableName);
    }

    @Test
    public void testShardKeyCreateTableIfExists() {
        String simpleTableName = testTableName + "_96";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  `id` BIGINT UNSIGNED NOT NULL  AUTO_INCREMENT,\n"
            + "  `name` VARCHAR(255) NULL,\n"
            + "  PRIMARY KEY (id)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by HASH(id) tbpartition by HASH(id) tbpartitions 2;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } catch (Exception e) {
            Assert.fail("error when create table");
        }

        dropTableIfExists(tableName);
    }

    @Test
    public void testRangeHashCreateTable() {
        String tableName = schemaPrefix + testTableName + "_97";
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  `id` BIGINT  NOT NULL  AUTO_INCREMENT,\n"
            + "  `id1` BIGINT  NOT NULL,\n"
            + "  `name` VARCHAR(255) NULL,\n"
            + "  PRIMARY KEY (id)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by RANGE_HASH(id, id1, 3) tbpartition by RANGE_HASH(id, id1, 3) tbpartitions 10;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values(1000, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1001, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1002, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1003, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1004, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1005, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1006, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1007, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1008, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1009, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName + " where id =1006";
        int count = 0;
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue(count == 1);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        dropTableIfExists(tableName);
    }

    @Test
    public void testRangeHash1CreateTable() {
        String tableName = schemaPrefix + testTableName + "_98";
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  `id` BIGINT NOT NULL  AUTO_INCREMENT,\n"
            + "  `id1` BIGINT  NOT NULL,\n"
            + "  `name` VARCHAR(255) NULL,\n"
            + "  PRIMARY KEY (id)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by RANGE_HASH1(id, id1, 3) tbpartition by RANGE_HASH1(id, id1, 3) tbpartitions 10;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values(1000, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1001, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1002, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1003, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1004, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1005, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1006, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1007, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1008, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values(1009, 1000, 'simiao_test')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName + " where id > 0";
        int count = 0;
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue(count == 10);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        dropTableIfExists(tableName);
    }

    @Test
    public void testRangeHash1ShowCreateTable() {
        String tableName = schemaPrefix + testTableName + "_99";
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  `id` BIGINT NOT NULL  AUTO_INCREMENT,\n"
            + "  `id1` BIGINT NOT NULL,\n"
            + "  `name` VARCHAR(255) NULL,\n"
            + "  PRIMARY KEY (id)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by RANGE_HASH1(id, id1, 3) tbpartition by RANGE_HASH1(id, id1, 3) tbpartitions 10;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "show create table " + tableName + " ";
        try {
            String showCreateInfo = "";
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                showCreateInfo += rs.getString(2);
            }
            System.out.println(showCreateInfo);
            Assert.assertTrue(showCreateInfo.toUpperCase()
                .contains(
                    "dbpartition by RANGE_HASH1(`id`, `id1`, 3) tbpartition by RANGE_HASH1(`id`, `id1`, 3) tbpartitions 10"
                        .toUpperCase()));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        dropTableIfExists(tableName);
    }

    /**
     * Collate may contacted whit default expr in column definition. It is hard
     * to fix this because many other products need this feature. So fix it in
     * fastSqlToCalcite.
     */
    @Test
    public void testDefaultAndCollate() {
        String tableName = schemaPrefix + testTableName + "_100";
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
            + "`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n" + "`author_id` INT(10) not NULL DEFAULT 0,\n"
            + "`platform` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',\n"
            + " PRIMARY KEY (`id`),\n" + " INDEX `IDX_AUTHOR_ID_PLATFORM` (`author_id`, `platform`)\n"
            + ") COLLATE='utf8_unicode_ci' ENGINE=InnoDB dbpartition by hash(author_id);";
        String sql_8 = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
            + "`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "`author_id` INT(10) not NULL DEFAULT 0,\n"
            + "`platform` VARCHAR(255) NULL DEFAULT NULL CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
            + " PRIMARY KEY (`id`),\n" + " INDEX `IDX_AUTHOR_ID_PLATFORM` (`author_id`, `platform`)\n"
            + ") CHARSET=utf8mb4 COLLATE=utf8mb4_bin ENGINE=InnoDB dbpartition by hash(author_id);";
        JdbcUtil.executeUpdateSuccess(tddlConnection, isMySQL80() ? sql_8 : sql);

        sql = "show create table " + tableName + " ";
        try {
            String showCreateInfo = "";
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                showCreateInfo += rs.getString(2);
            }
            System.out.println(showCreateInfo);
            Assert.assertTrue(showCreateInfo.toUpperCase()
                .contains(isMySQL80() ?
                    "`platform` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL".toUpperCase() :
                    "`platform` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL".toUpperCase()));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        dropTableIfExists(tableName);
    }

    @Test
    public void testTableNameContainingDot() {
        String tableName = schemaPrefix + "`" + testTableName + "_101.1.0`";

        dropTableIfExists(tableName);

        String alterSql = "ALTER TABLE " + tableName + " add c3 int";

        String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (c1 int, c2 int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (c1 int, c2 int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (c1 int, c2 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (c1 int, c2 int) dbpartition by hash(c1)"
            + " tbpartition by hash(c1) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithMysqlPartition() {

        String tableName = schemaPrefix + testTableName + "_mp";
        dropTableIfExists(tableName);
        String sql = "CREATE TABLE if not exists " + tableName
            + "(col1 INT, col2 CHAR(5), col3 DATETIME)PARTITION BY HASH ( YEAR(col3) )"
            + " dbpartition by hash(col1) dbpartitions 2 tbpartition by hash(col1) tbpartitions 2";
        JdbcUtil
            .executeUpdateFailed(tddlConnection, sql, "Do not support mix dbpartition with range/list/hash partition");
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableLike() {
        String origTableName = schemaPrefix + testTableName;
        String tableName = origTableName + "_like";

        dropTableIfExists(origTableName);
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE if not exists " + origTableName + " (c1 int, c2 int) dbpartition by hash(c1) "
            + "tbpartition by hash(c1) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "CREATE TABLE if not exists " + tableName + " like " + origTableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(origTableName);
        dropTableIfExists(tableName);
    }

//    @Test
//    public void testCreateTableSelect() {
//
//        String tableName = schemaPrefix + testTableName + "_sel";
//        dropTableIfExists(tableName);
//        String sql = "CREATE TABLE if not exists " + tableName + " as select * from " + testTableName;
//        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support create table select");
//        dropTableIfExists(tableName);
//    }

    private void testTable(String tableName, String colName) {
        dropTableIfExists(tableName);

        String insert = "insert into " + tableName + " (" + colName + ",c2) values (1,2)";
        String sel = "select " + colName + " from " + tableName;
        String alterSql = "ALTER TABLE " + tableName + " add c3 int";

        String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql =
            "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName
                + " int, c2 int) dbpartition by hash(" + colName + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql =
            "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int) dbpartition by hash("
                + colName + ")"
                + " tbpartition by hash(" + colName + ") tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);
    }

    @Test
    public void testDuplicateColumn() {
        String tableName = schemaPrefix + testTableName + "_dupcol";
        dropTableIfExists(tableName);

        String sql =
            "CREATE TABLE if not exists " + tableName + " (c1 int not null primary key, c2 int, c2 int, c3 int)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'c2'");

        sql = "CREATE TABLE if not exists " + tableName + " (c1 int not null primary key, c2 int, C2 int, c3 int)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'C2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, c2 int, c3 int) broadcast";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'c2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, C2 int, c3 int) broadcast";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'C2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, c2 int, c3 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'c2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, C2 int, c3 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'C2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, c2 int, c3 int) dbpartition by hash(c1) tbpartition by hash(c2) tbpartitions 2";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'c2'");

        sql = "CREATE TABLE if not exists " + tableName
            + " (c1 int not null primary key, c2 int, C2 int, c3 int) dbpartition by hash(c1) tbpartition by hash(c2) tbpartitions 2";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'C2'");

        dropTableIfExists(tableName);
    }

    @Test
    public void testNoColumn() {
        String tableName = schemaPrefix + testTableName + "_nocol";
        dropTableIfExists(tableName);

        String sql = "CREATE TABLE if not exists " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "A table must have at least 1 column");

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithTestTablePrefix() {
        String hint = "/* //1/ */";
        String testTablePrefix = "__test_";
        String tableName = schemaPrefix + testTableName + "_102";
        String tableNameWithPrefix = schemaPrefix + testTablePrefix + testTableName + "_102";
        dropTableIfExists(tableNameWithPrefix);

        String sql = hint + "CREATE TABLE if not exists " + tableName + " (id int,name varchar(30),primary key(id))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        showCreateTable(tddlConnection, tableNameWithPrefix);

        String likeTableName = schemaPrefix + testTableName + "_103";
        String likeTableNameWithPrefix = schemaPrefix + testTablePrefix + testTableName + "_103";
        dropTableIfExists(likeTableNameWithPrefix);
        sql = hint + "CREATE TABLE if not exists " + likeTableName + " like " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        showCreateTable(tddlConnection, likeTableNameWithPrefix);

        dropTableIfExists(tableNameWithPrefix);
        dropTableIfExists(likeTableNameWithPrefix);
    }

    @Test
    public void testCreateTableLikeWithGsiBinaryDefaultValue() throws Exception {
        String tableName = testTableName + "_105";
        String gsiName = testTableName + "_gsi_105";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTable = String.format("create table %s ("
            + "`pk` int primary key auto_increment, "
            + "`bin_col` varbinary(20) default x'0A08080E10011894AB0E', "
            + "`pad` varchar(20) default 'ggg' "
            + ")", tableName);
        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Use upsert to test default value on CN
        String upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", tableName);
        // Use insert to test default value on DN
        String insert = String.format("insert into %s(`pk`) values (null)", tableName);
        String select = String.format("select `bin_col` from %s", tableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        String likeTableName = testTableName + "_106";
        String likeGsiName = testTableName + "_gsi_106";

        dropTableIfExists(likeTableName);
        dropTableIfExistsInMySql(likeTableName);

        String createTableLike = String.format("create table %s like %s", likeTableName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableLike);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableLike);
        String createGsiLike =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", likeGsiName, likeTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiLike);

        // Use upsert to test default value on CN
        upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", likeTableName);
        // Use insert to test default value on DN
        insert = String.format("insert into %s(`pk`) values (null)", likeTableName);
        select = String.format("select `bin_col` from %s", likeTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCreateTableBitDefaultValue() throws Exception {
        String tableName = testTableName + "_107";
        String gsiName = testTableName + "_gsi_107";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTable = String.format("create table %s ("
            + "`pk` int primary key auto_increment, "
            + "`b1_col` bit(1) default b'0', "
            + "`b2_col` bit(3) default b'101', "
            + "`pad` varchar(20) default 'ggg' "
            + ")", tableName);
        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Use upsert to test default value on CN
        String upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", tableName);
        // Use insert to test default value on DN
        String insert = String.format("insert into %s(`pk`) values (null)", tableName);
        String select = String.format("select `b1_col`, `b2_col` from %s", tableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        String likeTableName = testTableName + "_108";
        String likeGsiName = testTableName + "_gsi_108";

        dropTableIfExists(likeTableName);
        dropTableIfExistsInMySql(likeTableName);

        String createTableLike = String.format("create table %s like %s", likeTableName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableLike);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableLike);
        String createGsiLike =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", likeGsiName, likeTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiLike);

        // Use upsert to test default value on CN
        upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", likeTableName);
        // Use insert to test default value on DN
        insert = String.format("insert into %s(`pk`) values (null)", likeTableName);
        select = String.format("select `b1_col`, `b2_col` from %s", likeTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCreateTableWithRowFormatAndCollate() {
        final String dropSql = "drop table if exists testCreateTableWithRowFormatAndCollate";
        final String sql = "create table if not exists `testCreateTableWithRowFormatAndCollate` "
            + "( `id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`warehouseCode` varchar(50) NOT NULL, "
            + "`code` varchar(50) NOT NULL, "
            + "PRIMARY KEY USING BTREE (`id`) "
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 ROW_FORMAT = DYNAMIC COLLATE `utf8_general_ci`";

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
    }
}
