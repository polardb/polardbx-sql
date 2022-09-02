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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.RuleValidator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiaowen.guoxw on 16-1-20. 测试建立按照日期分库的
 */

public class CreateTableWithDateTest extends DDLBaseNewDBTestCase {

    private String testTableName = "date_test";
    private String testCHNTableName = "中文表名测试带日期";
    final static private String hint = "/*+TDDL: cmd_extra(MAX_TABLE_PARTITIONS_PER_DB=367)*/";

    public CreateTableWithDateTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{true}});
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByWeek() {
        String tableName = schemaPrefix + testTableName + "_28";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by week(gmt) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 14);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by WEEK(`gmt`) tbpartitions 7"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByDD() {
        String tableName = schemaPrefix + testTableName + "_29";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by dd(gmt) tbpartitions 31";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 62);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by DD(`gmt`) tbpartitions 31"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMM() {
        String tableName = schemaPrefix + testTableName + "_36";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by mm(gmt) tbpartitions 12";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by MM(`gmt`) tbpartitions 12"));
        Assert.assertTrue(getShardNum(tableName) == 24);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMMDD() {
        String tableName = schemaPrefix + testTableName + "_351";
        dropTableIfExists(tableName);

        String sql = hint + "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by mmdd(gmt) tbpartitions 366";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == (1464 / 2));
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by MMDD(`gmt`) tbpartitions 366"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDbPartitionByDate() {
        String tableName = schemaPrefix + testTableName + "_32";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by mm(gmt) dbpartitions 2 tbpartition by mm(gmt) tbpartitions 12";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Not support dbpartition");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByWeekLessThan7() {
        String tableName = schemaPrefix + testTableName + "_37";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by week(gmt) tbpartitions 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByWeekLargeThan7() {
        String tableName = schemaPrefix + testTableName + "_38";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by week(gmt) tbpartitions 8";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "mismatch the actual generated number");

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMMLessThan12() {
        String tableName = schemaPrefix + testTableName + "_39";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by MM(gmt) tbpartitions 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMMLargeThan12() {
        String tableName = schemaPrefix + testTableName + "_40";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by MM(gmt) tbpartitions 13";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "mismatch the actual generated number");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMMDDLessThan31() {
        String tableName = schemaPrefix + testTableName + "_41";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by MMDD(gmt) tbpartitions 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByMMDDLargeThan31() {
        String tableName = schemaPrefix + testTableName + "_42";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by MMDD(gmt) tbpartitions 32";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByDDLessThan366() {
        String tableName = schemaPrefix + testTableName + "_43";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by DD(gmt) tbpartitions 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 2);
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByDDLargeThan366() {
        String tableName = schemaPrefix + testTableName + "_44";
        dropTableIfExists(tableName);

        String sql = hint + "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by DD(gmt) tbpartitions 367";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "mismatch the actual generated number");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testPartitionByIdWithDD() {
        String tableName = schemaPrefix + testTableName + "_45";
        dropTableIfExists(tableName);

        String sql = hint + "create table "
            + tableName
            + " (id int, gmt date) dbpartition by hash(id) dbpartitions 2 tbpartition by DD(id) tbpartitions 367";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "only support hash method");
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCHNPartitionByWeek() {
        String tableName = schemaPrefix + testCHNTableName + "_1";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (中文列名 int, 中文日期拆分字段 date) dbpartition by hash(中文列名) dbpartitions 2 tbpartition by week(中文日期拆分字段) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 14);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("dbpartition by hash(`中文列名`) tbpartition by WEEK(`中文日期拆分字段`) tbpartitions 7"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCHNPartitionByDD() {
        String tableName = schemaPrefix + testCHNTableName + "_29";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (中文列名 int, 中文日期拆分字段 date)  dbpartition by hash(中文列名) dbpartitions 2 tbpartition by dd(中文日期拆分字段) tbpartitions 31";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        Assert.assertTrue(getShardNum(tableName) == 62);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("dbpartition by hash(`中文列名`) tbpartition by DD(`中文日期拆分字段`) tbpartitions 31"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCHNPartitionByMM() {
        String tableName = schemaPrefix + testTableName + "_31";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (中文列名 int, 中文日期拆分字段 date) dbpartition by hash(中文列名) dbpartitions 2 tbpartition by mm(中文日期拆分字段) tbpartitions 12";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("dbpartition by hash(`中文列名`) tbpartition by MM(`中文日期拆分字段`) tbpartitions 12"));
        Assert.assertTrue(getShardNum(tableName) == 24);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testCHNPartitionByMMDD() {
        String tableName = schemaPrefix + testTableName + "_37";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (中文列名 int, 中文日期拆分字段 date) dbpartition by hash(中文列名) dbpartitions 2 tbpartition by mmdd(中文日期拆分字段) tbpartitions 12";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("dbpartition by hash(`中文列名`) tbpartition by MMDD(`中文日期拆分字段`) tbpartitions 12"));
        Assert.assertTrue(getShardNum(tableName) == 24);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void testCreateTableWithTimestampOfFsp() {
        String tableName = schemaPrefix + testTableName + "_fsp";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int not null auto_increment, create_time TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '', PRIMARY KEY (`id`) ) ENGINE=InnoDB;";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(
            "`create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void testCreateTableWithTimestampOfEmptyFsp() {
        String tableName = schemaPrefix + testTableName + "_empty_fsp";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int not null auto_increment, create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP() COMMENT '', PRIMARY KEY (`id`) ) ENGINE=InnoDB;";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(showCreateTableString
            .contains("`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testTimeZoneShardByMM() {
        String tableName = schemaPrefix + testTableName + "_timezone";
        String tableName2 = tableName + "2";
        dropTableIfExists(tableName);

        String sql = "create table if not exists "
            + tableName
            + " (id int, gmt datetime, gmt2 timestamp) dbpartition by hash(id) dbpartitions 2 tbpartition by mm(gmt) tbpartitions 12";

        String sql2 = "create table if not exists "
            + tableName2
            + " (id int, gmt datetime, gmt2 timestamp) dbpartition by hash(id) dbpartitions 2 tbpartition by mm(gmt2) tbpartitions 12";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName, tddlConnection);
        String showCreateTableString = showCreateTable(tddlConnection, tableName);
        Assert.assertTrue(
            showCreateTableString.contains("dbpartition by hash(`id`) tbpartition by MM(`gmt`) tbpartitions 12"));
        Assert.assertTrue(getShardNum(tableName) == 24);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        RuleValidator.assertMultiRuleWithBothDbAndTbExist(tableName2, tddlConnection);
        String showCreateTableString2 = showCreateTable(tddlConnection, tableName2);
        Assert.assertTrue(
            showCreateTableString2.contains("dbpartition by hash(`id`) tbpartition by MM(`gmt2`) tbpartitions 12"));
        Assert.assertTrue(getShardNum(tableName2) == 24);

        String oldTimeZone = "SYSTEM";
        String timeZone1 = "+08:00";
        String timeZone2 = "+00:00";
        String timeZoneSql = "show variables like 'time_zone';";

        String explainSql =
            String.format("explain select id, gmt, gmt2 from %s where id=1 and gmt='2020-05-31 16:00:53'", tableName);
        String explainSql2 =
            String.format("explain select id, gmt, gmt2 from %s where id=1 and gmt2='2020-05-31 16:00:53'", tableName2);

        try {

            ResultSet rs = JdbcUtil.executeQuery(timeZoneSql, tddlConnection);
            rs.next();
            oldTimeZone = rs.getString("Value");
            rs.close();

            // set time_zone='+00:00'
            JdbcUtil.executeSuccess(tddlConnection, String.format("set time_zone='%s';", timeZone1));

            rs = JdbcUtil.executeQuery(explainSql, tddlConnection);
            rs.next();
            String explainRs = rs.getString(1);
            rs.close();

            rs = JdbcUtil.executeQuery(explainSql2, tddlConnection);
            rs.next();
            String explainRs2 = rs.getString(1);
            rs.close();

            Assert.assertTrue(explainRs.contains("_05"));
            Assert.assertTrue(explainRs2.contains("_05"));

            // set time_zone='+08:00'
            JdbcUtil.executeSuccess(tddlConnection, String.format("set time_zone='%s';", timeZone2));

            rs = JdbcUtil.executeQuery(explainSql, tddlConnection);
            rs.next();
            explainRs = rs.getString(1);
            rs.close();

            rs = JdbcUtil.executeQuery(explainSql2, tddlConnection);
            rs.next();
            explainRs2 = rs.getString(1);
            rs.close();

            Assert.assertTrue(explainRs.contains("_05"));
            Assert.assertTrue(explainRs2.contains("_06"));

        } catch (Throwable ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, String.format("set time_zone='%s';", oldTimeZone));
        }

        dropTableIfExists(tableName);
        dropTableIfExists(tableName2);
    }
}
