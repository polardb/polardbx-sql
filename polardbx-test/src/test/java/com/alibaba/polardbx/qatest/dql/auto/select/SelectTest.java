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

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.usePrepare;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.resultsSize;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 简单查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

@RunWith(Enclosed.class)
@NotThreadSafe
public class SelectTest {

    @NotThreadSafe
    public static class SelectTest1 extends AutoCrudBasedLockTestCase {

        ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

        @Parameters(name = "{index}:table={0}")
        public static List<String[]> prepare() {
            return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
        }

        public SelectTest1(String baseOneTableName) {
            this.baseOneTableName = baseOneTableName;
        }

        /**
         * @since 5.0.1
         */
        @Test
        public void selectAllFieldTest() {
            String sql = "select * from " + baseOneTableName + " where pk=" + columnDataGenerator.pkValue;
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        @Test
        public void xplanPkTest() {
            if (!useXproto(tddlConnection)) {
                return;
            }

            JdbcUtil.executeQuerySuccess(tddlConnection,
                "trace select * from " + baseOneTableName + " where pk=" + columnDataGenerator.pkValue);
            final List<List<String>> res =
                JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("show trace", tddlConnection), false,
                    ImmutableList.of());
            final String trace = res.get(0).get(11);
            Assert.assertTrue(trace.contains("/*PolarDB-X Connection*/"));
            Assert.assertTrue(trace.contains("plan_digest"));
        }

        /**
         * @since 5.0.1
         */
        @Test
        public void selectAllFieldWithSpecialAliasTest() {
            String sql = "select top.* from " + baseOneTableName + " top where pk=" + columnDataGenerator.pkValue;
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.0.1
         */
        @Test
        @Ignore
        public void selectAllFieldWithFuncTest() {
            String sql = "select *,count(*) from " + "`" + TStringUtil.replace(baseOneTableName, "`", "``") + "`"
                + " where pk=" + columnDataGenerator.pkValue;
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.0.1
         */
        @Test
        public void selectSomeFieldTest() {
            String sql = "select integer_test,varchar_test from " + "`"
                + TStringUtil.replace(baseOneTableName, "`", "``") + "`" + "  where pk="
                + columnDataGenerator.pkValue;
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

            sql = "select integer_test,varchar_test from " + "`" + TStringUtil.replace(baseOneTableName, "`", "``")
                + "`" + " where varchar_test= ?";
            List<Object> param = new ArrayList<Object>();
            param.add(columnDataGenerator.varchar_testValue);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.0.1
         */
        @Test
        public void selectWithNotExistDateTest() {
            String sql = "select * from " + "`" + TStringUtil.replace(baseOneTableName, "`", "``") + "`"
                + " where pk=1000000";

            selectOrderAssert(sql, null, mysqlConnection, tddlConnection, true);
        }

        /**
         * @since 5.0.1
         */
        @Test
        public void selectWithNotExistTableTest() {
            String sql = "select * from stu where pk=" + columnDataGenerator.pkValue;
            selectErrorAssert(sql, null, tddlConnection, "doesn't exist");
        }

        /**
         * @since 5.1.22
         */
        @Test
        public void selectWithKeyWordTableTest() {

            String sqlForTddl = "select * from " + polardbxOneDB + "." + baseOneTableName + " where pk="
                + columnDataGenerator.pkValue + 1;
            String sqlForMySql = "select * from " + mysqlOneDB + "." + baseOneTableName + " where pk="
                + columnDataGenerator.pkValue + 1;
            selectContentSameAssert(sqlForMySql, sqlForTddl, null, mysqlConnection, tddlConnection);

        }

        /**
         * @since 5.1.28
         */
        @Test
        public void selectWithTinyint1bitTestAddIntegerTest() {
            String sqlForTddl = "select tinyint_1bit_test + integer_test  from " + baseOneTableName
                + " where pk > 2 and pk < 10 order by pk limit 1";
            String sqlForMySql = "select tinyint_1bit_test + integer_test  from " + mysqlOneDB + "." + baseOneTableName
                + " where pk > 2 and pk < 10 order by pk limit 1";
            selectContentSameAssert(sqlForMySql, sqlForTddl, null, mysqlConnection, tddlConnection);
        }

        @Test
        public void projectPushdownTest() {
            String sql = String
                .format("select tt1.pk from\n" + "(SELECT\n" + "t5.year_test,\n" + "t1.pk\n"
                        + "FROM %s t1 inner join %s t5 on " + "t5.pk=t1.pk\n" + ") tt1\n"
                        + "INNER JOIN %s tt3 ON tt3.pk=tt1.pk\n" + "INNER JOIN %s tt4 on tt4.pk=tt1.pk ;",
                    baseOneTableName,
                    baseOneTableName,
                    baseOneTableName,
                    baseOneTableName);

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        @Test
        public void joinPushDownTest() {
            String sql = String
                .format(
                    "/*+TDDL:Master*/select mtd.pk as ticketDefId from %s as mtd,select_base_one_multi_db_multi_tb as mti "
                        + "where mtd.pk = mti.integer_test and (case when (mtd.varchar_test = '0' and mtd.tinyint_test = -1) then 1 = 1 else mti.time_test > '12:27:50' end) "
                        + "order by ticketDefId " + "limit 10;",
                    baseOneTableName);

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        }

        @Test
        public void selectConstantTest() {
            List<String> sqls = new ArrayList<>();

            sqls.add("select ''");
            sqls.add("select ' '");
            sqls.add("select '*'");
            sqls.add("select '?'");
            sqls.add("select '1'");
            // Here we disable optimizer cache since "select 1" will hit the
            // plan
            // cache of "select '1'" after parameterized,
            // but actually "select 1" should has different plan as "select '1'"
            sqls.add("/*+TDDL({'extra':{'OPTIMIZER_CACHE':'TRUE'}})*/select 1");

            for (String sql : sqls) {
//                System.out.println("0: " + sql);
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//                System.out.println("1: " + sql);
            }
        }

        @Test
        public void selectServerVarTest() {
            List<String> sqls = new ArrayList<>();
            String mysqlTx = "", tddlTx = "";
            String sqlGet = "";

            //获取当前数据库的参数配置
            if (isMySQL80()) {
                sqlGet = "select @@transaction_isolation AS tx_isolation";
            } else {
                sqlGet = "select @@tx_isolation AS tx_isolation";
            }

            mysqlTx = JdbcUtil.executeQueryAndGetFirstStringResult(sqlGet, mysqlConnection);
            tddlTx = JdbcUtil.executeQueryAndGetFirstStringResult(sqlGet, tddlConnection);

            try {
                if (!mysqlTx.equals(tddlTx)) {
                    String setSql = "set session transaction isolation level " + StringUtils
                        .replace(tddlTx.toLowerCase(), "-", " ");
                    JdbcUtil.executeUpdate(mysqlConnection, setSql);
                }

                sqls.add("select @@global.auto_increment_increment");
                sqls.add("select @@auto_increment_increment");
                sqls.add("select @@session.auto_increment_increment");
                final StringBuilder e = new StringBuilder()
                    .append("SELECT @@session.auto_increment_increment AS auto_increment_increment, " +
                        // "@@character_set_client AS character_set_client, "
                        // +
                        // "@@character_set_connection AS character_set_connection, "
                        // +
                        // "@@character_set_results AS character_set_results, "
                        // +
                        // "@@character_set_server AS character_set_server, "
                        // +
                        // "@@collation_server AS collation_server, "
                        // +
                        "@@init_connect AS init_connect, " + "@@interactive_timeout AS interactive_timeout, "
                        + "@@license AS license, " + "@@lower_case_table_names AS lower_case_table_names, "
                        + "@@max_allowed_packet AS max_allowed_packet, "
                        + "@@net_buffer_length AS net_buffer_length, ");
                if (!isMySQL80()) {
                    // "@@net_write_timeout AS net_write_timeout, " +
                    e.append("@@query_cache_size AS query_cache_size, " + "@@query_cache_type AS query_cache_type, ");
                }
                // "@@sql_mode AS sql_mode, " +
                e.append("@@system_time_zone AS system_time_zone, ");
                // "@@time_zone AS time_zone, " +
                if (isMySQL80()) {
                    e.append("@@transaction_isolation AS tx_isolation, ");
                } else {
                    e.append("@@tx_isolation AS tx_isolation, ");
                }
                e.append("@@wait_timeout AS wait_timeout");
                sqls.add(e.toString());
                for (String sql : sqls) {
                    selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
                }
            } finally {
                if (!mysqlTx.isEmpty()) {
                    String setSql = "set session transaction isolation level " + StringUtils
                        .replace(mysqlTx.toLowerCase(), "-", " ");
                    JdbcUtil.executeUpdate(mysqlConnection, setSql);
                }
            }

        }

        @Test
        public void selectSequenctTest() {
            String sql;
            ResultSet tddlRs = null;
            PreparedStatement tddlPs = null;

            try {
                sql = "drop sequence test_seq";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlPs.executeUpdate();
            } catch (Exception e) {
                // pass
            } finally {
                JdbcUtil.close(tddlPs);
            }

            try {
                sql = "create sequence test_seq";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlPs.executeUpdate();
            } catch (Exception e) {
                // pass
            } finally {
                JdbcUtil.close(tddlPs);
            }

            try {
                sql = "select test_seq.nextval from dual";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                Assert.assertEquals("size = 1", 1, resultsSize(tddlRs));
            } finally {
                JdbcUtil.close(tddlPs);
                JdbcUtil.close(tddlRs);
            }

            try {
                sql = "select test_seq.nextval from dual where count = 1";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                Assert.assertEquals("size = 1", 1, resultsSize(tddlRs));
            } finally {
                JdbcUtil.close(tddlPs);
                JdbcUtil.close(tddlRs);
            }

            try {
                sql = "select test_seq.nextval from dual where count = 10";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                Assert.assertEquals("size = 10", 10, resultsSize(tddlRs));
            } finally {
                JdbcUtil.close(tddlPs);
                JdbcUtil.close(tddlRs);
            }

            try {
                sql = "select test_seq.nextval as abc from dual where count = 20";
                tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                Assert.assertEquals("size = 20", 20, resultsSize(tddlRs));
            } finally {
                JdbcUtil.close(tddlPs);
                JdbcUtil.close(tddlRs);
            }
        }

        @Test
        public void selectAggregationTest() {
            String sql1 = "select all 99 as col1, count(*) as col2";
            selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

            String sql2 = "select all 99 as col1, count(*) as col2 from dual";
            selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.10
         */
        @Test
        public void distinctOrderByTest() throws Exception {
            String tsm = JdbcUtil.getSqlMode(tddlConnection);
            String msm = JdbcUtil.getSqlMode(mysqlConnection);
            try {
                String sql =
                    "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'";
                if (isMySQL80()) {
                    sql =
                        "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'";
                }
                JdbcUtil.updateDataTddl(tddlConnection, sql, null);
                JdbcUtil.updateDataTddl(mysqlConnection, sql, null);
                sql = "select distinct integer_test from " + baseOneTableName + " as t order by char_test";
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            } finally {
                JdbcUtil.updateDataTddl(tddlConnection, "set session sql_mode = '" + tsm + "'", null);
                JdbcUtil.updateDataTddl(mysqlConnection, "set session sql_mode = '" + msm + "'", null);
            }
        }

        /**
         * @since 5.3.10
         */
        @Test
        public void distinctOrderBy2Test() throws Exception {
            String tsm = JdbcUtil.getSqlMode(tddlConnection);
            String msm = JdbcUtil.getSqlMode(mysqlConnection);
            try {
                String sql =
                    "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'";
                if (isMySQL80()) {
                    sql =
                        "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'";
                }
                JdbcUtil.updateDataTddl(tddlConnection, sql, null);
                JdbcUtil.updateDataTddl(mysqlConnection, sql, null);
                sql = "select distinct integer_test,1 from " + baseOneTableName + " as t order by char_test";
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            } finally {
                JdbcUtil.updateDataTddl(tddlConnection, "set session sql_mode = '" + tsm + "'", null);
                JdbcUtil.updateDataTddl(mysqlConnection, "set session sql_mode = '" + msm + "'", null);
            }
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void distinctOrderBy3Test() throws Exception {
            if (usingNewPartDb()) {
                return;
            }
            String sql = "select distinct integer_test from " + baseOneTableName
                + " order by pk, concat(char_test, 'hehe')";
            // __FIRST_VALUE is not determistic
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test

        public void selectOrderByTest() throws Exception {
            String sql = "select integer_test from " + baseOneTableName
                + " order by convert(ifnull(integer_test, '999'), signed)";
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void selectMultiNotLikeTest() throws Exception {
            String sql = "select * from " + baseOneTableName
                + " where varchar_test not like 'abc' and varchar_test not like '%a' and varchar_test not like '%b'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void selectWhereNot() throws Exception {
            String sql = "select NOT ( - - 11 ) + - 58 IS NOT NULL as col";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = "select NOT ( integer_test ) * 56 IS NOT NULL as col from " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = "select ( NOT ( 23 ) * + ( integer_test ) IS NOT NULL ) as col from " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void selectWithMultiPlus() throws Exception {
            String sql = "select * from " + baseOneTableName
                + " WHERE (integer_test + - 12 + + CAST( NULL AS SIGNED ) + - 62 NOT BETWEEN - + integer_test * - CAST( NULL AS SIGNED ) * 8 * - - integer_test AND + + 99) or 1";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql =
                "SELECT - CASE + 3 WHEN + 24 + COUNT( * ) + + NULLIF ( + 33, + 34 + + 30 ) THEN + SUM( ALL + 73 ) * - CAST( + 3 AS SIGNED ) + - COUNT( * ) WHEN - COUNT( * ) * - 47 + + ( - - 33 ) + - 40 + + ( + 69 ) * COUNT( * ) / + 66 THEN NULL ELSE NULL END * 98 - - COUNT( 22 ) AS col0;";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql =
                "SELECT + 92 * ( - MAX( - 66 ) ) * - 83 + + - 31 * - - CASE - - CASE 52 WHEN - COUNT( * ) THEN - 8 + - 86 WHEN - COALESCE ( ( - 10 ), - ( + 14 ), - - 92 * + + 69 / - 30 * + COUNT( * ) / + - 91 * 34 ) * COUNT( ALL + 25 ) * + CAST( NULL AS SIGNED ) + - - 61 THEN NULL END WHEN + + 7 * 19 + CASE WHEN NOT ( ( + 6 ) = 63 ) OR NOT NULL IS NULL THEN NULL WHEN NOT NULL > + 70 / - 88 THEN - 34 + CAST( 6 - 48 AS SIGNED ) END THEN NULL ELSE 66 * + 35 END;";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = "SELECT + + 64";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql =
                "SELECT DISTINCT 2 + CASE 86 WHEN + - 95 + - 10 * 7 * - AVG ( 74 ) + - 96 + + 39 THEN NULL WHEN - - CAST( NULL AS DECIMAL ) THEN NULL WHEN + + 79 THEN NULL ELSE - 29 END + 71 AS col0";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void selectShardingKeyLargeThanMinusTen() throws Exception {
            String sql = "select pk from " + baseOneTableName + " where pk > -10";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.12
         */
        @Test
        public void selectLmitWithinDerivedSubquery() throws Exception {
            String sql = "select count(*) from (select * from " + baseOneTableName + " where pk > 0 limit 2,5) tmp";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.3.13
         */
        @Test
        public void selectOrderByWithNotExistsColumnTest() throws Exception {
            if (!baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)) {
                return;
            }

            String sql = "select pk,integer_test from " + baseOneTableName + " order by pk1;";
            final Statement statement = tddlConnection.createStatement();
            try {
                statement.execute(sql);
            } catch (Exception e) {
                final String message = e.getMessage();
                com.alibaba.polardbx.common.utils.Assert.assertTrue(
                    message.contains(String.format("Column '%s' not found", "pk1")),
                    "expect 'Column not found'.");
            }
        }

        /**
         * @since 5.3.12
         */
        @Test
        @FileStoreIgnore
        public void selectExplainExecute() throws Exception {
            if (baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX) || baseOneTableName.endsWith(
                ExecuteTableName.BROADCAST_TB_SUFFIX)) {
                return;
            }
            String sql = "select a.pk from " + baseOneTableName + " a join " + baseOneTableName
                + " b on a.integer_test=b.integer_test where b.pk=-10 and a.pk = -10";
            explainAllResultMatchAssert("explain " + sql, null, tddlConnection,
                "[\\s\\S]*" + "PhyTableOperation" + "[\\s\\S]*");

            final Statement statement = tddlConnection.createStatement();
            try {
                ResultSet rs = statement.executeQuery("explain execute " + sql);
                Assert.assertTrue(rs.next());
            } catch (Exception e) {
                final String message = e.getMessage();
                com.alibaba.polardbx.common.utils.Assert
                    .assertTrue(message.contains(String.format("Column '%s' not found",
                        "pk1")), "expect 'Column not found'.");
            } finally {
                statement.close();
            }
        }

        @Test
        public void selectRandTest() {
            String sql = "select rand(null)=rand(0) as a";
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        @Test
        public void selectReplaceTest() {
            String sql = "select REPLACE( NULL, '', 'bravo' ) as xxx;";
            selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.4.5
         */
        @Test
        public void selectRowWithJoinCanPushProjectToLVTest() {
            String sql =
                "SELECT (1, 2) NOT IN ((65 + layer_0_left_tb.layer_0_column_0, 2), (layer_0_right_tb.time_test, 9)) FROM ( SELECT tinyint_test AS layer_0_column_0 FROM "
                    + baseOneTableName
                    + " GROUP BY layer_0_column_0 ORDER BY layer_0_column_0 LIMIT 34, 18 ) layer_0_left_tb LEFT JOIN "
                    + baseOneTableName
                    + " layer_0_right_tb ON layer_0_right_tb.tinyint_1bit_test = layer_0_right_tb.varchar_test;";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        }

        @Test
        public void selectWithMysqlPartitionTest() {
            String sql = "select * from `" + baseOneTableName + "` partition (p123)";

            if (!usingNewPartDb()) {
                JdbcUtil.executeQueryFaied(tddlConnection, sql, "Do not support table with mysql partition");
            } else {
                if (!baseOneTableName.toLowerCase().contains("broadcast")) {
                    JdbcUtil.executeQueryFaied(tddlConnection, sql, "Unknown partition 'p123'");
                } else {
                    JdbcUtil.executeQueryFaied(tddlConnection, sql, "on non partitioned table");
                }

            }

        }

        @Test
        public void selectWithBinaryExprTest() {
            String sql = "select * from `" + baseOneTableName + "` where bit_test=b'1'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    public static class SelectTest2 extends AutoReadBaseTestCase {

        @Test
        public void selectLimitOffset() {
            String tableName = "t_selectNegativeTime";
            prepareTimeTypeTable(tableName);
            String sql = "select * from " + tableName + " limit 1 offset 1";
            checkRowCount(1, sql);
            sql = "select * from " + tableName + " limit 1 offset 0";
            checkRowCount(1, sql);
            sql = "select * from " + tableName + " limit 1999 offset 0";
            checkRowCount(2, sql);
            sql = "select * from " + tableName + " limit 99999999999 offset 1";
            checkRowCount(1, sql);
            sql = "select * from " + tableName + " limit 99999999999 offset 199999999999";
            checkRowCount(0, sql);
            sql = "select * from " + tableName + " order by c_year limit 99999999999 offset 199999999999";
            checkRowCount(0, sql);
            sql = "select * from " + tableName + " order by c_year limit 99999999999 offset 1";
            checkRowCount(1, sql);
        }

        @Test
        public void selectNegativeTime() {
            String tableName = "t_selectNegativeTime";
            prepareTimeTypeTable(tableName);
            String sql = "select * from " + tableName;
            checkRowCount(2, sql);

            dropTableIfExists(tableName);
        }

        @Test
        public void selectCommaName() {
            String tableName = "type_newdecimal_t1_2";
            dropTableIfExists(tableName);
            String sql = "CREATE TABLE `type_newdecimal_t1_2` (\n"
                + "  `nullif(1.1, 1.1)` decimal(2,1) DEFAULT NULL,\n"
                + "  `nullif(1.1, 1.2)` decimal(2,1) DEFAULT NULL,\n"
                + "  `nullif(1.1, 0.11e1)` decimal(2,1) DEFAULT NULL,\n"
                + "  `nullif(1.0, 1)` decimal(2,1) DEFAULT NULL,\n"
                + "  `nullif(1, 1.0)` int(1) DEFAULT NULL,\n"
                + "  `nullif(1, 1.1)` int(1) DEFAULT NULL\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            String selectSql = "select * from " + tableName;
            checkRowCount(0, selectSql);
            dropTableIfExists(tableName);
        }

        @Test
        public void checkUnsignedIntAndRounding() {
            String tableName = "t_selectUnsignedInt";
            String dropSql = "drop table if exists " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropSql, null);
            prepareUnsignedTypeTable(tableName);
            String sql = "select * from " + tableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = "select * from " + tableName + " where id1=null";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
            sql = "select * from " + tableName + " where id1 is not null";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropSql, null);
        }

        public void prepareUnsignedTypeTable(String tableName) {
            dropTableIfExists(tableName);
            String sqlMode = "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
            if (isMySQL80()) {
                sqlMode = "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
            }
            try {
                setSqlMode(sqlMode, tddlConnection);
                JdbcUtil.updateDataTddl(mysqlConnection, "SET session sql_mode = '" + sqlMode + "'", null);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id1` INT UNSIGNED NOT NULL ,\n"
                + "  `id2` INT NOT NULL,\n" + "  `strV` VARCHAR(20), PRIMARY KEY (id1)\n"
                + ") ";

            if (usingNewPartDb()) {
                sql += " partition BY hash(id1) partitions 3;";
            } else {
                sql += " dbpartition BY hash(id1) tbpartition by hash(id1) tbpartitions 3;";
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id1` INT UNSIGNED NOT NULL ,\n"
                + "  `id2` INT NOT NULL,\n" + "  `strV` VARCHAR(20), PRIMARY KEY (id1)\n" + ")";
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            sql = "insert into " + tableName
                + "(id1,id2, strV) values(-20,-20,'-20'),(20,20.5,'20.5'),(23,23.4,'23.4')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        }

        public void checkRowCount(int expectedRowCount, String sql) {
            int count = 0;
            try {
                ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
                while (rs.next()) {
                    count++;
                }
                Assert.assertTrue(count == expectedRowCount);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }

        public void prepareTimeTypeTable(String tableName) {
            dropTableIfExists(tableName);
            String sqlMode =
                "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
            if (isMySQL80()) {
                sqlMode = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
            }
            try {
                setSqlMode(sqlMode, tddlConnection);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
                + "  `id` BIGINT UNSIGNED NOT NULL  AUTO_INCREMENT,\n" + "  `c_year_4` year(4) DEFAULT NULL,\n"
                + "  `c_year` year DEFAULT NULL,\n" + "  `c_time` time DEFAULT NULL,\n"
                + "  `c_datetime` datetime DEFAULT NULL,\n" + "  PRIMARY KEY (id)\n";

            if (usingNewPartDb()) {
                sql += ") partition BY hash(id);";
            } else {
                sql += ") dbpartition BY hash(id);";
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "insert into " + tableName
                + "(id,c_year_4, c_year, c_datetime, c_time) values(null,'0000', '0000', '0000-00-00 01:01:01', '-01:01:01')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "insert into " + tableName
                + "(id,c_year_4, c_year, c_datetime, c_time) values(null,'0001', '0001', '0000-00-00 01:01:02', '-02:01:01')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        public void dropTableIfExists(String tableName, Connection connection) {
            String sql = "drop table if exists " + tableName;
            JdbcUtil.executeUpdateSuccess(connection, sql);
        }

        @Test
        public void selectTimestamp() {
            String sqlMode =
                "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
            if (isMySQL80()) {
                sqlMode = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
            }
            try {
                setSqlMode(sqlMode, tddlConnection);
                dropTableIfExists("t001", mysqlConnection);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                setSqlMode(sqlMode, tddlConnection);
                dropTableIfExists("t001", tddlConnection);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String sql1 = "CREATE TABLE `t001` (\n" +
                "  `a` int(11) DEFAULT NULL,\n" +
                "  `b` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n" +
                "  KEY `auto_shard_key_a` (`a`) USING BTREE\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
            String sql = "CREATE TABLE `t001` (\n" +
                "  `a` int(11) DEFAULT NULL,\n" +
                "  `b` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n" +
                "  KEY `auto_shard_key_a` (`a`) USING BTREE\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ";

            if (usingNewPartDb()) {
                sql += " partition BY hash(`a`);";
            } else {
                sql += "dbpartition BY hash(`a`);";
            }

            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "insert into t001 values(1,now(6));";
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            selectContentSameAssert("select * from t001", null, mysqlConnection, tddlConnection);
            //make sure the result is the same when using condition
            selectContentSameAssert("select * from t001 where a = 1", null, mysqlConnection, tddlConnection);
            sql = "insert into t001 values(2,now());";
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            selectContentSameAssert("select * from t001", null, mysqlConnection, tddlConnection);
            selectContentSameAssert("select * from t001 where a = 2", null, mysqlConnection, tddlConnection);
            sql = "insert into t001 values(3,now(1));";
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            selectContentSameAssert("select * from t001", null, mysqlConnection, tddlConnection);
            selectContentSameAssert("select * from t001 where a = 3", null, mysqlConnection, tddlConnection);

        }

        @Test
        public void joinInvolveWithBit8Type() {
            String tableName = "t_joinWithBit8";
            String dropSql = "drop table if exists " + tableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
            prepareBIT8TypeTable(tableName);
            String sql = "select t1.*, t2.* from " + tableName + " t1, " + tableName + " t2 where t1.id = t2.id";
            JdbcUtil.executeQuery(sql, tddlConnection);
        }

        public void prepareBIT8TypeTable(String tableName) {
            String sql = "CREATE TABLE if not exists " + tableName + " (\n"
                + "  `id` bigint(20) DEFAULT NULL,\n"
                + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
                + "  `c_int_32_un` int(32) unsigned NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "  `c_bigint_64_un` bigint(64) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`c_int_32_un`)) ";

            if (usingNewPartDb()) {
                sql += " partition by hash(`c_bigint_64_un`) partitions 3;";
            } else {
                sql += " dbpartition by hash(`c_bigint_64_un`) tbpartition by hash(`c_bigint_64_un`) tbpartitions 3;";
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "insert into " + tableName + "(id, c_bit_8, c_bigint_64_un) values(10001, 21, 190);";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        @Test
        @FileStoreIgnore
        public void selectBkaWithConditionTest() {
            selectContentSameAssert(
                "/*+TDDL:BKA_JOIN(select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb) BKA_JOIN((select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb),select_base_one_one_db_one_tb)*/"
                    + "SELECT tbl_dba_test_t1.pk, t2.varchar_test, t3.varchar_test FROM select_base_one_multi_db_one_tb tbl_dba_test_t1 "
                    + "LEFT JOIN select_base_one_one_db_multi_tb t2 "
                    + "ON tbl_dba_test_t1.integer_test = t2.pk AND tbl_dba_test_t1.tinyint_test = 1 "
                    + "LEFT JOIN select_base_one_one_db_one_tb t3 "
                    + "ON tbl_dba_test_t1.integer_test = t3.pk AND tbl_dba_test_t1.tinyint_test = 2 ORDER BY tbl_dba_test_t1.pk;",
                null, mysqlConnection, tddlConnection);
        }

        @Test
        @FileStoreIgnore
        /**
         * test a.id = b.id and a.other_field = b.other_field
         */
        public void selectBkaWithCondition1Test() {
            selectContentSameAssert(
                "/*+TDDL:BKA_JOIN(select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb) BKA_JOIN((select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb),select_base_one_one_db_one_tb)*/"
                    + "SELECT tbl_dba_test_t1.pk, t2.varchar_test, t3.varchar_test FROM select_base_one_multi_db_one_tb tbl_dba_test_t1 "
                    + "LEFT JOIN select_base_one_one_db_multi_tb t2 "
                    + "ON tbl_dba_test_t1.integer_test = t2.pk AND tbl_dba_test_t1.tinyint_test = 1 and tbl_dba_test_t1.varchar_test = t2.varchar_test "
                    + "LEFT JOIN select_base_one_one_db_one_tb t3 "
                    + "ON tbl_dba_test_t1.integer_test = t3.pk AND tbl_dba_test_t1.tinyint_test = 2 ORDER BY tbl_dba_test_t1.pk;",
                null, mysqlConnection, tddlConnection);
        }

        @Test
        @FileStoreIgnore
        /**
         * test a.id = b.id or a.other_field = b.other_field
         */
        public void selectBkaWithConditionOrTest() {
            selectContentSameAssert(
                "/*+TDDL:BKA_JOIN(select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb) BKA_JOIN((select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb),select_base_one_one_db_one_tb)*/"
                    + "SELECT tbl_dba_test_t1.pk, t2.varchar_test, t3.varchar_test FROM select_base_one_multi_db_one_tb tbl_dba_test_t1 "
                    + "LEFT JOIN select_base_one_one_db_multi_tb t2 "
                    + "ON tbl_dba_test_t1.integer_test = t2.pk AND (tbl_dba_test_t1.tinyint_test = 1 or tbl_dba_test_t1.varchar_test = t2.varchar_test) "
                    + "LEFT JOIN select_base_one_one_db_one_tb t3 "
                    + "ON tbl_dba_test_t1.integer_test = t3.pk AND tbl_dba_test_t1.tinyint_test = 2 ORDER BY tbl_dba_test_t1.pk;",
                null, mysqlConnection, tddlConnection);
        }

        @Test
        @FileStoreIgnore
        public void selectBkaWithCondition2Test() {
            selectContentSameAssert(
                "/*+TDDL:BKA_JOIN(select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb) BKA_JOIN((select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb),select_base_one_one_db_one_tb)*/"
                    + "SELECT tbl_dba_test_t1.pk, t2.varchar_test, t3.varchar_test FROM select_base_one_multi_db_one_tb tbl_dba_test_t1 "
                    + "LEFT JOIN select_base_one_one_db_multi_tb t2 "
                    + "ON tbl_dba_test_t1.integer_test = t2.pk AND tbl_dba_test_t1.tinyint_test = 1 and tbl_dba_test_t1.tinyint_test <> t2.integer_test "
                    + "LEFT JOIN select_base_one_one_db_one_tb t3 "
                    + "ON tbl_dba_test_t1.integer_test = t3.pk AND tbl_dba_test_t1.tinyint_test = 2 ORDER BY tbl_dba_test_t1.pk;",
                null, mysqlConnection, tddlConnection);
        }

        /**
         * @since 5.4.9
         */
        @Test
        public void selectInnerFourRightBushyJoin() {
            String sql =
                "/*+TDDL:master() cmd_extra(workload_type=ap)*/ select pk0, pk1, pk2, pk3 from ( select (case when pk >= 750 then null else pk end ) as pk0 from select_base_one_multi_db_one_tb) t0 right join ( select (case when pk < 250 then null else pk end ) as pk1 from select_base_two_multi_db_one_tb) t1 on t0.pk0 = t1.pk1 right join ( select (case when pk >= 250 and pk < 500 then null else pk end ) as pk2 from select_base_three_multi_db_one_tb) t2 on t1.pk1 = t2.pk2 right join ( select (case when pk >= 750 then null else pk end ) as pk3 from select_base_four_multi_db_one_tb) t3 on t2.pk2 = t3.pk3 order by pk3;";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        /**
         * Special expression from random SQL: (BINARY ('a')='A') <=> (a.smallint_test NOT IN (b.time_test)))
         */
        @Test
        @FileStoreIgnore
        public void selectBkaWithRowRexCallAndOtherRexCallTest() {
            selectContentSameAssert(
                "/*+TDDL:BKA_JOIN(select_base_one_multi_db_one_tb,select_base_one_one_db_multi_tb)*/select a.* from select_base_one_multi_db_one_tb a "
                    + "left join select_base_one_one_db_multi_tb b "
                    + "on a.pk = b.pk and (a.pk,a.varchar_test) = ((b.pk,b.varchar_test)) "
                    + "and ((BINARY ('a')='A') <=> (a.smallint_test NOT IN (b.time_test)))", null, mysqlConnection,
                tddlConnection);
        }

        /**
         * @since 5.4.6
         * test aggregate pruning,use join to make aggregate is not push down to logical view
         */
        @Test
        public void aggregatePruningTest() {

            if (usingNewPartDb()) {
                return;
            }

            String sql1 = "/*+TDDL:master()*/select r.pk,r.integer_test,count(*) "
                + "     from select_base_one_multi_db_multi_tb r join select_base_one_one_db_multi_tb s "
                + "        on r.pk = s.pk "
                + "   group by r.pk,r.integer_test;";

            selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
            String explainSql = "explain " + sql1;
            String explainResult = explainResultString(explainSql, null, tddlConnection);
            System.out.println(explainResult);
            Assert.assertTrue(explainResult.indexOf("group=\"pk\", $f1=\"__FIRST_VALUE(integer_test)\"") > -1
                || explainResult.indexOf("group=\"pk\", integer_test=\"__FIRST_VALUE(integer_test)\"") > -1
                || explainResult.indexOf("group=\"pk0\", $f1=\"__FIRST_VALUE(integer_test)\"") > -1
                || explainResult.indexOf("group=\"pk0\", integer_test=\"__FIRST_VALUE(integer_test)\"") > -1);
        }

        /**
         * @since 5.4.6
         * test aggregate pruning,use join to make aggregate is not push down to logical view
         */
        @Test
        public void aggregatePruningTest1() {
            String sql1 =
                "SELECT DISTINCT integer_test, ( + COUNT( * ) ) AS col1 FROM select_base_three_one_db_multi_tb GROUP BY integer_test, integer_test;";

            selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
        }

        private String explainResultString(String sql, List<Object> param, Connection tddlConnection) {
            PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            StringBuilder actualExplainResult = new StringBuilder();
            ResultSet rs = null;
            try {
                rs = tddlPs.executeQuery();
                while (rs.next()) {
                    actualExplainResult.append(rs.getString(1));
                }
                return actualExplainResult.toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                JdbcUtil.close(rs);
                JdbcUtil.close(tddlPs);
            }
        }

        public void dropTableIfExists(String tableName) {
            String sql = "drop table if exists " + tableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        @Test
        public void testBadTableSource() {
            JdbcUtil.executeQueryFaied(tddlConnection, "select * from 1", "You have an error in your SQL syntax");
        }

        @Test
        public void selectPrepareTest() {
            if (usePrepare()) {
                return;
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "PREPARE sel from \"select pk from select_base_one_one_db_multi_tb where pk = ?\"");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set @id = 1");

            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                "PREPARE sel from \"select pk from select_base_one_one_db_multi_tb where pk = ?\"");
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "set @id = 1");

            selectContentSameAssert(
                "EXECUTE sel USING @id",
                null, mysqlConnection, tddlConnection);
        }

        @Test
        public void selectPrepareBkaJoinTest() {
            if (usePrepare()) {
                return;
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "PREPARE sel from \"select pk from select_base_one_one_db_multi_tb where pk in (1, 2, 3) and integer_test != ? \"");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set @id = 17");

            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                "PREPARE sel from \"select pk from select_base_one_one_db_multi_tb where pk in (1, 2, 3) and integer_test != ? \"");
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "set @id = 17");

            selectContentSameAssert(
                "EXECUTE sel USING @id",
                null, mysqlConnection, tddlConnection);
        }

        @Test
        public void selectSqlNoCacheTest() {
            String sql =
                "trace SELECT /*!40001 SQL_NO_CACHE */ * FROM select_base_three_multi_db_multi_tb";
            Statement statement1 = null;
            ResultSet resultSet1 = null;
            Statement statement2 = null;
            ResultSet resultSet2 = null;
            try {
                statement1 = tddlConnection.createStatement();
                resultSet1 = statement1.executeQuery(sql);
                while (resultSet1.next()) {
                }
                resultSet1.close();
                statement1.close();
                statement2 = tddlConnection.createStatement();
                resultSet2 = statement2.executeQuery("show trace");
                while (resultSet2.next()) {
                    Assert.assertTrue(!resultSet2.getString(12).toUpperCase().contains("SQL_NO_CACHE"));
                }
            } catch (SQLException sqlException) {
                Assert.fail(sqlException.getMessage());
            } finally {
                JdbcUtil.close(resultSet1);
                JdbcUtil.close(statement1);
                JdbcUtil.close(resultSet2);
                JdbcUtil.close(statement2);
            }

        }
    }

    public static class SelectInTest extends AutoReadBaseTestCase {

        @Test
        public void testDynamicValues() {
            String sql = "/*TDDL:a()*/ select 1 in (1, 2*2) as a";
            String explain = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(explain != null && explain.contains("?2"));

            sql = "/*TDDL:a()*/ select 1 in (1,2,3,4) as a";
            explain = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(explain != null && !explain.contains("?2"));

            sql =
                "/*TDDL:a()*/ SELECT  * FROM select_base_one_multi_db_multi_tb a join select_base_three_multi_db_multi_tb b on a.pk = b.integer_test where (a.pk, b.pk) in ((1,2),(3,4), (4, 5*6))";
            explain = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(explain != null && explain.contains("?2"));

            sql =
                " /*TDDL:a()*/ SELECT  * FROM select_base_one_multi_db_multi_tb a join select_base_three_multi_db_multi_tb b on a.pk = b.integer_test where (a.pk, b.pk) in ((1,2),(3,4), (4, 5))";
            explain = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(explain != null && !explain.contains("?2"));
        }

        @Test
        public void testDynamicValues2() {
            String tableName = "DynamicValues2_" + new Random().nextInt(10000);
            try {
                String sql = "/*TDDL:a()*/ select 1 in (1, 2*2) as a";
                JdbcUtil.executeQuery(sql, tddlConnection);
                sql = "/*TDDL:a()*/ select 1 in (1,2,3,4) as a";
                JdbcUtil.executeQuery(sql, tddlConnection);
                sql = "/*TDDL:a()*/ select 1 not in (1,2,3,4) as a";
                JdbcUtil.executeQuery(sql, tddlConnection);
                sql = "/*TDDL:a()*/ select (1,2) in ((1,2),(3,4))";
                JdbcUtil.executeQuery(sql, tddlConnection);
                sql = "/*TDDL:a()*/ select (1,2) not in ((1,2),(3,4))";
                JdbcUtil.executeQuery(sql, tddlConnection);

                DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "drop table if exists " + tableName,
                    null, false);
                // create new table and insert data
                sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id` INT UNSIGNED NOT NULL ,\n"
                    + "  `name` VARCHAR(20), PRIMARY KEY (id)\n"
                    + ") ";
                JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);

                if (usingNewPartDb()) {
                    sql += " partition BY hash(id) partitions 3;";
                } else {
                    sql += " dbpartition BY hash(id) tbpartition by hash(id) tbpartitions 3;";
                }
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                sql = "insert into " + tableName + "(id,name) values (1,'2'), (2,'b')";
                DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

                sql = "/*TDDL:a()*/  select * from " + tableName
                    + " where (`id`) in (1,1,2,3,4,5,6);";
                DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

                DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "drop table if exists " + tableName,
                    null, false);
                // rebuild table by two sharding cols
                sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id` INT UNSIGNED NOT NULL ,\n"
                    + "  `name` VARCHAR(20), PRIMARY KEY (id)\n"
                    + ") ";
                JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
                if (usingNewPartDb()) {
                    sql += " PARTITION BY KEY(`ID`,`name`) PARTITIONS 4";
                } else {
                    sql += " dbpartition BY hash(id) tbpartition by hash(name) tbpartitions 3;";
                }
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                sql = "insert into " + tableName + "(id,name) values (1,'2'), (2,'b')";
                DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

                sql = "/*TDDL:a()*/  select * from " + tableName
                    + " where (`id`, `name`) in ((1,'2'),(2,'b'),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2));";
                DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

                sql = "/*TDDL:a()*/  select * from " + tableName
                    + " where (`id`, `name`) in ((1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,name));";
                DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

                sql = "select * from " + tableName
                    + " where (`id`, `name`) in ((1,'2'),(2,'b'),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2),(1,2));";
                DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
                DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            } finally {
                DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "drop table if exists " + tableName,
                    null, false);
            }
        }
    }

    public static class SelectSubquery {
        public static final String dbNamePre = "select_subquery_";
        public static final Random r = new Random();

        @Test
        public void testSingleTableInDifferentLocality() throws SQLException {
            String dbName = dbNamePre + r.nextInt(1000);
            try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
                Statement statement = conn.createStatement();
                // create database
                statement.execute("create database if not exists " + dbName + " mode='auto'");

                statement.execute("use " + dbName);

                String createSql = "CREATE TABLE %s (\n"
                    + "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "  `name` varchar(20) DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ") SINGLE ENGINE=InnoDB DEFAULT CHARSET=utf8 locality = 'dn=%s';";

                ResultSet rs = statement.executeQuery("show storage where INST_KIND='MASTER'");
                List<String> storageIds = Lists.newArrayList();
                while (rs.next()) {
                    storageIds.add(rs.getString("STORAGE_INST_ID"));
                }

                if (storageIds.size() < 2) {
                    return;
                }

                // prepare tables in different storage inst
                String tb1 = "tb1";
                String tb2 = "tb2";
                statement.execute(String.format(createSql, tb1, storageIds.get(0)));
                statement.execute(String.format(createSql, tb2, storageIds.get(1)));

                // test subquery sql
                String testSql = "select 1 from %s where name in (select id from %s);";
                statement.executeQuery(String.format(testSql, tb1, tb2));
            } finally {
                ConnectionManager.getInstance().getDruidPolardbxConnection().createStatement()
                    .execute("drop database if exists " + dbName);
            }
        }
    }
}
