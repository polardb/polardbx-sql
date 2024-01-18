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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class GsiViewTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "gsi_view_test";
    private static final String INDEX_NAME = "g_i_c_view_test";

    @Before
    public void before() {
        dropTableWithGsi(PRIMARY_TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(PRIMARY_TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @Test
    public void globalIndexViewTest() throws Exception {
        final String sql = "CREATE TABLE `" + PRIMARY_TABLE_NAME + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  GLOBAL INDEX `" + INDEX_NAME
            + "` using btree (`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2";

        // Create a table with GSI.
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final String query =
            "select * from information_schema.global_indexes where `TABLE`='" + PRIMARY_TABLE_NAME + "'";

        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {
            final StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                builder.append(rs.getMetaData().getColumnName(i)).append(',');
            }
            Assert.assertEquals(
                "SCHEMA,TABLE,NON_UNIQUE,KEY_NAME,INDEX_NAMES,COVERING_NAMES,INDEX_TYPE,DB_PARTITION_KEY,DB_PARTITION_POLICY,DB_PARTITION_COUNT,TB_PARTITION_KEY,TB_PARTITION_POLICY,TB_PARTITION_COUNT,STATUS,SIZE_IN_MB,USE_COUNT,LAST_ACCESS_TIME,CARDINALITY,ROW_COUNT,",
                builder.toString());

            List<List<String>> result = JdbcUtil.getStringResult(rs, false);
            Assert.assertEquals(1, result.size());
            Assert.assertTrue(result.get(0).stream().collect(Collectors.joining(",")).contains(
                ",gsi_view_test,1,g_i_c_view_test,seller_id,id, order_id,BTREE,seller_id,HASH,2,seller_id,HASH,2,PUBLIC"));
            // NOTE: Remove schema name, because this may different on DRDS & PolarX.
        }
    }

    @Test
    public void metadataLockViewTest() throws Exception {
        final String query =
            "select * from information_schema.metadata_lock where `TABLE`='" + PRIMARY_TABLE_NAME + "'";

        // May no data, just check the column name.
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {
            final StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                builder.append(rs.getMetaData().getColumnName(i)).append(',');
            }
            Assert.assertEquals(
                "NODE,CONN_ID,TRX_ID,TRACE_ID,SCHEMA,TABLE,TYPE,DURATION,VALIDATE,FRONTEND,SQL,",
                builder.toString());
        }
    }

    @Test
    public void testGlobalIndexesFilter() throws Exception {
        final String tableName1 = PRIMARY_TABLE_NAME + "_test1";
        final String tableName2 = PRIMARY_TABLE_NAME + "_test2";
        final String tableName3 = PRIMARY_TABLE_NAME + "_test3";
        final String gsiName1 = INDEX_NAME + "_test1";
        final String gsiName2 = INDEX_NAME + "_test2";
        final String gsiName3 = INDEX_NAME + "_test3";
        final String gsiName4 = INDEX_NAME + "_test4";
        final String dropSql = "DROP TABLE IF EXISTS ";
        final String queryGsi = "SELECT * FROM information_schema.GLOBAL_INDEXES ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName3);

        try {
            // create a table with a GSI
            String sql = "CREATE TABLE " + tableName1 + " ( "
                + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                + "GLOBAL INDEX " + gsiName1
                + " using btree (g1) COVERING (c1) DBPARTITION BY HASH(g1) TBPARTITION BY HASH(g1) TBPARTITIONS 4"
                + ") DBPARTITION by hash(id)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // create another GSI
            sql = "CREATE GLOBAL INDEX " + gsiName2 + " using btree ON " + tableName1
                + " (g2) COVERING (c1, c2) DBPARTITION by HASH(g2) TBPARTITION BY HASH(g2) TBPARTITIONS 8";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // create another table with a GSI
            sql = "CREATE TABLE " + tableName2 + " ( "
                + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                + "GLOBAL INDEX " + gsiName3
                + " using btree (g2) COVERING (c1, c2) DBPARTITION BY HASH(g2) TBPARTITION BY HASH(g2) TBPARTITIONS 2"
                + ") DBPARTITION by hash(id)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // create another table with a GSI
            sql = "CREATE TABLE " + tableName3 + " ( "
                + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                + "GLOBAL INDEX " + gsiName4
                + " using btree (g2) COVERING (c1, c2) DBPARTITION BY HASH(g2) TBPARTITION BY HASH(g2) TBPARTITIONS 4"
                + ") DBPARTITION by hash(id)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // query GSI: table 1
            sql = String.format(queryGsi + " WHERE TABLE = '%s' ORDER BY KEY_NAME", tableName1);
            try (Statement statement = tddlConnection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
                List<List<String>> result = JdbcUtil.getStringResult(rs, false);
                // table 1 should contain two GSI
                Assert.assertEquals(2, result.size());
                String resultStr = String.join(",", result.get(0));
                Assert.assertTrue(resultStr.contains(tableName1 + ",1," + gsiName1 + ",g1,id, c1,BTREE,g1,HASH,"));
                Assert.assertTrue(resultStr.contains("g1,HASH,4,PUBLIC"));
                resultStr = String.join(",", result.get(1));
                Assert.assertTrue(resultStr.contains(tableName1 + ",1," + gsiName2 + ",g2,id, c1, c2,BTREE,g2,HASH,"));
                Assert.assertTrue(resultStr.contains("g2,HASH,8,PUBLIC"));
            }

            // query GSI: table 2 and table 3
            sql = String.format(queryGsi + "WHERE TABLE IN ('%s', '%s') ORDER BY KEY_NAME", tableName2, tableName3);
            try (Statement statement = tddlConnection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
                List<List<String>> result = JdbcUtil.getStringResult(rs, false);
                // should contain two GSI, one in table 2 and one in table 3
                Assert.assertEquals(2, result.size());
                String resultStr = String.join(",", result.get(0));
                Assert.assertTrue(resultStr.contains(tableName2 + ",1," + gsiName3 + ",g2,id, c1, c2,BTREE,g2,HASH,"));
                Assert.assertTrue(resultStr.contains("g2,HASH,2,PUBLIC"));
                resultStr = String.join(",", result.get(1));
                Assert.assertTrue(resultStr.contains(tableName3 + ",1," + gsiName4 + ",g2,id, c1, c2,BTREE,g2,HASH,"));
                Assert.assertTrue(resultStr.contains("g2,HASH,4,PUBLIC"));
            }

            // query GSI: all
            sql = String.format(
                queryGsi + " WHERE TABLE IN ('%s', '%s', '%s')", tableName1, tableName2, tableName3);
            try (Statement statement = tddlConnection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
                List<List<String>> result = JdbcUtil.getStringResult(rs, false);
                // four gsi in table 1, 2, 3
                Assert.assertEquals(4, result.size());
            }
        } finally {
            // drop tables
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);
        }
    }
}
