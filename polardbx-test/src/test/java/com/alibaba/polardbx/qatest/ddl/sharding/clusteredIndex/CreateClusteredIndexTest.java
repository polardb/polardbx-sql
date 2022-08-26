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

package com.alibaba.polardbx.qatest.ddl.sharding.clusteredIndex;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author lijiu
 */
public class CreateClusteredIndexTest extends DDLBaseNewDBTestCase {

    private boolean supportXA = false;

    @Before
    public void cleanEnv() throws SQLException {

        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @After
    public void clean() {

    }

    private static void checkCreateTableExecute(Connection tddlConnection, String tableName,
                                                Set<String> indexNames, Map<String, Set<String>> indexColumns,
                                                Map<String, Set<String>> coveringColumns) throws SQLException {

        // check index / covering columns exists
        try (final ResultSet resultSet = JdbcUtil
            .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(tableName), tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            final String createTable = resultSet.getString(2);
            Assert.assertTrue(TStringUtil.contains(createTable, "CLUSTERED INDEX")
                || TStringUtil.contains(createTable, "UNIQUE CLUSTERED"));

            for (Entry<String, Set<String>> entry : indexColumns.entrySet()) {
                for (String indexColumn : entry.getValue()) {
                    Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain index column "
                            + indexColumn,
                        TStringUtil.countMatches(createTable.toLowerCase(), indexColumn.toLowerCase()) > 2);
                }
            }
        }

        for (String indexName : indexNames) {
            try (final ResultSet resultSet = JdbcUtil
                .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(indexName), tddlConnection)) {
                Assert.assertTrue(resultSet.next());
                final String createTable = resultSet.getString(2);
                Assert.assertFalse("Incorrect SHOW CREATE TABLE result " + createTable,
                    TStringUtil.contains(createTable, "CLUSTERED INDEX")
                        || TStringUtil.contains(createTable, "UNIQUE CLUSTERED"));
                for (String indexColumn : indexColumns.get(indexName)) {
                    Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain index column "
                        + indexColumn, TStringUtil.contains(createTable, indexColumn));
                }

                if (!coveringColumns.isEmpty()) {
                    for (String coveringColumn : indexColumns.get(indexName)) {
                        Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain covering column "
                            + coveringColumn, TStringUtil.contains(createTable, coveringColumn));
                    }
                }
            }
        }

        try (final ResultSet resultSet = JdbcUtil
            .executeQuery("SHOW INDEX FROM " + quoteSpecialName(tableName), tddlConnection)) {
            final Map<String, Set<String>> resultIndexColumnMap = new HashMap<>();
            final Map<String, Set<String>> resultCoveringColumnMap = new HashMap<>();

            Assert.assertTrue(resultSet.next());
            do {

                final String indexName = resultSet.getString(3).toLowerCase();
                final String comment = resultSet.getString(12).toLowerCase();

                if (!TStringUtil.equalsIgnoreCase("INDEX", comment)
                    && !TStringUtil.equalsIgnoreCase("COVERING", comment)) {
                    continue;
                }

                final String indexType = resultSet.getString(11);
                final String columnName = resultSet.getString(5).toLowerCase();

                Assert.assertEquals(resultSet.getString(1), tableName);
                Assert.assertTrue("Unexpected index name " + indexName, indexNames.contains(indexName));
                Assert.assertEquals("GLOBAL", indexType);

                final Set<String> resultIndexColumns = resultIndexColumnMap.computeIfAbsent(indexName,
                    s -> new HashSet<>());
                final Set<String> resultCoveringColumns = resultCoveringColumnMap.computeIfAbsent(indexName,
                    s -> new HashSet<>());

                if (TStringUtil.equalsIgnoreCase("INDEX", comment)) {
                    resultIndexColumns.add(columnName);
                } else if (TStringUtil.equalsIgnoreCase("COVERING", comment)) {
                    resultCoveringColumns.add(columnName);
                }

                Assert.assertTrue("Unexpected index columns " + TStringUtil.join(resultIndexColumns, ","),
                    indexColumns.get(indexName).containsAll(resultIndexColumns));
                if (!coveringColumns.isEmpty()) {
                    Assert.assertTrue("Unexpected index columns " + TStringUtil.join(resultCoveringColumns, ","),
                        coveringColumns.get(indexName).containsAll(resultCoveringColumns));
                }
            } while (resultSet.next());

            for (Entry<String, Set<String>> entry : indexColumns.entrySet()) {
                final String indexName = entry.getKey();
                final Set<String> indexColumnSet = entry.getValue();
                Assert.assertTrue("Unexpected index columns " + TStringUtil.join(indexColumnSet, ","),
                    indexColumnSet.equals(resultIndexColumnMap.get(indexName)));

            }
            if (!coveringColumns.isEmpty()) {
                for (Entry<String, Set<String>> entry : coveringColumns.entrySet()) {
                    final String indexName = entry.getKey();
                    final Set<String> coveringColumnSet = entry.getValue();
                    Assert.assertTrue("Unexpected index columns " + TStringUtil.join(coveringColumnSet, ","),
                        coveringColumnSet.equals(resultCoveringColumnMap.get(indexName)));
                }
            }

        }

    }

    @Test
    public void testCreate1MultiDb() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`seller_id`) dbpartition by hash(`seller_id`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`buyer_id`) dbpartition by hash(`buyer_id`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);
            checkCreateTableExecute(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "buyer_id", "order_snapshot"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "seller_id", "order_snapshot")));
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void testCreate2MultiDb() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`seller_id`,`buyer_id`) dbpartition by hash(`seller_id`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`buyer_id`,`seller_id`) dbpartition by hash(`buyer_id`);",
            gsiTestTableName, gsiTestUkName);

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);
            checkCreateTableExecute(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id", "buyer_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id", "seller_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "order_snapshot"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "order_snapshot")));
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void testCreateDiffCase() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`seller_Id`,`buyer_iD`) dbpartition by hash(`sEller_id`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`buyeR_id`,`selLer_id`) dbpartition by hash(`bUyer_id`);",
            gsiTestTableName, gsiTestUkName);

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);
            checkCreateTableExecute(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id", "buyer_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id", "seller_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "order_snapshot"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "order_snapshot")));
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

}
