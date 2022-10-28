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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * @author chenmo.cm
 */
public class CreateTableWithGsiTest extends DDLBaseNewDBTestCase {

    private final String HINT_GSI_ON_CURRENT_TIMESTAMP =
        "/*+TDDL:CMD_EXTRA(GSI_ON_UPDATE_CURRENT_TIMESTAMP = FALSE, GSI_DEFAULT_CURRENT_TIMESTAMP = FALSE)*/";
    private boolean supportXA = false;

    @Before
    public void cleanEnv() throws SQLException {

        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @After
    public void clean() {

    }

    private static void checkCreateTableExecute(Connection tddlConnection, final String sql, String tableName,
                                                Set<String> indexNames, Map<String, Set<String>> indexColumns,
                                                Map<String, Set<String>> coveringColumns) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI + sql);

        // check index / covering columns exists
        try (final ResultSet resultSet = JdbcUtil
            .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(tableName), tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            final String createTable = resultSet.getString(2);
            Assert.assertTrue(TStringUtil.contains(createTable, "GLOBAL INDEX")
                || TStringUtil.contains(createTable, "UNIQUE GLOBAL"));

            for (Entry<String, Set<String>> entry : indexColumns.entrySet()) {
                for (String indexColumn : entry.getValue()) {
                    Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain index column "
                        + indexColumn, TStringUtil.countMatches(createTable, indexColumn) > 2);
                }
            }

            if (!coveringColumns.isEmpty()) {
                for (Entry<String, Set<String>> entry : coveringColumns.entrySet()) {
                    for (String coveringColumn : entry.getValue()) {
                        Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain covering column "
                            + coveringColumn, TStringUtil.countMatches(createTable, coveringColumn) > 1);
                    }
                }
            }
        }

        for (String indexName : indexNames) {
            try (final ResultSet resultSet = JdbcUtil
                .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(indexName), tddlConnection)) {
                Assert.assertTrue(resultSet.next());
                final String createTable = resultSet.getString(2);
                Assert.assertFalse("Incorrect SHOW CREATE TABLE result " + createTable,
                    TStringUtil.contains(createTable, "GLOBAL INDEX")
                        || TStringUtil.contains(createTable, "UNIQUE GLOBAL"));
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

                final String indexName = resultSet.getString(3);
                final String comment = resultSet.getString(12);

                if (!TStringUtil.equalsIgnoreCase("INDEX", comment)
                    && !TStringUtil.equalsIgnoreCase("COVERING", comment)) {
                    continue;
                }

                final String indexType = resultSet.getString(11);
                final String columnName = resultSet.getString(5);

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
    public void testCreate1_multi_db() {
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
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "\t`rint` double ( 10, 2 ),\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`seller_id`) DBPARTITION BY HASH (`seller_id`) TBPARTITION BY hash(`seller_id`) TBPARTITIONS 4,\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `order_detail`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection, gsiTestTable, gsiTestTableName);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate2_multi_tb() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`seller_id`) DBPARTITION BY HASH (`seller_id`) TBPARTITION BY hash(`seller_id`) TBPARTITIONS 4,\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `order_detail`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection, gsiTestTable, gsiTestTableName);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate3_multi_tb() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`gmt_create` timestamp NOT NULL DEFAULT \"2019-01-27 18:23:00\",\n"
            + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`seller_id`) DBPARTITION BY HASH (`seller_id`) TBPARTITION BY hash(`seller_id`) TBPARTITIONS 4,\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `order_detail`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by week(`gmt_create`) tbpartitions 7;\n";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection, gsiTestTable, gsiTestTableName);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate4_error_using_current_timestamp() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestUkName = "g_i_test_buyer";
        final List<String> forbiddenTimestampColumns = ImmutableList
            .of("\t`gmt_modified` timestamp not null default current_timestamp on update current_timestamp,\n",
                "\t`gmt_modified` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),\n",
                "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n",
                "\t`gmt_modified` timestamp DEFAULT CURRENT_TIMESTAMP,\n",
                "\t`gmt_modified` timestamp NULL ON UPDATE CURRENT_TIMESTAMP,\n",
                "\t`gmt_modified` timestamp NULL,\n");
        final List<String> allowedTimestampColumns = isMySQL80() ? ImmutableList.of(
            "\t`gmt_modified` timestamp NULL DEFAULT NULL,\n") : ImmutableList.of(
            "\t`gmt_modified` timestamp NOT NULL DEFAULT 0,\n",
            "\t`gmt_modified` timestamp NOT NULL DEFAULT \"000-00-00 00:00:00\",\n",
            "\t`gmt_modified` timestamp NULL DEFAULT NULL,\n");
        final String gsiTestTablePart1 = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n";
        final String gsiTestTablePart2 = "\t`gmt_create` timestamp DEFAULT \"2019-01-27 18:23:00\",\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `gmt_modified`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by week(`gmt_create`) tbpartitions 2;\n";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestUkName));
            for (String forbiddenTimestampColumn : forbiddenTimestampColumns) {
                JdbcUtil
                    .executeUpdateFailed(tddlConnection,
                        HINT_GSI_ON_CURRENT_TIMESTAMP
                            + HINT_CREATE_GSI + gsiTestTablePart1 + forbiddenTimestampColumn
                            + gsiTestTablePart2, "CURRENT_TIMESTAMP");
            }
            for (String timestampColumn : allowedTimestampColumns) {
                dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestUkName));
                checkCreateTableExecute(tddlConnection,
                    gsiTestTablePart1 + timestampColumn + gsiTestTablePart2,
                    gsiTestTableName,
                    ImmutableSet.of(gsiTestUkName),
                    ImmutableMap.of(gsiTestUkName, ImmutableSet.of("buyer_id")),
                    ImmutableMap.of(gsiTestUkName,
                        ImmutableSet.of("order_id", "id", "order_snapshot", "gmt_modified", "gmt_create")));
            }
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate5_error_no_pk() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n");
            // Will success because of the implicit PK.
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));

            checkCreateTableExecute(tddlConnection,
                "CREATE TABLE `gsi_test_table` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    PRIMARY KEY (`id`), \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n",
                gsiTestTableName,
                ImmutableSet.of(gsiTestIndexName),
                ImmutableMap.of(gsiTestIndexName, ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestIndexName, ImmutableSet.of("order_id", "id", "order_snapshot")));

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            checkCreateTableExecute(tddlConnection,
                "CREATE TABLE `gsi_test_table` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n",
                gsiTestTableName,
                ImmutableSet.of(gsiTestIndexName),
                ImmutableMap.of(gsiTestIndexName, ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestIndexName, ImmutableSet.of("order_id", "id", "order_snapshot")));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate6_error_single_primary_table() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8;\n", null,
                "Creating Global Secondary Index on single or broadcast table not support yet");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 broadcast;\n", null,
                "Creating Global Secondary Index on single or broadcast table not support yet");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate7_error_storage_check() {
        if (supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `gsi_test_table` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                    + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Cannot create global secondary index, MySQL 5.7 or higher version is needed");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate7_error_duplicate_index_name() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI +
                "CREATE TABLE `gsi_test_table` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n");

            executeErrorAssert(tddlConnection,
                "CREATE TABLE `gsi_test_table2` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n" + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n" + "    GLOBAL INDEX " + gsiTestIndexName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH " + "(`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Global Secondary Index '" + gsiTestIndexName + "' already exists");

            executeErrorAssert(tddlConnection,
                "CREATE TABLE `gsi_test_table2` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    INDEX g_i_buyer_id(`buyer_id`), \n"
                    + "    GLOBAL INDEX g_i_buyer_id(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Duplicate index name 'g_i_buyer_id'");

            executeErrorAssert(tddlConnection,
                "CREATE TABLE `gsi_test_table2` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n" + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    INDEX g_i_buyer_id(`buyer_id`), \n"
                    + "    GLOBAL INDEX gsi_test_table2(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Global Secondary Index 'gsi_test_table2' already exists");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate8_error_index_contains_sharding_columns() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n" + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n" + "    GLOBAL INDEX " + gsiTestIndexName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    UNIQUE GLOBAL INDEX " + gsiTestIndexName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "The index columns of global secondary index must contains all the sharding columns of index table");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate9_error_index_contains_sharding_columns() {
        final String gsiTestTableName = "gsi_test_table";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n" + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n" + "    INDEX i_buyer_id(`buyer_id`), \n"
                    + "    GLOBAL INDEX g_i_buyer_id(`buyer_id`) COVERING (`order_snapshot`) "
                    + "DBPARTITION BY HASH (`buyer_id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 3 \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    INDEX i_buyer_id(`buyer_id`), \n"
                    + "    UNIQUE GLOBAL INDEX g_i_buyer_id(`buyer_id`) COVERING (`order_snapshot`) "
                    + "DBPARTITION BY HASH (`buyer_id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 3 \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "The index columns of global secondary index must contains all the sharding columns of index table");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate10_index_contains_sharding_columns() {
        final String gsiTestTableName = "gsi_test_table";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI +
                "CREATE TABLE `"
                + gsiTestTableName
                + "` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    INDEX i_buyer_id(`buyer_id`), \n"
                + "    GLOBAL INDEX g_i_buyer_id(`buyer_id`, `id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI +
                "CREATE TABLE `"
                + gsiTestTableName
                + "` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    INDEX i_buyer_id(`buyer_id`), \n"
                + "    UNIQUE GLOBAL INDEX g_i_buyer_id(`buyer_id`, `id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate11_error_duplicated_column_name() {
        final String gsiTestTableName = "gsi_test_table";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            executeErrorAssert(tddlConnection, HINT_CREATE_GSI +
                    "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    INDEX i_buyer_id(`buyer_id`), \n"
                    + "    GLOBAL INDEX g_i_buyer_id(`buyer_id`, `id`) COVERING (`order_snapshot`, `order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Duplicate column name 'order_snapshot' in index 'g_i_buyer_id'");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            executeErrorAssert(tddlConnection, HINT_CREATE_GSI +
                    "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    INDEX i_buyer_id(`buyer_id`), \n"
                    + "    UNIQUE GLOBAL INDEX g_i_buyer_id(`buyer_id`, `order_snapshot`) COVERING (`order_snapshot`, id) DBPARTITION BY HASH (`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "Duplicate column name 'order_snapshot' in index 'g_i_buyer_id'");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate12_multi_db() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "\t`rint` REAL ( 10, 2 ) REFERENCES tt1 ( rint ) MATCH FULL ON DELETE RESTRICT,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`seller_id`) DBPARTITION BY HASH (`seller_id`) TBPARTITION BY hash(`seller_id`) TBPARTITIONS 4,\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `order_detail`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("order_id", "id"),
                    gsiTestUkName,
                    ImmutableSet.of("order_id", "id", "order_snapshot", "order_detail")));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate13() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `gmt_create` datetime NOT NULL ,\n"
            + "        `gmt_modified` datetime NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n"
            + "        `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `product_key` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `is_deleted` tinyint(4) NOT NULL DEFAULT '0' ,\n"
            + "        `tenant_id` bigint(20) UNSIGNED DEFAULT '0' ,\n"
            + "        `rbac_tenant_id` varchar(64) DEFAULT '0' ,\n"
            + "        `thing_type` varchar(64) DEFAULT NULL ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE `uk_iot_id` (`iot_id`),\n"
            + "        UNIQUE `uk_name_pk` (`name`, `product_key`),\n"
            + "        GLOBAL INDEX " + gsiTestKeyName
            + "(`rbac_tenant_id`) COVERING (`id`, `product_key`) DBPARTITION BY HASH(`rbac_tenant_id`),\n"
            + "        UNIQUE GLOBAL " + gsiTestUkName
            + " (`iot_id`) COVERING (`id`, `product_key`, `is_deleted`, `thing_type`) DBPARTITION BY HASH(`iot_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 CHARSET = utf8mb4 COMMENT '设备表' dbpartition by hash"
            + "(`product_key`) tbpartition by hash(`product_key`) tbpartitions 20";

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("rbac_tenant_id"),
                    gsiTestUkName,
                    ImmutableSet.of("iot_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "product_key"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "product_key", "is_deleted", "thing_type")));
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select * from " + gsiTestTableName + " FORCE INDEX (" + gsiTestUkName
                    + ")  where iot_id = 'W2FBh2OKV9e7zkrjGknr000999' ;");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate14_with_special_name() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi-test_table";
        final String gsiTestKeyName = "g_i-test_seller";
        final String gsiTestUkName = "g_i-test_buyer";
        final String gsiTestTable = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `gmt_create` datetime NOT NULL ,\n"
            + "        `gmt_modified` datetime NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n"
            + "        `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `product_key` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `is_deleted` tinyint(4) NOT NULL DEFAULT '0' ,\n"
            + "        `tenant_id` bigint(20) UNSIGNED DEFAULT '0' ,\n"
            + "        `rbac_tenant_id` varchar(64) DEFAULT '0' ,\n"
            + "        `thing_type` varchar(64) DEFAULT NULL ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE `uk_iot_id` (`iot_id`),\n"
            + "        UNIQUE `uk_name_pk` (`name`, `product_key`),\n"
            + "        GLOBAL INDEX `" + gsiTestKeyName
            + "` (`rbac_tenant_id`) COVERING (`id`, `product_key`) DBPARTITION BY HASH(`rbac_tenant_id`),\n"
            + "        UNIQUE GLOBAL `" + gsiTestUkName
            + "` (`iot_id`) COVERING (`id`, `product_key`, `is_deleted`, `thing_type`) DBPARTITION BY HASH(`iot_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 CHARSET = utf8mb4 COMMENT '设备表' dbpartition by hash"
            + "(`product_key`) tbpartition by hash(`product_key`) tbpartitions 20";

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("rbac_tenant_id"),
                    gsiTestUkName,
                    ImmutableSet.of("iot_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "product_key"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "product_key", "is_deleted", "thing_type")));
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select * from `" + gsiTestTableName + "` FORCE INDEX (`" + gsiTestUkName
                    + "`)  where iot_id = 'W2FBh2OKV9e7zkrjGknr000999' ;");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate15_long_column_name() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `gmt_create` datetime NOT NULL ,\n"
            + "        `gmt_modified` datetime NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n"
            + "        `vary_long_column_name_abc` varchar(64) NOT NULL ,\n"
            + "        `another_vary_long_column_name_def` varchar(64) NOT NULL ,\n"
            + "        `finally_one_column_with_the_vary_length_of_45` varchar(64) NOT NULL ,\n"
            + "        `finally_one_column_with_the_vary_length_of_64_aaaaaaaaaaaaaaaaaa` varchar(64) NOT NULL ,\n"
            + "        `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `product_key` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `is_deleted` tinyint(4) NOT NULL DEFAULT '0' ,\n"
            + "        `tenant_id` bigint(20) UNSIGNED DEFAULT '0' ,\n"
            + "        `rbac_tenant_id` varchar(64) DEFAULT '0' ,\n"
            + "        `thing_type` varchar(64) DEFAULT NULL ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE `uk_iot_id` (`iot_id`),\n"
            + "        UNIQUE `uk_name_pk` (`name`, `product_key`),\n"
            + "        GLOBAL INDEX `" + gsiTestKeyName
            + "` (`vary_long_column_name_abc`, `finally_one_column_with_the_vary_length_of_45`) COVERING (`id`, `product_key`)"
            + " DBPARTITION BY HASH(`vary_long_column_name_abc`) TBPARTITION BY HASH(`finally_one_column_with_the_vary_length_of_45`) TBPARTITIONS 3,\n"
            + "        UNIQUE GLOBAL `" + gsiTestUkName
            + "` (`finally_one_column_with_the_vary_length_of_64_aaaaaaaaaaaaaaaaaa`) COVERING (`id`, `product_key`, `is_deleted`, `thing_type`) DBPARTITION BY HASH(`finally_one_column_with_the_vary_length_of_64_aaaaaaaaaaaaaaaaaa`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 CHARSET = utf8mb4 COMMENT '设备表' dbpartition by hash"
            + "(`product_key`) tbpartition by hash(`product_key`) tbpartitions 20";

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("vary_long_column_name_abc", "finally_one_column_with_the_vary_length_of_45"),
                    gsiTestUkName,
                    ImmutableSet.of("finally_one_column_with_the_vary_length_of_64_aaaaaaaaaaaaaaaaaa")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "product_key"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "product_key", "is_deleted", "thing_type")));
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select * from `" + gsiTestTableName + "` FORCE INDEX (`" + gsiTestUkName
                    + "`)  where iot_id = 'W2FBh2OKV9e7zkrjGknr000999' ;");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate16_with_backtick_name() {
        if (!supportXA) {
            return;
        }

        // Not double backtick here means single backtick, because we use backtick to pack names in routines.
        final String gsiTestTableName = "`gsi-`test_table`";
        final String gsiTestKeyName = "`g_i-`test_seller`";
        final String gsiTestUkName = "`g_i-`test_buyer`";
        final String gsiTestTable = "CREATE TABLE " + quoteSpecialName(gsiTestTableName) + " (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `gmt_create` datetime NOT NULL ,\n"
            + "        `gmt_modified` datetime NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n"
            + "        `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `product_key` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `is_deleted` tinyint(4) NOT NULL DEFAULT '0' ,\n"
            + "        `tenant_id` bigint(20) UNSIGNED DEFAULT '0' ,\n"
            + "        `rbac_tenant_id` varchar(64) DEFAULT '0' ,\n"
            + "        `thing_type` varchar(64) DEFAULT NULL ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE `uk_iot_id` (`iot_id`),\n"
            + "        UNIQUE `uk_name_pk` (`name`, `product_key`),\n"
            + "        GLOBAL INDEX " + quoteSpecialName(gsiTestKeyName)
            + " (`rbac_tenant_id`) COVERING (`id`, `product_key`) DBPARTITION BY HASH(`rbac_tenant_id`),\n"
            + "        UNIQUE GLOBAL " + quoteSpecialName(gsiTestUkName)
            + " (`iot_id`) COVERING (`id`, `product_key`, `is_deleted`, `thing_type`) DBPARTITION BY HASH(`iot_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 CHARSET = utf8mb4 COMMENT '设备表' dbpartition by hash"
            + "(`product_key`) tbpartition by hash(`product_key`) tbpartitions 20";

        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("rbac_tenant_id"),
                    gsiTestUkName,
                    ImmutableSet.of("iot_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "product_key"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "product_key", "is_deleted", "thing_type")));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into " + quoteSpecialName(gsiTestTableName)
                    + " (gmt_create,gmt_modified,iot_id,name,product_key,rbac_tenant_id) values (now(),now(),\"W2FBh2OKV9e7zkrjGknr000999\",\"name\",\"product\",\"rbac_tenant_id\");");
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select * from " + quoteSpecialName(gsiTestTableName) + " FORCE INDEX (" + quoteSpecialName(
                    gsiTestUkName) + ")  where iot_id = 'W2FBh2OKV9e7zkrjGknr000999' ;");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    /***
     * Let DN rather CN validate whether the GSI key is too long.
     * The following test cases depend on CN's validation, so ignore them.
     */
    @Ignore
    @Test
    public void testCreate17ColumnTooLong() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_etest_buyer";
        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            // 表定义中长度超过3072字节，索引定义中加上长度且不大于767字节，建表成功
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "     id int(11) NOT NULL primary key, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(1025) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX " + gsiTestIndexName + "(`buyer_id`(767)) COVERING (`order_snapshot`) "
                    + "    DBPARTITION BY HASH(`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            // 表定义中长度超过3072字节，索引定义不加长度，使用默认值，超长报错
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "     id int(11) NOT NULL primary key, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(1025) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX " + gsiTestIndexName + "(`buyer_id`) COVERING (`order_snapshot`) "
                    + "    DBPARTITION BY HASH(`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "too long, max key length is");

            // 测试UK的情况
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "     id int(11) NOT NULL primary key, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(1025) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    UNIQUE GLOBAL INDEX " + gsiTestIndexName + "(`buyer_id`(1025)) COVERING (`order_snapshot`) "
                    + "    DBPARTITION BY HASH(`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n", null,
                "too long, max key length is");

            // 测试建表时对列指定字符集, latin1字符集每字符1字节，下面建表不会报错
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "     id int(11) NOT NULL primary key, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(767) CHARACTER SET latin1 DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    GLOBAL INDEX " + gsiTestIndexName + "(`buyer_id`) COVERING (`order_snapshot`) "
                    + "    DBPARTITION BY HASH(`buyer_id`) \n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 dbpartition by hash(`order_id`);\n");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            // 指定列为utf8, 即使默认为latin1, 1025个字符长度也会超过3072字节
            executeErrorAssert(tddlConnection,
                "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "     id int(11) NOT NULL primary key, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(1025) CHARACTER SET utf8 DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    UNIQUE GLOBAL INDEX " + gsiTestIndexName + "(`buyer_id`(3073)) COVERING (`order_snapshot`) "
                    + "    DBPARTITION BY HASH(`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = latin1 dbpartition by hash(`order_id`);\n", null,
                "too long, max key length is");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate18DuplicatedGlobalSecondaryUniqueIndex() {

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`shop` varchar(8) NOT NULL,\n"
            + "\t`order_no` varchar(8) NOT NULL,\n"
            + "\t`other` varchar(20) NULL,\n"
            + "\t`create_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "\t`update_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tUNIQUE KEY `uk_orderno` (`order_no`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`order_no`) COVERING (`id`) DBPARTITION BY HASH (`order_no`),\n"
            + "\tUNIQUE GLOBAL KEY "
            + gsiTestUkName
            + "(`order_no`) COVERING (`id`, `shop`) DBPARTITION BY HASH (`order_no`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by UNI_HASH(`shop`) tbpartition BY UNI_HASH(`shop`);\n";
        final String gsiTestKeyTableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `id` bigint(11) NOT NULL,\n"
                + "  `shop` varchar(8) NOT NULL,\n"
                + "  `order_no` varchar(8) NOT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `uk_orderno` (`order_no`),\n"
                + "  KEY `auto_shard_key_shop` (`shop`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`order_no`)",
            gsiTestKeyName);
        final String gsiTestUKeyTableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `id` bigint(11) NOT NULL,\n"
                + "  `shop` varchar(8) NOT NULL,\n"
                + "  `order_no` varchar(8) NOT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `uk_orderno` (`order_no`) USING BTREE,\n"
                + "  KEY `auto_shard_key_shop` (`shop`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`order_no`)",
            gsiTestUkName);
        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("order_no"),
                    gsiTestUkName,
                    ImmutableSet.of("order_no")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "shop"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "shop")));
            final TableChecker gsiKeyTableChecker = getTableChecker(tddlConnection, gsiTestKeyName);
            gsiKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestKeyTableDef, true, Litmus.THROW);
            final TableChecker gsiUKeyTableChecker = getTableChecker(tddlConnection, gsiTestUkName);
            gsiUKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestUKeyTableDef, true, Litmus.THROW);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate19CreateGsiWithIndex() {

        final String gsiTestTableName = "idx_primary_table";
        final String gsiTestKeyName = "idx_primary_table_gsi";
        final String gsiTestUkName = "idx_primary_table_ugsi";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(20) DEFAULT NULL,\n"
            + "\t`c2` bigint(20) DEFAULT NULL,\n"
            + "\t`c3` bigint(20) DEFAULT NULL,\n"
            + "\t`c4` bigint(20) DEFAULT NULL,\n"
            + "\t`c5` bigint(20) DEFAULT NULL,\n"
            + "\t`create_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "\t`update_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
            + "\tPRIMARY KEY (`pk`),\n"
            + "\tINDEX c3_4(`c3`,`c4`),\n"
            + "\tGLOBAL INDEX "
            + gsiTestKeyName
            + "(`c3`,`c4`) DBPARTITION BY HASH (`c3`),\n"
            + "\tUNIQUE GLOBAL KEY "
            + gsiTestUkName
            + "(`c3`,`c4`) DBPARTITION BY HASH (`c3`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by HASH(`c1`) tbpartition BY HASH(`c2`) tbpartitions 2;\n";
        final String gsiTestKeyTableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c4` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `c3_4` (`c3`,`c4`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c3`)",
            gsiTestKeyName);
        final String gsiTestUKeyTableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c4` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  UNIQUE KEY `auto_shard_key_c3_c4` (`c3`,`c4`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c3`)",
            gsiTestUkName);
        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
            checkCreateTableExecute(tddlConnection,
                gsiTestTable,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c3", "c4"),
                    gsiTestUkName,
                    ImmutableSet.of("c3", "c4")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c2"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2")));
            final TableChecker gsiKeyTableChecker = getTableChecker(tddlConnection, gsiTestKeyName);
            gsiKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestKeyTableDef, true, Litmus.THROW);
            final TableChecker gsiUKeyTableChecker = getTableChecker(tddlConnection, gsiTestUkName);
            gsiUKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestUKeyTableDef, true, Litmus.THROW);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate20_CreateGsiWithDupCol() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `gmt_create` datetime NOT NULL ,\n"
            + "        `gmt_modified` datetime NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n"
            + "        `iot_id` varchar(64) NOT NULL ,\n" // Duplicate column.
            + "        `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `product_key` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL ,\n"
            + "        `is_deleted` tinyint(4) NOT NULL DEFAULT '0' ,\n"
            + "        `tenant_id` bigint(20) UNSIGNED DEFAULT '0' ,\n"
            + "        `rbac_tenant_id` varchar(64) DEFAULT '0' ,\n"
            + "        `thing_type` varchar(64) DEFAULT NULL ,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        UNIQUE `uk_iot_id` (`iot_id`),\n"
            + "        UNIQUE `uk_name_pk` (`name`, `product_key`),\n"
            + "        GLOBAL INDEX `" + gsiTestKeyName
            + "` (`rbac_tenant_id`) COVERING (`id`, `product_key`) DBPARTITION BY HASH(`rbac_tenant_id`),\n"
            + "        UNIQUE GLOBAL `" + gsiTestUkName
            + "` (`iot_id`) COVERING (`id`, `product_key`, `is_deleted`, `thing_type`) DBPARTITION BY HASH(`iot_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 CHARSET = utf8mb4 COMMENT '设备表' dbpartition by hash"
            + "(`product_key`) tbpartition by hash(`product_key`) tbpartitions 20";

        dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        JdbcUtil.executeUpdateFailed(tddlConnection, gsiTestTable, "Duplicate column name 'iot_id'");
    }

    @Test
    public void testCreate21_CreateGsiWithMultiColumn() {
        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "test_index2";
        final String gsiTestKeyName = "gidx_k12";
        final String gsiTestKeyName2 = "gidx_k212";
        final String gsiTestTableDef = "CREATE TABLE `" + gsiTestTableName + "` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `k1` int(11) NOT NULL,\n"
            + "  `k2` int(11) NOT NULL,\n"
            + "  `value` char(40) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  GLOBAL INDEX `" + gsiTestKeyName + "`(`k1`) COVERING (`id`) DBPARTITION BY HASH(`k1`),\n"
            + "  GLOBAL INDEX `" + gsiTestKeyName2 + "`(`k2`, `k1`) COVERING (`id`) DBPARTITION BY HASH(`k2`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = latin1  dbpartition by hash(`id`)";

        final String gsiTestKeyTableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`id` int(11) NOT NULL,\n"
                + "\t`k1` int(11) NOT NULL,\n"
                + "\tPRIMARY KEY (`id`),\n"
                + "\tKEY `auto_shard_key_k1` USING BTREE (`k1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = latin1  dbpartition by hash(`k1`)",
            gsiTestKeyName);
        final String gsiTestKeyTableDef2 = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`id` int(11) NOT NULL,\n"
                + "\t`k1` int(11) NOT NULL,\n"
                + "\t`k2` int(11) NOT NULL,\n"
                + "\tPRIMARY KEY (`id`),\n"
                + "\tKEY `auto_shard_key_k2` USING BTREE (`k2`),\n"
                + "\tKEY `i_k2_k1` USING BTREE (`k2`, `k1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = latin1  dbpartition by hash(`k2`)",
            gsiTestKeyName2);
        try {

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestKeyName2));
            checkCreateTableExecute(tddlConnection,
                gsiTestTableDef,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestKeyName2),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("k1"),
                    gsiTestKeyName2,
                    ImmutableSet.of("k2", "k1")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id"),
                    gsiTestKeyName2,
                    ImmutableSet.of("id")));
            final TableChecker gsiKeyTableChecker = getTableChecker(tddlConnection, gsiTestKeyName);
            gsiKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestKeyTableDef, true, Litmus.THROW);
            final TableChecker gsiUKeyTableChecker = getTableChecker(tddlConnection, gsiTestKeyName2);
            gsiUKeyTableChecker.identicalTableDefinitionAndKeysTo(gsiTestKeyTableDef2, true, Litmus.THROW);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate22_indexNameTooLong() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "finally_one_column_with_the_vary_length_of_64_aaaaaaaaaaaaaaaaaaa";
        dropTableWithGsi(gsiTestTableName, ImmutableList.of());
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                + "    id int(11) NOT NULL primary key, \n"
                + "    `c1` varchar(20) DEFAULT NULL, \n"
                + "    GLOBAL INDEX " + gsiTestIndexName + "(`c1`) DBPARTITION BY HASH(`c1`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`id`);\n", "too long (max = 64)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                + "    id int(11) NOT NULL primary key, \n"
                + "    `c1` varchar(20) DEFAULT NULL, \n"
                + "    UNIQUE GLOBAL INDEX " + gsiTestIndexName + "(`c1`) DBPARTITION BY HASH(`c1`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`id`);\n", "too long (max = 64)");

    }
}
