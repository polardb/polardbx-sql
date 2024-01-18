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
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static org.hamcrest.Matchers.is;

/**
 * @author chenmo.cm
 */
public class CreateGsiTest extends DDLBaseNewDBTestCase {

    private static final String GSI_PRIMARY_TABLE_NAME = "gsi_primary_table";
    private static final String CREATE_TABLE_BASE = "CREATE TABLE IF NOT EXISTS `"
        + GSI_PRIMARY_TABLE_NAME
        + "` (\n"
        + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n"
        + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) COLLATE utf8_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n" + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n" + "\t`lb` longblob,\n"
        + "\t`tt` tinytext COLLATE utf8_bin,\n"
        + "\t`mt` mediumtext COLLATE utf8_bin,\n"
        + "\t`lt` longtext COLLATE utf8_bin,\n"
        + "\t`en` enum('1', '2') COLLATE utf8_bin NOT NULL,\n"
        + "\t`st` set('5', '6') COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING HASH (`id1`),\n"
        + "\tKEY `idx2` USING HASH (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`)";
    private static final String CREATE_TABLE_TAIL_DB =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 );\n";

    private static final String CREATE_TABLE_TAIL_TB =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 ) tbpartition BY HASH (id1) tbpartitions 3;\n";

    private static final String CREATE_PRIMARY_TABLE = HINT_CREATE_GSI + CREATE_TABLE_BASE + CREATE_TABLE_TAIL_TB;

    private static final String CREATE_TABLE_BASE_8 = "CREATE TABLE IF NOT EXISTS `" + GSI_PRIMARY_TABLE_NAME
        + "` (\n" + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n" + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n" + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n" + "\t`lb` longblob,\n"
        + "\t`tt` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`mt` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`lt` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`en` enum('1', '2') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
        + "\t`st` set('5', '6') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING BTREE (`id1`),\n"
        + "\tKEY `idx2` USING BTREE (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`)";
    private static final String CREATE_TABLE_TAIL_DB_8 =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 );\n";

    private static final String CREATE_TABLE_TAIL_TB_8 =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 ) tbpartition BY HASH (id1) tbpartitions 3;\n";

    private static final String CREATE_PRIMARY_TABLE_8 = HINT_CREATE_GSI + CREATE_TABLE_BASE_8 + CREATE_TABLE_TAIL_TB_8;

    private boolean supportXA = false;

    @Before
    public void init() throws SQLException {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + GSI_PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, isMySQL80() ? CREATE_PRIMARY_TABLE_8 : CREATE_PRIMARY_TABLE);

        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @After
    public void clean() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + GSI_PRIMARY_TABLE_NAME);
    }

    @Test
    public void createGsi1_simple() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition = " (`id2`) dbpartition by hash(`id2`)";
        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI + "CREATE GLOBAL INDEX `" + gsiName + "` ON "
                + GSI_PRIMARY_TABLE_NAME + gsiPartition);

            final TableChecker tableChecker = getTableChecker(tddlConnection, GSI_PRIMARY_TABLE_NAME);

            tableChecker.identicalTableDefinitionTo(buildCreateTable("GLOBAL INDEX `" + gsiName + "`" + gsiPartition),
                true,
                Litmus.THROW);
        }
    }

    @Test
    public void createGsi2_error_no_pk() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            String gsiCreateSql =
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ";
            // There is an implicit primary key on primary table.
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiCreateSql);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    PRIMARY KEY (`id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            if (PropertiesUtil.enableAsyncDDL) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    HINT_CREATE_GSI + "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                        + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            if (PropertiesUtil.enableAsyncDDL) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    HINT_CREATE_GSI + "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                        + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ");
            }

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void createGsi3_error_single_primary_table() {
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
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4;\n");

            executeErrorAssert(tddlConnection,
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`)", null,
                "Does not support create Global Secondary Index on single or broadcast table");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 broadcast;\n");

            executeErrorAssert(tddlConnection,
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "CREATE GLOBAL INDEX g_i_test_buyer ON " + gsiTestTableName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`)", null,
                "Does not support create Global Secondary Index on single or broadcast table");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void createGsi4_error_storage_check() {
        if (supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");
            executeErrorAssert(tddlConnection,
                "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n", null,
                "Cannot create global secondary index, MySQL 5.7 or higher version is needed");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void createGsi5_error_duplicate_index_name() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI +
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    INDEX g_i_buyer_id2(`buyer_id`), \n"
                + "    GLOBAL INDEX " + gsiTestIndexName
                + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            executeErrorAssert(tddlConnection, HINT_CREATE_GSI +
                    "CREATE GLOBAL INDEX " + gsiTestIndexName + " ON " + gsiTestTableName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)", null,
                "Global Secondary Index '" + gsiTestIndexName + "' already exists");

            executeErrorAssert(tddlConnection, HINT_CREATE_GSI +
                    "CREATE GLOBAL INDEX g_i_buyer_id2 ON " + gsiTestTableName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)", null,
                "Duplicate index name 'g_i_buyer_id2'",
                "ALREADY EXISTS");

            executeErrorAssert(tddlConnection, HINT_CREATE_GSI +
                    "CREATE GLOBAL INDEX " + gsiTestTableName + " ON " + gsiTestTableName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)", null,
                "Global Secondary Index '" + gsiTestTableName + "' already exists");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void createGsi6_error_index_contains_sharding_columns() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition = " (`id2`) dbpartition by hash(`id3`)";
        final String gsiPartition1 = " (`id2`) dbpartition by hash(`id2`) tbpartition by hash(id3) tbpartitions 3";

        try {
            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "CREATE GLOBAL INDEX `" + gsiName + "` ON " + GSI_PRIMARY_TABLE_NAME
                    + gsiPartition, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "CREATE UNIQUE GLOBAL INDEX `" + gsiName + "` ON " + GSI_PRIMARY_TABLE_NAME
                    + gsiPartition, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection, HINT_CREATE_GSI + "CREATE GLOBAL INDEX `" + gsiName + "` ON "
                    + GSI_PRIMARY_TABLE_NAME + gsiPartition1, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");
        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void createGsi7_error_duplicated_column_name() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition =
            " (`id2`, `id3`) covering(id3, vc1) dbpartition by hash(`id2`) tbpartition by hash(id3) tbpartitions 3";

        try {
            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "CREATE GLOBAL INDEX `" + gsiName + "` ON " + GSI_PRIMARY_TABLE_NAME
                    + gsiPartition, null, "Duplicate column name 'id3' in index 'gsi_create_1'");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "CREATE UNIQUE GLOBAL INDEX `" + gsiName + "` ON " + GSI_PRIMARY_TABLE_NAME
                    + gsiPartition, null, "Duplicate column name 'id3' in index 'gsi_create_1'");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi1_simple() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition = " (`id2`) dbpartition by hash(`id2`)";
        if (PropertiesUtil.enableAsyncDDL) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME
                + "` ADD GLOBAL INDEX " + gsiName + gsiPartition);

            final TableChecker tableChecker = getTableChecker(tddlConnection, GSI_PRIMARY_TABLE_NAME);

            tableChecker.identicalTableDefinitionTo(buildCreateTable("GLOBAL INDEX `" + gsiName + "`" + gsiPartition),
                true,
                Litmus.THROW);
        }
    }

    @Test
    public void addGsi2_error_no_pk() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            String gsiCreateSql =
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "ALTER TABLE " + gsiTestTableName + " ADD UNIQUE GLOBAL INDEX " + gsiTestIndexName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ";
            // There is an implicit primary key on primary table.
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiCreateSql);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    PRIMARY KEY (`id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            if (PropertiesUtil.enableAsyncDDL) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    HINT_CREATE_GSI + "ALTER TABLE " + gsiTestTableName + " ADD UNIQUE GLOBAL INDEX " + gsiTestIndexName
                        + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `" + gsiTestTableName + "` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            if (PropertiesUtil.enableAsyncDDL) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    HINT_CREATE_GSI + "ALTER TABLE " + gsiTestTableName + " ADD UNIQUE GLOBAL INDEX " + gsiTestIndexName
                        + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`) ");
            }
        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi3_error_single_primary_table() {
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
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4;\n");

            executeErrorAssert(tddlConnection,
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "ALTER TABLE " + gsiTestTableName
                    + " ADD GLOBAL INDEX " + gsiTestIndexName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`)", null,
                "Does not support create Global Secondary Index on single or broadcast table");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `"
                    + gsiTestTableName
                    + "` ( \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 broadcast;\n");

            executeErrorAssert(tddlConnection,
                "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_IGNORE_RESTRICTION=true)*/ "
                    + "ALTER TABLE " + gsiTestTableName
                    + " ADD UNIQUE GLOBAL INDEX " + gsiTestIndexName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH(`buyer_id`)",
                null,
                "Does not support create Global Secondary Index on single or broadcast table");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi4_error_storage_check() {
        if (supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE `gsi_test_table` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");
            executeErrorAssert(tddlConnection,
                "ALTER TABLE "
                    + gsiTestTableName
                    + " ADD GLOBAL INDEX " + gsiTestIndexName
                    + "(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n", null,
                "Cannot create global secondary index, MySQL 5.7 or higher version is needed");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi5_error_duplicate_index_name() {
        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestIndexName = "g_i_test_buyer";
        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestIndexName));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                HINT_CREATE_GSI
                    + "CREATE TABLE `gsi_test_table` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    PRIMARY KEY (`id`), \n"
                    + "    INDEX g_i_buyer_id2(`buyer_id`), \n"
                    + "    GLOBAL INDEX g_i_test_buyer(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`) \n"
                    + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE " + gsiTestTableName + " ADD GLOBAL INDEX " + gsiTestIndexName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)", null,
                "Global Secondary Index '" + gsiTestIndexName + "' already exists");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI
                    + "ALTER TABLE "
                    + gsiTestTableName
                    + " ADD GLOBAL INDEX g_i_buyer_id2 (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)",
                null, "Duplicate index name 'g_i_buyer_id2'");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE " + gsiTestTableName + " ADD GLOBAL INDEX " + gsiTestTableName
                    + " (`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH (`buyer_id`)", null,
                "Global Secondary Index '" + gsiTestTableName + "' already exists");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi6_error_index_contains_sharding_columns() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition = " (`id2`) dbpartition by hash(`id3`)";
        final String gsiPartition1 = " (`id2`) dbpartition by hash(`id2`) tbpartition by hash(id3) tbpartitions 3";

        try {
            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME + "` ADD GLOBAL INDEX " + gsiName
                    + gsiPartition, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME + "` ADD UNIQUE GLOBAL INDEX " + gsiName
                    + gsiPartition, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME + "` ADD GLOBAL INDEX " + gsiName
                    + gsiPartition1, null,
                "The index columns of global secondary index must contains all the sharding columns of index table");
        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void addGsi7_error_duplicated_column_name() {
        final String gsiName = "gsi_create_1";
        final String gsiPartition =
            " (`id2`, `id3`) covering(id3, vc1) dbpartition by hash(`id2`) tbpartition by hash(id3) tbpartitions 3";

        try {
            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME + "` ADD GLOBAL INDEX " + gsiName
                    + gsiPartition, null, "Duplicate column name 'id3' in index 'gsi_create_1'");

            executeErrorAssert(tddlConnection,
                HINT_CREATE_GSI + "ALTER TABLE `" + GSI_PRIMARY_TABLE_NAME + "` ADD UNIQUE GLOBAL INDEX " + gsiName
                    + gsiPartition, null, "Duplicate column name 'id3' in index 'gsi_create_1'");

        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }

    @Test
    public void testMysqlBug26780307() {
        String primaryTableName = "testMysqlBug26780307";
        String gsiName = "testMysqlBug26780307_g1";
        String createPrimaryTable =
            String.format("create table %s(a int, b json) dbpartition by hash(a);", primaryTableName);
        String insert = String.format("insert into %s values(%d,'%s')", primaryTableName, 1,
            "{\"text\": \"c3\\\\u0001^l\\\\u001F@@Z\"}");
        String createGsi = String.format("create global index %s on %s(a) covering(b) dbpartition by hash(a)", gsiName,
            primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createPrimaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
    }

    @Test
    public void testPlan() {

        String primaryTableName = "tbl_check_plan";
        String gsiName = "tbl_check_plan_gsi";
        String createPrimaryTable =
            String.format("create table %s (a bigint(11) , b int(11), c int) dbpartition by hash(a)", primaryTableName);
        String insert1 = String.format("insert into %s(a,b,c) values (%d,%d,%d);", primaryTableName, 2, 5, 3);
        String insert2 = String.format("insert into %s(a,b,c) values (%d,%d,%d);", primaryTableName, 1, 5, 3);
        String update = String.format("trace update %s set a = 4 where a = 1", primaryTableName);
        String createGsi = String.format(
            "/*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=DELETE_ONLY) */ alter table %s add global index %s(a,b,c) dbpartition by hash(a)",
            primaryTableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createPrimaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.toString(), trace.size(), is(4));
    }

    private String buildCreateTable(String gsiDef) {
        if (isMySQL80()) {
            return CREATE_TABLE_BASE_8 + ",\n\t" + gsiDef + CREATE_TABLE_TAIL_TB_8;
        }
        return CREATE_TABLE_BASE + ",\n\t" + gsiDef + CREATE_TABLE_TAIL_TB;
    }
}
