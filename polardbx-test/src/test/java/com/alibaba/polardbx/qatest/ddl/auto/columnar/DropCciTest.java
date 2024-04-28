/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Formatter;
import java.util.List;
import java.util.Random;

public class DropCciTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_PREFIX = "drop_cci_prim";
    private static final String INDEX_PREFIX = "drop_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_2";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testDrop1_cleanup_table_partition() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            createCciSuccess(sqlCreateTable1);

            // Get real cci name
            final String realCciName = getRealCciName(cciTestTableName1, cciTestIndexName1);
            Truth.assertWithMessage("Query real cci name failed!")
                .that(realCciName)
                .isNotNull();

            // Drop cci
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("drop index %s on %s", cciTestIndexName1, cciTestTableName1));

            // Check table partitions cleared
            final List<TablePartitionRecord> tablePartitions = queryCciTablePartitionRecords(realCciName);
            Truth.assertWithMessage("%s dangling table partition record found", tablePartitions.size())
                .that(tablePartitions)
                .isEmpty();

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testDrop2_cleanup_table_group_cache() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            createCciSuccess(sqlCreateTable1);

            // Get real cci name
            final String realCciName = getRealCciName(cciTestTableName1, cciTestIndexName1);
            Truth.assertWithMessage("Query real cci name failed!")
                .that(realCciName)
                .isNotNull();

            final List<TableGroupRecord> tgList = queryCciTgFromMetaDb(realCciName);
            Truth.assertWithMessage("Unexpected table group record count found for cci `%s`!", realCciName)
                .that(tgList)
                .hasSize(1);

            // Drop cci
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("drop index %s on %s", cciTestIndexName1, cciTestTableName1));

            checkTableGroup(realCciName, tgList.get(0).tg_name);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testDrop3_alter_table_drop_cci_on_partition_table() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            createCciSuccess(sqlCreateTable1);

            // Get real cci name
            final String realCciName = getRealCciName(cciTestTableName1, cciTestIndexName1);
            Truth.assertWithMessage("Query real cci name failed!")
                .that(realCciName)
                .isNotNull();

            final List<TableGroupRecord> tgList = queryCciTgFromMetaDb(realCciName);
            Truth.assertWithMessage("Unexpected table group record count found for cci `%s`!", realCciName)
                .that(tgList)
                .hasSize(1);

            // Drop cci
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("alter table %s drop index %s", cciTestTableName1, cciTestIndexName1));

            // Check partition group cleared
            checkTableGroup(realCciName, tgList.get(0).tg_name);

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testDrop4_alter_table_drop_cci_on_single_table() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME2;
        final String cciTestIndexName1 = INDEX_NAME2;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 single;\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            createCciSuccess(sqlCreateTable1);

            // Get real cci name
            final String realCciName = getRealCciName(cciTestTableName1, cciTestIndexName1);
            Truth.assertWithMessage("Query real cci name failed!")
                .that(realCciName)
                .isNotNull();

            final List<TableGroupRecord> tgList = queryCciTgFromMetaDb(realCciName);
            Truth.assertWithMessage("Unexpected table group record count found for cci `%s`!", realCciName)
                .that(tgList)
                .hasSize(1);

            // Drop cci
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("alter table %s drop index %s", cciTestTableName1, cciTestIndexName1));

            // Check partition group cleared
            checkTableGroup(realCciName, tgList.get(0).tg_name);

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testDrop5_alter_table_drop_cci_on_broadcast_table() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME3;
        final String cciTestIndexName1 = INDEX_NAME3;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 broadcast;\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            createCciSuccess(sqlCreateTable1);

            // Get real cci name
            final String realCciName = getRealCciName(cciTestTableName1, cciTestIndexName1);
            Truth.assertWithMessage("Query real cci name failed!")
                .that(realCciName)
                .isNotNull();

            final List<TableGroupRecord> tgList = queryCciTgFromMetaDb(realCciName);
            Truth.assertWithMessage("Unexpected table group record count found for cci `%s`!", realCciName)
                .that(tgList)
                .hasSize(1);

            // Drop cci
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("alter table %s drop index %s", cciTestTableName1, cciTestIndexName1));

            // Check partition group cleared
            checkTableGroup(realCciName, tgList.get(0).tg_name);

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testDrop6_alter_table_drop_cci_check_cdc_mark() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        final String cciTestTableName1 = PRIMARY_TABLE_PREFIX + suffix;
        final String cciTestIndexName1 = INDEX_PREFIX + suffix;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            dropTableIfExists(cciTestTableName1);
            createCciSuccess(sqlCreateTable1);

            // Drop cci
            final String sqlDdl1 = String.format("drop index %s on %s", cciTestIndexName1, cciTestTableName1);
            executeDdlAndCheckCdcRecord(sqlDdl1, sqlDdl1, cciTestTableName1, true);
            checkLatestColumnarSchemaEvolutionRecordByDdlSql(sqlDdl1,
                getDdlSchema(),
                cciTestTableName1,
                cciTestIndexName1,
                DdlType.DROP_INDEX,
                ColumnarTableStatus.DROP);

            // Cleanup
            dropTableIfExists(cciTestTableName1);

            // Create table with cci
            final String suffix2 = "__" + formatter.format("%04x", random.nextInt(0x10000));
            final String cciTestTableName2 = PRIMARY_TABLE_PREFIX + suffix2;
            final String cciTestIndexName2 = INDEX_PREFIX + suffix2;
            final String sqlCreateTable2 = String.format(
                creatTableTmpl,
                cciTestTableName2,
                cciTestIndexName2);
            createCciSuccess(sqlCreateTable2);

            // Alter table drop cci
            final String sqlDdl2 =
                String.format("ALTER TABLE %s\n" + "\tDROP INDEX %s", cciTestTableName2, cciTestIndexName2);
            final String expectedSqlDdl2 = String.format("drop index %s on %s", cciTestIndexName2, cciTestTableName2);
            executeDdlAndCheckCdcRecord(sqlDdl2, expectedSqlDdl2, cciTestTableName2, true);
            checkLatestColumnarSchemaEvolutionRecordByDdlSql(expectedSqlDdl2,
                getDdlSchema(),
                cciTestTableName2,
                cciTestIndexName2,
                DdlType.DROP_INDEX,
                ColumnarTableStatus.DROP);

            // Cleanup
            dropTableIfExists(cciTestTableName1);
        } catch (Exception e) {
            throw new RuntimeException("sql statement execution failed!", e);
        }
    }

    private void checkTableGroup(String realCciName, String tgName) throws Exception {
        // Check partition group cleared
        final List<TableGroupRecord> clearedTgList = queryCciTgFromMetaDb(realCciName);
        Truth.assertWithMessage("Dangling table group record found for cci `%s`!", realCciName)
            .that(clearedTgList)
            .isEmpty();

        final List<Long> clearedShowTgResultList = queryCciTgFromInformationSchema(tgName);
        Truth.assertWithMessage(
                "Dangling show tablegroup result found for cci `%s` and tg `%s`!",
                realCciName,
                tgName)
            .that(clearedShowTgResultList)
            .isEmpty();
    }
}
