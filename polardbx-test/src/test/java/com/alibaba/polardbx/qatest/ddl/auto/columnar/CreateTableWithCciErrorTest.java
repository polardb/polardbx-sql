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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CreateTableWithCciErrorTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME1 = "create_table_with_cci_err_prim_pt_1";
    private static final String INDEX_NAME1 = "create_table_with_cci_err_cci_pt_1";
    private static final String PRIMARY_TABLE_NAME2 = "create_table_with_cci_err_prim_pt_2";
    private static final String INDEX_NAME2 = "create_table_with_cci_err_cci_pt_2";
    private static final String PRIMARY_TABLE_NAME3 = "create_table_with_cci_err_prim_pt_3";
    private static final String INDEX_NAME3 = "create_table_with_cci_err_cci_pt_3";

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
    public void testCreate1_error_duplicate_index_name() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestTableName2 = PRIMARY_TABLE_NAME2;
        final String cciTestTableName3 = PRIMARY_TABLE_NAME3;
        final String cciTestIndexName1 = INDEX_NAME1;
        final String cciTestIndexName2 = INDEX_NAME2;
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
            createCciSuccess(sqlCreateTable1);

            // Duplicated cci name(conflict with cci name)
            final String sqlCreateTable2 = String.format(
                creatTableTmpl,
                cciTestTableName2,
                cciTestIndexName1);
            createCciSuccess(sqlCreateTable2);

            // Duplicated cci name(conflict with table name)
            final String sqlCreateTable3 = String.format(
                creatTableTmpl,
                cciTestTableName3,
                cciTestTableName1);
            createCciSuccess(sqlCreateTable3);

            dropTableIfExists(cciTestTableName1);
            dropTableIfExists(cciTestTableName2);
            dropTableIfExists(cciTestTableName3);

            // Duplicated cci name(conflict with local index name on same table)
            final String creatTableTmpl1 = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    INDEX `%s`(`buyer_id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable4 = String.format(
                creatTableTmpl1,
                cciTestTableName1,
                cciTestIndexName1,
                cciTestIndexName1);
            createCciWithErr(
                sqlCreateTable4,
                String.format("Duplicate index name '%s'", cciTestIndexName1));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate2_error_duplicated_column_name() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            // Duplicate index column
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`, buyer_id) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateTable1,
                String.format("Duplicate column name 'buyer_id' in index '%s'", cciTestIndexName1));

            // Duplicate index and covering column
            final String creatTableTmpl1 = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) COVERING(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable2 = String.format(
                creatTableTmpl1,
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateTable2,
                String.format("Duplicate column name 'buyer_id' in index '%s'", cciTestIndexName1));

            // Duplicate index and covering column
            final String creatTableTmpl2 = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) COVERING(`order_snapshot`, `order_snapshot`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable3 = String.format(
                creatTableTmpl2,
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateTable3,
                String.format("Duplicate column name 'order_snapshot' in index '%s'", cciTestIndexName1));

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate3_error_not_clustered() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(sqlCreateTable1, "Columnar index must be specified as a clustered index.");

        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate4_error_multi_cci() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        final String cciTestIndexName2 = INDEX_NAME2;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`order_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1,
                cciTestIndexName2);

            createCciWithErr(
                sqlCreateTable1,
                String.format("Do not support create more than one Clustered Columnar Index on table '%s' ",
                    cciTestTableName1));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate5_error_implicit_primary_key() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        final String cciTestIndexName2 = INDEX_NAME2;
        try {
            String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`order_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1,
                cciTestIndexName2);

            createCciWithErr(
                sqlCreateTable1,
                "Do not support create Clustered Columnar Index on table without primary key");

            creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`order_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1,
                cciTestIndexName2);
            createCciSuccess(sqlCreateTable1);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }
}
