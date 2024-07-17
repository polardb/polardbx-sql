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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlterTableAddCciErrorTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME1 = "alter_table_add_cci_err_prim_pt_1";
    private static final String INDEX_NAME1 = "alter_table_add_cci_err_cci_pt_1";
    private static final String PRIMARY_TABLE_NAME2 = "alter_table_add_cci_err_prim_pt_2";
    private static final String INDEX_NAME2 = "alter_table_add_cci_err_cci_pt_2";
    private static final String PRIMARY_TABLE_NAME3 = "alter_table_add_cci_err_prim_pt_3";
    private static final String INDEX_NAME3 = "alter_table_add_cci_err_cci_pt_3";

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
                + "    PRIMARY KEY (`id`)"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String createCciTmpl =
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) PARTITION BY KEY(`ID`)";

            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1);
            final String sqlCreateTable2 = String.format(
                creatTableTmpl,
                cciTestTableName2);
            final String sqlCreateTable3 = String.format(
                creatTableTmpl,
                cciTestTableName3);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable3);

            // Create cci success
            final String sqlCreateCci1 = String.format(createCciTmpl, cciTestTableName1, cciTestIndexName1);
            createCciSuccess(sqlCreateCci1);

            // Duplicated cci name
            createCciWithErr(
                sqlCreateCci1,
                String.format("Duplicate index name '%s'", cciTestIndexName1));

            // Duplicated cci name(conflict with cci on another table)
            final String sqlCreateCci2 = String.format(createCciTmpl, cciTestTableName2, cciTestIndexName1);
            createCciSuccess(sqlCreateCci2);

            // Duplicated cci name(conflict with name of another table)
            final String sqlCreateCci3 = String.format(createCciTmpl, cciTestTableName3, cciTestTableName1);
            createCciSuccess(sqlCreateCci3);

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
                + "    INDEX `%s`(`buyer_id`)"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable4 = String.format(
                creatTableTmpl1,
                cciTestTableName1,
                cciTestIndexName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable4);

            final String sqlCreateCci4 = String.format(createCciTmpl, cciTestTableName1, cciTestIndexName1);
            createCciWithErr(
                sqlCreateCci4,
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
                + "    PRIMARY KEY (`id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);

            final String sqlCreateIndex1 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`, buyer_id) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateIndex1,
                String.format("Duplicate column name 'buyer_id' in index '%s'", cciTestIndexName1));

            // Duplicate index and covering column
            final String sqlCreateIndex2 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) COVERING(`buyer_id`) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateIndex2,
                String.format("Duplicate column name 'buyer_id' in index '%s'", cciTestIndexName1));

            // Duplicate index and covering column
            final String sqlCreateIndex3 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) COVERING(`order_snapshot`, `order_snapshot`) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateIndex3,
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
            // Duplicate index column
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

        final String sqlCreateIndex1 = String.format(
            "ALTER TABLE %s ADD COLUMNAR INDEX %s(`buyer_id`) PARTITION BY KEY(`id`)",
            cciTestTableName1,
            cciTestIndexName1);

        createCciWithErr(sqlCreateIndex1, "Columnar index must be specified as a clustered index.");
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
                + "    PRIMARY KEY (`id`) \n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);

            // Create one cci
            final String sqlCreateIndex1 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName1);

            createCciSuccess(sqlCreateIndex1);

            // Create another cci
            final String sqlCreateIndex2 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName2);

            createCciWithErr(
                sqlCreateIndex2,
                String.format("Columnar index on table '%s' already exists", cciTestTableName1));

            createCciSuccess("/*+TDDL:CMD_EXTRA(MAX_CCI_COUNT=2)*/" + sqlCreateIndex2);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    @Test
    public void testCreate5_error_implicit_primary_key() {
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1;
        final String cciTestIndexName1 = INDEX_NAME1;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext \n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);

            // Create one cci
            final String sqlCreateIndex1 = String.format(
                "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX %s(`buyer_id`) PARTITION BY KEY(`id`)",
                cciTestTableName1,
                cciTestIndexName1);

            createCciWithErr(
                sqlCreateIndex1,
                "Do not support create Clustered Columnar Index on table without primary key");
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }
}
