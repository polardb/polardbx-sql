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
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Formatter;
import java.util.Random;

public class DropTableWithCciTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "drop_table_prim";
    private static final String INDEX_PREFIX = "drop_table_cci";
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
    public void testDrop_table_with_cci_check_cdc_mark() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        final String cciTestTableName1 = PRIMARY_TABLE_NAME1 + suffix;
        final String cciTestIndexName1 = INDEX_NAME1 + suffix;
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

            // Drop table
            final String sqlDdl1 = String.format("drop table if exists %s ", cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlDdl1);
            checkLatestColumnarSchemaEvolutionRecordByDdlSql(sqlDdl1,
                getDdlSchema(),
                cciTestTableName1,
                cciTestIndexName1,
                DdlType.DROP_INDEX,
                ColumnarTableStatus.DROP);

        } catch (Exception e) {
            throw new RuntimeException("sql statement execution failed!", e);
        }
    }

    @Test
    public void testDrop_table_with_multi_cci_check_cdc_mark() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        final String cciTestTableName1 = PRIMARY_TABLE_PREFIX + suffix;
        final String cciTestIndexName1 = INDEX_NAME1 + suffix;
        final String cciTestIndexName2 = INDEX_NAME2 + suffix;
        try {
            final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    CLUSTERED COLUMNAR INDEX `%s`(`order_id`) PARTITION BY KEY(`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                cciTestTableName1,
                cciTestIndexName1);

            // Create table with cci
            dropTableIfExists(cciTestTableName1);
            createCciSuccess(sqlCreateTable1);

            // Create another cci
            final String sqlCreateIndex2 = String.format(
                "CREATE CLUSTERED COLUMNAR INDEX %s ON %s(`buyer_id`) PARTITION BY KEY(`id`)",
                cciTestIndexName2,
                cciTestTableName1);

            createCciSuccess("/*+TDDL:CMD_EXTRA(MAX_CCI_COUNT=2)*/" + sqlCreateIndex2);

            // Drop table
            final String sqlDdl1 = String.format("drop table if exists %s ", cciTestTableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlDdl1);
            checkColumnarSchemaEvolutionRecordByDdlSql(sqlDdl1,
                getDdlSchema(),
                cciTestTableName1,
                ImmutableList.of(cciTestIndexName2, cciTestIndexName1),
                DdlType.DROP_INDEX,
                ColumnarTableStatus.DROP);

        } catch (Exception e) {
            throw new RuntimeException("sql statement execution failed!", e);
        }
    }
}
