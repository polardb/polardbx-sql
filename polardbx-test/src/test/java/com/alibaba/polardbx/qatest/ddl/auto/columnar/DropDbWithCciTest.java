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
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.util.Formatter;
import java.util.Random;

public class DropDbWithCciTest extends PartitionTestBase {
    private static final String dataBaseNameBase = "test_db_drop_db_with_cci";
    private static final String PRIMARY_TABLE_NAME1 = "drop_table_prim";
    private static final String INDEX_NAME1 = "drop_table_cci";

    @Test
    public void testDrop_table_with_cci_check_cdc_mark() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        final String dataBaseName = dataBaseNameBase + suffix;
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CREATE DATABASE IF NOT EXISTS " + dataBaseName + " MODE = AUTO");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + dataBaseName);

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
                PRIMARY_TABLE_NAME1,
                INDEX_NAME1);

            // Create table with cci
            dropTableIfExists(PRIMARY_TABLE_NAME1);
            createCciSuccess(sqlCreateTable1);

            // Drop database
            final String sqlDdl1 = String.format("drop database if exists %s ", dataBaseName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlDdl1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + getDdlSchema());
            checkLatestColumnarMappingRecordByDropDbSql(sqlDdl1,
                dataBaseName,
                PRIMARY_TABLE_NAME1,
                INDEX_NAME1,
                DdlType.DROP_INDEX,
                ColumnarTableStatus.DROP);

        } catch (Exception e) {
            throw new RuntimeException("sql statement execution failed!", e);
        }
    }
}
