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

package com.alibaba.polardbx.qatest.failpoint.recoverable.legacy;

import com.alibaba.polardbx.qatest.failpoint.base.BaseTableFailPointTestCase;
import org.junit.Assert;

public class AlterTableRecoverableTest extends BaseTableFailPointTestCase {

    protected void testSingleTable() {
        String tableName = createAndCheckTable("alter_single_table", EXTENDED_SINGLE);
        testAlterOperations(tableName);
    }

    protected void testBroadcastTable() {
        String tableName = createAndCheckTable("alter_broadcast_table", EXTENDED_BROADCAST);
        testAlterOperations(tableName);
    }

    protected void testShardingTable() {
        String tableName = createAndCheckTable("alter_sharding_table", EXTENDED_SHARDING);
        testAlterOperations(tableName);
    }

    protected void testAlterOperations(String tableName) {
        // Add Column
        String columnName = "colAdded";
        String ddl = String.format("alter table %s add column %s int", tableName, columnName);
        alterAndCheckTableWithColumn(ddl, tableName, columnName, true);

        // Add Index
        String indexName = "idxAdded";
        ddl = String.format("alter table %s add index %s (%s)", tableName, indexName, columnName);
        alterAndCheckTableWithIndex(ddl, tableName, indexName, true);

        // Rename Index
        String newIndexName = "idxNew";
        ddl = String.format("alter table %s rename index %s to %s", tableName, indexName, newIndexName);
        alterAndCheckTableWithIndex(ddl, tableName, indexName, false);
        checkTableWithIndex(tableName, newIndexName, true);

        // Drop Index
        ddl = String.format("alter table %s drop index %s", tableName, newIndexName);
        alterAndCheckTableWithIndex(ddl, tableName, newIndexName, false);

        // Alter Column Set Default
        ddl = String.format("alter table %s alter column %s set default 100", tableName, columnName);
        alterAndCheckTableWithColumn(ddl, tableName, columnName, true);

        // Alter Column Drop Default
        ddl = String.format("alter table %s alter column %s drop default", tableName, columnName);
        alterAndCheckTableWithColumn(ddl, tableName, columnName, true);

        // Drop Primary Key
        String primaryKeyName = "PRIMARY";
        ddl = String.format("alter table %s drop primary key", tableName);
        alterAndCheckTableWithIndex(ddl, tableName, primaryKeyName, false);

        // Add Primary Key
        ddl = String.format("alter table %s add primary key(id)", tableName);
        alterAndCheckTableWithIndex(ddl, tableName, primaryKeyName, true);

        // Drop Column
        ddl = String.format("alter table %s drop column %s", tableName, columnName);
        alterAndCheckTableWithColumn(ddl, tableName, columnName, false);
    }

    protected void alterAndCheckTableWithColumn(String alterTableStmt, String tableName, String columnName,
                                                boolean checkIfExists) {
        executeAndCheckTable(alterTableStmt, tableName);
        boolean exists = hasColumn(tableName, columnName);
        Assert.assertTrue(checkIfExists ? exists : !exists);
    }

    protected void alterAndCheckTableWithIndex(String alterTableStmt, String tableName, String indexName,
                                               boolean checkIfExists) {
        executeAndCheckTable(alterTableStmt, tableName);
        checkTableWithIndex(tableName, indexName, checkIfExists);
    }

    protected void checkTableWithIndex(String tableName, String indexName, boolean checkIfExists) {
        boolean exists = hasIndex(tableName, indexName);
        Assert.assertTrue(checkIfExists ? exists : !exists);
    }

}
