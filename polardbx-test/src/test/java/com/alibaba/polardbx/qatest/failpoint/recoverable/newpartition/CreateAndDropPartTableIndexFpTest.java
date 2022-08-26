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

package com.alibaba.polardbx.qatest.failpoint.recoverable.newpartition;

import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

/**
 * @author chenghui.lch
 */
public class CreateAndDropPartTableIndexFpTest extends BasePartitionedTableFailPointTestCase {

    protected String tableName = "create_test_tbl";
    protected String gsiName = "new_gsi";

    protected void createPartitionTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    protected void dropPartitionTable() {
        String dropTableStmt = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropTableStmt);
    }

    protected void addGsi() {
        String addGsiStmt = PartitionedTableDdlTestCase.buildCreateGsiSql(tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, addGsiStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(gsiName));
    }

    protected void dropGsi() {
        String dropGsiStmt = PartitionedTableDdlTestCase.buildDropGsiSql(tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropGsiStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertFalse(showCreateTableString.contains(gsiName));
    }

    @Override
    protected void doPrepare() {
        dropPartitionTable();
        createPartitionTable();
    }

    @Override
    protected void execDdlWithFailPoints() {
        addGsi();
        dropGsi();
        addGsi();
        dropGsi();
    }

}
