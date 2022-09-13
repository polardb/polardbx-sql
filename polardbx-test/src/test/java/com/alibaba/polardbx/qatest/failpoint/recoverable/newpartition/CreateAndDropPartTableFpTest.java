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
public class CreateAndDropPartTableFpTest extends BasePartitionedTableFailPointTestCase {

    protected String tableName = "create_test_tbl";

    protected void createPartitionTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    protected void createPartitionTableWithGsi() {
        String createTableWithGsiStmt = PartitionedTableDdlTestCase.buildCreateTblWithGsiSql(tableName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableWithGsiStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    protected void dropPartitionTable() {
        String dropTableStmt = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropTableStmt);
    }

    @Override
    protected void doPrepare() {
        dropPartitionTable();
    }

    @Override
    protected void execDdlWithFailPoints() {
        createPartitionTable();
        dropPartitionTable();
        createPartitionTableWithGsi();
        dropPartitionTable();
    }

}
