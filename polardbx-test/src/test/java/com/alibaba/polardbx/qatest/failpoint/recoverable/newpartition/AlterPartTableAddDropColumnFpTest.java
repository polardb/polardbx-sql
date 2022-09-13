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
public class AlterPartTableAddDropColumnFpTest extends BasePartitionedTableFailPointTestCase {

    protected String tblName = "create_test_tbl";
    protected String tblWithGsiName = "create_test_tbl_with_gsi";

    protected String newColumn1 = "new_col1";
    protected String newColumn2 = "new_col2";

    protected String alterAddColumnSqlTemplate =
        "alter table %s add column new_col1 decimal DEFAULT NULL, add column new_col2 decimal DEFAULT NULL";
    protected String alterDropColumnSqlTemplate = "alter table %s drop column new_col1, drop column new_col2";

    protected void createPartitionTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tblName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tblName);
        Assert.assertTrue(showCreateTableString.contains(tblName));
    }

    protected void alterPartTableAddColumns() {
        String addColStr1 = String.format(alterAddColumnSqlTemplate, tblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, addColStr1);
        String showCreateTableString = showCreateTable(failPointConnection, tblName);
        Assert.assertTrue(showCreateTableString.toLowerCase().contains(newColumn1));
        Assert.assertTrue(showCreateTableString.toLowerCase().contains(newColumn2));

        String addColStr2 = String.format(alterAddColumnSqlTemplate, tblWithGsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, addColStr2);
        String showCreateTableString2 = showCreateTable(failPointConnection, tblWithGsiName);
        Assert.assertTrue(showCreateTableString2.toLowerCase().contains(newColumn1));
        Assert.assertTrue(showCreateTableString2.toLowerCase().contains(newColumn2));
    }

    protected void alterPartTableDropColumns() {

        String dropColStr1 = String.format(alterDropColumnSqlTemplate, tblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropColStr1);
        String showCreateTableString = showCreateTable(failPointConnection, tblName);
        Assert.assertFalse(showCreateTableString.toLowerCase().contains(newColumn1));
        Assert.assertFalse(showCreateTableString.toLowerCase().contains(newColumn2));

        String dropColStr2 = String.format(alterDropColumnSqlTemplate, tblWithGsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropColStr2);
        String showCreateTableString2 = showCreateTable(failPointConnection, tblWithGsiName);
        Assert.assertFalse(showCreateTableString2.toLowerCase().contains(newColumn1));
        Assert.assertFalse(showCreateTableString2.toLowerCase().contains(newColumn2));
    }

    protected void createPartitionTableWithGsi() {
        String createTableWithGsiStmt = PartitionedTableDdlTestCase.buildCreateTblWithGsiSql(tblWithGsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableWithGsiStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tblWithGsiName);
        Assert.assertTrue(showCreateTableString.contains(tblWithGsiName));
    }

    protected void dropPartitionTable() {
        String dropTableStmt = String.format("drop table if exists %s", tblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropTableStmt);
        dropTableStmt = String.format("drop table if exists %s", tblWithGsiName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropTableStmt);
    }

    @Override
    protected void doPrepare() {
        dropPartitionTable();
        createPartitionTable();
        createPartitionTableWithGsi();
    }

    @Override
    protected void execDdlWithFailPoints() {
        alterPartTableAddColumns();
        alterPartTableDropColumns();
        alterPartTableAddColumns();
        alterPartTableDropColumns();
    }

}
