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
public class RenamePartTableFpTest extends BasePartitionedTableFailPointTestCase {

    protected String tblName = "create_test_tbl";

    protected String newTblName = "create_test_tbl_new";

    protected String renameTableSqlTemplate = "rename table %s to %s";

    protected void createPartitionTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tblName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
        String showCreateTableString = showCreateTable(failPointConnection, tblName);
        Assert.assertTrue(showCreateTableString.contains(tblName));
    }

    protected void renameTables() {
        String renameTableStr1 = String.format(renameTableSqlTemplate, tblName, newTblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, renameTableStr1);
        String showCreateTableString = showCreateTable(failPointConnection, newTblName);
        Assert.assertTrue(showCreateTableString.contains(newTblName));

        String renameTableStr2 = String.format(renameTableSqlTemplate, newTblName, tblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, renameTableStr2);
        showCreateTableString = showCreateTable(failPointConnection, tblName);
        Assert.assertTrue(showCreateTableString.contains(tblName));
    }

    protected void dropPartitionTable() {
        String dropTableStmt = String.format("drop table if exists %s", tblName);
        JdbcUtil.executeUpdateSuccess(failPointConnection, dropTableStmt);
    }

    @Override
    protected void doPrepare() {
        dropPartitionTable();
        createPartitionTable();
    }

    @Override
    protected void execDdlWithFailPoints() {
        renameTables();
    }

}
