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

package com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.failpoint.recoverable.newpartition.BasePartitionedTableFailPointTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class RenamePartTableFpTest extends BaseAutoPartitionNewPartition {

    protected String tblName = "create_test_tbl";

    protected String newTblName = "create_test_tbl_new";

    protected String singleTblName = "create_test_single_tbl";

    protected String singleNewTblName = "create_test_single_tbl_new";

    protected String broadcastTblName = "create_test_brd_tbl";

    protected String broadcastNewTblName = "create_test_brd_tbl_new";

    protected String renameTableSqlTemplate = "rename table %s to %s";

    protected String select = "select * from %s";

    protected void createPartitionTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tblName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
        String showCreateTableString = showCreateTable(tddlConnection, tblName);
        Assert.assertTrue(showCreateTableString.contains(tblName));
    }
    protected void createSingleTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(singleTblName,
                "single");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
        String showCreateTableString = showCreateTable(tddlConnection, singleTblName);
        Assert.assertTrue(showCreateTableString.contains(singleTblName));
    }

    protected void createBroadcastTable() {
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(broadcastTblName,
                "broadcast");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
        String showCreateTableString = showCreateTable(tddlConnection, broadcastTblName);
        Assert.assertTrue(showCreateTableString.contains(broadcastTblName));
    }

    protected void renameTables() {
        List<Pair<String, String>> tablePairs = new ArrayList<>();
        tablePairs.add(new Pair<>(tblName, newTblName));
        tablePairs.add(new Pair<>(singleTblName, singleNewTblName));
        tablePairs.add(new Pair<>(broadcastTblName, broadcastNewTblName));

        for (Pair<String, String> pair : tablePairs) {
            String renameTableStr1 = String.format(renameTableSqlTemplate, pair.getKey(), pair.getValue());
            JdbcUtil.executeUpdateSuccess(tddlConnection, renameTableStr1);
            String showCreateTableString = showCreateTable(tddlConnection, pair.getValue());
            Assert.assertTrue(showCreateTableString.contains(pair.getValue()));
            JdbcUtil.executeQuery(String.format(select, pair.getValue()), tddlConnection);

            String renameTableStr2 = String.format(renameTableSqlTemplate, pair.getValue(), pair.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, renameTableStr2);
            showCreateTableString = showCreateTable(tddlConnection, pair.getKey());
            Assert.assertTrue(showCreateTableString.contains(pair.getKey()));
            JdbcUtil.executeQuery(String.format(select, pair.getKey()), tddlConnection);
        }
    }

    protected void dropTables() {
        List<String> allTableNames = new ArrayList<>();
        allTableNames.add(tblName);
        allTableNames.add(newTblName);
        allTableNames.add(singleTblName);
        allTableNames.add(singleNewTblName);
        allTableNames.add(broadcastTblName);
        allTableNames.add(broadcastNewTblName);

        for(String tableName : allTableNames) {
            String dropTableStmt = String.format("drop table if exists %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableStmt);
        }
    }

    private  void doPrepare() {
        dropTables();
        createPartitionTable();
        createSingleTable();
        createBroadcastTable();
    }

    @Test
    public void testRenameTable() {
        doPrepare();
        renameTables();
        dropTables();
    }

}
