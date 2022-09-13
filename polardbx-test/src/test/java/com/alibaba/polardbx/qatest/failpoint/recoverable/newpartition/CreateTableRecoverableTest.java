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

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CreateTableRecoverableTest extends BasePartitionedTableFailPointTestCase {

    @Before
    public void doBefore() {
        clearFailPoints();
    }

    @After
    public void doAfter() {
        clearFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_FAIL_ONCE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_FAIL_ONCE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "30,3000");
        execDdlWithFailPoints();
    }

    protected void execDdlWithFailPoints() {
        createSingleTable();
        createBroadcastTable();
        createPartitionTable();
    }

    protected void createSingleTable() {
        String tableName = randomTableName("create_test_tbl", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName, "");

        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);

        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    protected void createBroadcastTable() {
        String tableName = randomTableName("create_test_tbl", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName, "broadcast");

        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);

        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    protected void createPartitionTable() {
        String tableName = randomTableName("create_test_tbl", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName,
                BasePartitionedTableFailPointTestCase.DEFAULT_PARTITIONING_DEFINITION);

        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);

        String showCreateTableString = showCreateTable(failPointConnection, tableName);
        Assert.assertTrue(showCreateTableString.contains(tableName));
    }

    public void foobar() {
        List<String[]> gsiDef = ExecuteTableSelect.supportedGsiDef();
        System.out.println(gsiDef);
    }

}
