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

package com.alibaba.polardbx.qatest.failpoint.rollbackable.legacy;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.failpoint.base.BaseFailPointTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;

public class CreateTableRollbackAbleTest extends BaseFailPointTestCase {

    @Before
    public void doBefore() {
        clearFailPoints();
    }

    @After
    public void doAfter() {
        clearFailPoints();
    }

    @Test
    public void test_FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION() {
        //异常注入：指定某个Task执行之后DDL回滚
        enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION, "CreateTableAddTablesMetaTask");
        executeFailedThenAutoRollback();
        //清理注入点之后，再次执行DDL，预期成功
        clearFailPoints();
        executeSuccess();
    }

    protected void executeFailedThenAutoRollback() {
        createSingleTable(false);
        createBroadcastTable(false);
        createPartitionTable(false);
    }

    protected void executeSuccess() {
        createSingleTable(true);
        createBroadcastTable(true);
        createPartitionTable(true);
    }

    protected void createSingleTable(boolean assertExecutionSuccess) {
        String tableName = randomTableName("create_single_table", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName, "");

        if (assertExecutionSuccess) {
            JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertTrue(showCreateTableString.contains(tableName));
        } else {
            JdbcUtil.executeUpdateFailed(failPointConnection, createTableStmt, "");
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertFalse(showCreateTableString.contains(tableName));
        }
    }

    protected void createBroadcastTable(boolean assertExecutionSuccess) {
        String tableName = randomTableName("create_broadcast_table", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName, "broadcast");

        if (assertExecutionSuccess) {
            JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertTrue(showCreateTableString.contains(tableName));
        } else {
            JdbcUtil.executeUpdateFailed(failPointConnection, createTableStmt, "");
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertFalse(showCreateTableString.contains(tableName));
        }
    }

    protected void createPartitionTable(boolean assertExecutionSuccess) {
        String tableName = randomTableName("create_db_partition_table", 4);
        String createTableStmt =
            ExecuteTableSelect.getFullTypeMultiPkTableDef(tableName, DEFAULT_PARTITIONING_DEFINITION);

        if (assertExecutionSuccess) {
            JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertTrue(showCreateTableString.contains(tableName));
        } else {
            JdbcUtil.executeUpdateFailed(failPointConnection, createTableStmt, "");
            String showCreateTableString = showCreateTable(failPointConnection, tableName);
            Assert.assertFalse(showCreateTableString.contains(tableName));
        }
    }

}