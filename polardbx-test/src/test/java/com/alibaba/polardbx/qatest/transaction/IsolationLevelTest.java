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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * @author Eric Fu
 */

public class IsolationLevelTest extends CrudBasedLockTestCase {

    private boolean useBedTrans;

    public IsolationLevelTest() {
        // Test on table `update_delete_base_multi_db_multi_tb`
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
    }

    @Before
    public void prepareData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        useBedTrans = !JdbcUtil.supportXA(tddlConnection);
    }

    /**
     * Test isolation level REPEATABLE READ
     */
    @Test
    public void testIsolationLevelRepeatableRead() throws Exception {

        try (Connection conn1 = getPolardbxConnection();
            Connection conn2 = getPolardbxConnection()) {
            conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            conn1.setAutoCommit(false);

            if (useBedTrans) {
                //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, conn1);
            }

            int result1;
            int result2;

            try (Statement stmt = conn1.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select @@tx_isolation")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(IsolationLevel.REPEATABLE_READ.nameWithHyphen(), rs.getString(1));
                }
                try (ResultSet rs = stmt.executeQuery("select sum(integer_test) from " + baseOneTableName)) {
                    Assert.assertTrue(rs.next());
                    result1 = rs.getInt(1);
                }
            }

            try (Statement stmt = conn2.createStatement()) {
                int count = stmt.executeUpdate("update " + baseOneTableName + " set integer_test = 0");
                Assert.assertTrue(count > 0);
            }

            try (Statement stmt = conn1.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select sum(integer_test) from " + baseOneTableName)) {
                    Assert.assertTrue(rs.next());
                    result2 = rs.getInt(1);
                }
            }

            Assert.assertEquals(result1, result2);
            conn1.setAutoCommit(true);
        }
    }

    /**
     * Test isolation level READ COMMITTED
     */
    @Test
    public void testIsolationLevelReadCommitted() throws Exception {

        try (Connection conn1 = getPolardbxConnection();
            Connection conn2 = getPolardbxConnection()) {
            conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn1.setAutoCommit(false);

            if (useBedTrans) {
                //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, conn1);
            }

            try (Statement stmt = conn1.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select @@transaction_isolation")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(IsolationLevel.READ_COMMITTED.nameWithHyphen(), rs.getString(1));
                }
                try (ResultSet rs = stmt.executeQuery("select sum(integer_test) from " + baseOneTableName)) {
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.getInt(1) != 0);
                }
            }

            try (Statement stmt = conn2.createStatement()) {
                int count = stmt.executeUpdate("update " + baseOneTableName + " set integer_test = 0");
                Assert.assertTrue(count > 0);
            }

            try (Statement stmt = conn1.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select sum(integer_test) from " + baseOneTableName)) {
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.getInt(1) == 0);
                }
            }

            conn1.setAutoCommit(true);
        }
    }

    private void assertIsolation(Statement stmt, String expected) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select @@transaction_isolation")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getString(1));
        }
    }

    /**
     * Test setting/getting isolation level
     */
    @Test
    public void testSetIsolationLevel() throws Exception {
        try (Connection conn = getPolardbxConnection(); Statement stmt = conn.createStatement()) {
            // set session transaction isolation
            {
                int count = stmt.executeUpdate("set transaction isolation level repeatable read");
                Assert.assertEquals(0, count);
            }
            assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());

            // set @@tx_isolation
            {
                int count = stmt.executeUpdate("set @@tx_isolation='READ-COMMITTED'");
                Assert.assertEquals(0, count);
            }
            assertIsolation(stmt, IsolationLevel.READ_COMMITTED.nameWithHyphen());

            // set transaction isolation
            {
                int count = stmt.executeUpdate("set transaction isolation level repeatable read");
                Assert.assertEquals(count, 0);
            }
            assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());
            assertIsolation(stmt, IsolationLevel.READ_COMMITTED.nameWithHyphen());

            {
                stmt.execute("set transaction isolation level repeatable read");
                stmt.execute("BEGIN");
                assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());
                stmt.execute("SELECT 1");
                assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());
                stmt.execute("COMMIT");
                assertIsolation(stmt, IsolationLevel.READ_COMMITTED.nameWithHyphen());
            }

            {
                stmt.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
                stmt.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());
                stmt.execute("SELECT 1");
                assertIsolation(stmt, IsolationLevel.REPEATABLE_READ.nameWithHyphen());
                stmt.execute("COMMIT");
                assertIsolation(stmt, IsolationLevel.READ_COMMITTED.nameWithHyphen());
            }

            stmt.execute("SET @@tx_isolation=DEFAULT");
        }
    }

}
