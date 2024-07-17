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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.NotThreadSafe.DeadlockTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NotThreadSafe
public class HandleTrxErrorTest extends CrudBasedLockTestCase {
    @Test
    public void testHandleStatementErrorFail()
        throws SQLException, ExecutionException, InterruptedException, TimeoutException {
        final Connection conn = this.getPolardbxConnection();
        final String tableName = "thsef";
        // 1. Create a table with 2 entries: (0, 0), (1, 1)
        JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + tableName);
        JdbcUtil.executeUpdateSuccess(conn, "create table " + tableName
            + "(id int primary key, a int) dbpartition by hash(id)");
        JdbcUtil.executeUpdateSuccess(conn, "insert into " + tableName
            + " values (0, 0), (1, 1)");
        try {
            // 2. In a trx, lock the whole table.
            conn.setAutoCommit(false);
            JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " for update");
            // 3. In another trx, update the whole table, and it should be blocked.
            Future<Boolean> future = updateTableInOtherConn(tableName, true);
            // 4. Kill the physical query of the blocked UPDATE statement.
            final String sql = "select * from information_schema.physical_processlist "
                + "where info like '%" + tableName + "%' and state = 'updating'";
            int cnt = 0;
            while (cnt < 10) {
                final ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
                if (rs.next()) {
                    final String group = rs.getString("GROUP");
                    final String atom = rs.getString("ATOM");
                    final String id = rs.getString("ID");
                    // kill this physical query
                    JdbcUtil.executeQuerySuccess(conn, String.format("kill '%s-%s-%s'", group, atom, id));
                    // 5. The UPDATE statement should be interrupted.
                    Assert.assertTrue(future.get(5, TimeUnit.SECONDS));
                    return;
                }
                cnt++;
                Thread.sleep(1000);
            }
            Assert.fail("Try to fetch blocking physical connections " + cnt + " times but still failed.");
        } finally {
            try {
                conn.rollback();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            try {
                conn.setAutoCommit(true);
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            JdbcUtil.executeUpdate(conn, "drop table if exists " + tableName, true, true);
            try {
                conn.close();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
        }
    }

    @Test
    public void testHandleStatementErrorFail2()
        throws SQLException, ExecutionException, InterruptedException, TimeoutException {
        final Connection conn = this.getPolardbxConnection();
        final String tableName = "thsef2";
        // 1. Create a table with 2 entries: (0, 0), (1, 1)
        JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + tableName);
        JdbcUtil.executeUpdateSuccess(conn, "create table " + tableName
            + "(id int primary key, a int) dbpartition by hash(id)");
        JdbcUtil.executeUpdateSuccess(conn, "insert into " + tableName
            + " values (0, 0), (1, 1)");
        try {
            // 2. In a trx, lock the whole table.
            conn.setAutoCommit(false);
            JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " for update");
            // 3. In another trx, update the whole table, and it should be blocked.
            Future<Boolean> future = updateTableInOtherConn(tableName, false);
            // 4. Kill the physical query of the blocked UPDATE statement.
            final String sql = "select * from information_schema.physical_processlist "
                + "where info like '%" + tableName + "%' and state = 'updating'";
            int cnt = 0;
            while (cnt < 10) {
                final ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
                if (rs.next()) {
                    final String group = rs.getString("GROUP");
                    final String atom = rs.getString("ATOM");
                    final String id = rs.getString("ID");
                    // kill this physical query
                    JdbcUtil.executeQuerySuccess(conn, String.format("kill '%s-%s-%s'", group, atom, id));
                    // 5. The UPDATE statement should be interrupted.
                    Assert.assertTrue(future.get(5, TimeUnit.SECONDS));
                    return;
                }
                cnt++;
                Thread.sleep(1000);
            }
            Assert.fail("Try to fetch blocking physical connections " + cnt + " times but still failed.");
        } finally {
            try {
                conn.rollback();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            try {
                conn.setAutoCommit(true);
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            JdbcUtil.executeUpdate(conn, "drop table if exists " + tableName, true, true);
            try {
                conn.close();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
        }
    }

    private Future<Boolean> updateTableInOtherConn(String tableName, boolean enableAutoSp) {
        final ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(DeadlockTest.class.getSimpleName(), false));
        return threadPool.submit(() -> {
            final Connection conn2 = this.getPolardbxConnection();
            JdbcUtil.executeUpdateSuccess(conn2, "set enable_auto_savepoint = " + enableAutoSp);
            try {
                conn2.setAutoCommit(false);
                JdbcUtil.executeUpdate(conn2, "update " + tableName + " set a = 100");
                return false;
            } catch (Throwable t) {
                t.printStackTrace();
                try {
                    // Some physical connection of this trx is killed, and this trx should only be rolled back.
                    JdbcUtil.executeQueryFaied(conn2, "select * from " + tableName,
                        enableAutoSp ? "Rolling back statement fails, you can only rollback the whole transaction."
                            : "Cannot continue or commit transaction");
                } catch (Throwable t2) {
                    return false;
                }
                return true;
            } finally {
                try {
                    conn2.rollback();
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
                try {
                    conn2.setAutoCommit(true);
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
                try {
                    conn2.close();
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
            }
        });
    }
}
