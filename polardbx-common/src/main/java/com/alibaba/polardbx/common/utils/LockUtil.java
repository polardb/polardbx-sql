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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * @author yaozhili
 */
public class LockUtil {
    private static final Logger logger = LoggerFactory.getLogger(LockUtil.class);

    /**
     * Wrapped operations with specified lock_wait_timeout to avoid long MDL-wait.
     */
    public static void wrapWithLockWaitTimeout(Connection conn, int lockWaitTimeout, Runnable task)
        throws SQLException {
        int originLockWaitTimeout = 0;
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select @@lock_wait_timeout")) {
            while (rs.next()) {
                originLockWaitTimeout = rs.getInt(1);
            }
        }
        // Only decrease the lock wait timeout, not increase it.
        if (originLockWaitTimeout > lockWaitTimeout) {
            try (Statement stmt = conn.createStatement()) {
                try {
                    stmt.execute("set lock_wait_timeout = " + lockWaitTimeout);
                    task.run();
                } finally {
                    try {
                        stmt.execute("set lock_wait_timeout = " + originLockWaitTimeout);
                    } catch (Throwable t) {
                        if (conn instanceof IConnection) {
                            // Discard connection to prevent reuse.
                            ((IConnection) conn).discard(t);
                        }
                    }
                }
            }
        } else {
            task.run();
        }
    }

    public static void wrapWithInnodbLockWaitTimeout(Connection conn, int lockWaitTimeout, Runnable task)
        throws SQLException {
        int originLockWaitTimeout = 0;
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select @@innodb_lock_wait_timeout")) {
            while (rs.next()) {
                originLockWaitTimeout = rs.getInt(1);
            }
        }
        // Only decrease the lock wait timeout, not increase it.
        if (originLockWaitTimeout > lockWaitTimeout) {
            try (Statement stmt = conn.createStatement()) {
                try {
                    stmt.execute("set innodb_lock_wait_timeout = " + lockWaitTimeout);
                    task.run();
                } finally {
                    try {
                        stmt.execute("set innodb_lock_wait_timeout = " + originLockWaitTimeout);
                    } catch (Throwable t) {
                        if (conn instanceof IConnection) {
                            // Discard connection to prevent reuse.
                            ((IConnection) conn).discard(t);
                        }
                    }
                }
            }
        } else {
            task.run();
        }
    }

    /**
     * Wrapped operations with specified lock_wait_timeout to avoid long MDL-wait.
     */
    public static <T> T wrapWithSocketTimeout(Connection conn, int socketTimeout, Executor socketTimeoutExecutor,
                                              Callable<T> task) throws SQLException {
        int originNetworkTimeout = conn.getNetworkTimeout();
        // Only decrease the socket timeout, not increase it.
        try {
            if (0 == originNetworkTimeout || originNetworkTimeout > socketTimeout) {
                conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeout);
                try {
                    return task.call();
                } finally {
                    conn.setNetworkTimeout(socketTimeoutExecutor, originNetworkTimeout);
                }
            } else {
                return task.call();
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            logger.error("wrapWithSocketTimeout error: ", e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, e.getMessage());
        }
    }
}
