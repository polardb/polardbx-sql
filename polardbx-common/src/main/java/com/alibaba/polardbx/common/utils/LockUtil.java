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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author yaozhili
 */
public class LockUtil {
    /**
     * Wrapped operations with specified lock_wait_timeout to avoid long MDL-wait.
     */
    public static void wrapWithLockWaitTimeout(Connection conn, int logWaitTimeout, Consumer<Statement> consumer)
        throws SQLException {
        int originLockWaitTimeout = 0;
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select @@lock_wait_timeout")) {
            while (rs.next()) {
                originLockWaitTimeout = rs.getInt(1);
            }
        }
        if (originLockWaitTimeout > 0) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set lock_wait_timeout = " + logWaitTimeout);
                try {
                    consumer.accept(stmt);
                } finally {
                    stmt.execute("set lock_wait_timeout = " + originLockWaitTimeout);
                }
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                "Get wrong lock_wait_timeout value: " + originLockWaitTimeout);
        }
    }
}
