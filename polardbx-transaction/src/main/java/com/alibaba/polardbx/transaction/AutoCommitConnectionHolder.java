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

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.utils.TransactionAsyncUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class AutoCommitConnectionHolder extends BaseConnectionHolder {

    public AutoCommitConnectionHolder() {
    }

    /**
     *
     */
    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        IConnection conn = null;
        try {
            conn = ds.getConnection();
        } finally {
            if (conn != null) {
                this.connections.add(conn);
            }
        }
        return conn;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        IConnection conn = null;
        try {
            conn = ds.getConnection(masterSlave);
        } finally {
            if (conn != null) {
                this.connections.add(conn);
            }
        }
        return conn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        try {
            conn.close();
        } finally {
            this.connections.remove(conn);
        }
    }

    /**
     * Execute actions concurrently or sequentially, depending on number of tasks
     */
    void forEachConnection(AsyncTaskQueue asyncQueue, Consumer<IConnection> action) {
        List<Runnable> tasks = new ArrayList<>(connections.size());
        connections.forEach((heldConn) -> tasks.add(() -> action.accept(heldConn)));

        TransactionAsyncUtils.runTasksConcurrently(asyncQueue, tasks);
    }
}
