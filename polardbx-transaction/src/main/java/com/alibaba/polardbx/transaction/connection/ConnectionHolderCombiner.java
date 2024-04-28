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

package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class ConnectionHolderCombiner extends BaseConnectionHolder {

    private final IConnectionHolder[] chs;

    public ConnectionHolderCombiner(IConnectionHolder... chs) {
        this.chs = chs;
    }

    @Override
    public Set<IConnection> getAllConnection() {
        Set<IConnection> conns = new HashSet<>();
        for (IConnectionHolder ch : chs) {
            conns.addAll(ch.getAllConnection());
        }
        return conns;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void kill() {
        for (IConnectionHolder ch : chs) {
            ch.kill();
        }
    }

}
