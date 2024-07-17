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

package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.common.IInnerConnectionManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InnerConnectionManager implements IInnerConnectionManager {
    private static final InnerConnectionManager INSTANCE = new InnerConnectionManager();

    private static final ConcurrentHashMap<Long, InnerConnection> activeConnections = new ConcurrentHashMap<>();

    private InnerConnectionManager() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new InnerConnection();
    }

    @Override
    public Connection getConnection(String schema) throws SQLException {
        return new InnerConnection(schema);
    }

    public static InnerConnectionManager getInstance() {
        return INSTANCE;
    }

    public static ConcurrentHashMap<Long, InnerConnection> getActiveConnections() {
        return activeConnections;
    }
}
