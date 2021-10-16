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

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class CrossGroupConnectionHolder extends BaseConnectionHolder {

    private final static Logger logger = LoggerFactory.getLogger(StrictConnectionHolder.class);

    private ConcurrentMap<String, BlockingQueue<IConnection>> connsMap = new ConcurrentHashMap<>();

    public CrossGroupConnectionHolder() {
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        BlockingQueue<IConnection> conns = connsMap.computeIfAbsent(groupName, k -> new LinkedBlockingQueue<>());

        IConnection conn = conns.poll();
        if (conn == null) {
            synchronized (ds) {
                conn = conns.poll();
                if (conn == null) {
                    // double-check
                    conn = ds.getConnection();
                    this.connections.add(conn);
                }
            }
        }
        return conn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) {
        BlockingQueue<IConnection> trxConns = this.connsMap.get(groupName);
        if (trxConns == null) {
            return;
        }
        trxConns.offer(conn);

        if (logger.isDebugEnabled()) {
            logger.debug("tryClose:" + conn);
        }
    }

    @Override
    public void closeAllConnections() {
        super.closeAllConnections();
        this.connsMap.clear();
    }

}
