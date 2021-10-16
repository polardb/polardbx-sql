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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 参见InnerConnection
 *
 * @author ziyang.lb 2020-12-05
 */
public class InnerTransManager {
    private final Connection connection;

    public InnerTransManager(Connection connection) {
        this.connection = connection;
    }

    public void executeWithTransaction(Callback callback) throws SQLException {
        connection.setAutoCommit(false);

        try {
            callback.execute();
            connection.commit();
        } catch (Throwable t) {
            connection.rollback();
            throw t;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public interface Callback {
        void execute() throws SQLException;
    }
}
