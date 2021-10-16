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

package com.alibaba.polardbx.atom.utils;

import com.mysql.jdbc.JDBC4Connection;
import com.alibaba.polardbx.config.ConfigDataMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

public class NetworkUtils {

    public static void setNetworkTimeout(Connection conn, Executor executor,
                                         final int milliseconds) throws SQLException {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        JDBC4Connection connInner = conn.unwrap(JDBC4Connection.class);
        connInner.setNetworkTimeout(executor, milliseconds);
    }

    public static int getNetworkTimeout(Connection conn) throws SQLException {
        if (ConfigDataMode.isFastMock()) {
            return 100;
        }
        JDBC4Connection connInner = conn.unwrap(JDBC4Connection.class);
        return connInner.getNetworkTimeout();
    }

}
