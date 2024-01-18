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

package com.alibaba.polardbx.common.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executor;

public interface IConnection extends Connection {

    void setEncoding(String encoding) throws SQLException;

    String getEncoding();

    void setSqlMode(String sqlMode);

    String getSqlMode();

    void setStressTestValid(boolean stressTestValid);

    boolean isStressTestValid();

    boolean isBytesSqlSupported() throws SQLException;

    PreparedStatement prepareStatement(BytesSql sql, byte[] hint) throws SQLException;

    @Override
    void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException;

    @Override
    int getNetworkTimeout() throws SQLException;

    Map<String, Object> getServerVariables();

    void setServerVariables(Map<String, Object> serverVariables) throws SQLException;

    default void setGlobalServerVariables(Map<String, Object> GlobalServerVariables) throws SQLException {
    }

    default void setConnectionVariables(Map<String, Object> connectionVariables) throws SQLException {
    }

    /**
     * Run a SQL later within the next execution.
     *
     * @param sql - The deferred SQL.
     */
    void executeLater(String sql) throws SQLException;

    void flushUnsent() throws SQLException;

    default long getId() {
        return 0L;
    }

    default ConnectionStats getConnectionStats() {
        return null;
    }

    default IConnection getRealConnection() {
        return this;
    }

    default String getTrxXid() {
        throw new UnsupportedOperationException("Connection does not support trx xid.");
    }

    default void setTrxXid(String xid) {
        throw new UnsupportedOperationException("Connection does not support trx xid.");
    }

    default void setInShareReadView(boolean inShareReadView) {
        throw new UnsupportedOperationException("Connection does not support share read view.");
    }

    default boolean isInShareReadView() {
        throw new UnsupportedOperationException("Connection does not support share read view.");
    }

    default void setShareReadViewSeq(int seq) {
        throw new UnsupportedOperationException("Connection does not support share read view.");
    }

    default int getShareReadViewSeq() {
        throw new UnsupportedOperationException("Connection does not support share read view.");
    }

    void discard(Throwable t);

    void kill() throws SQLException;
}
