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

import com.alibaba.polardbx.common.lock.LockingFunctionHandle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public interface IConnection extends Connection {

    void kill() throws SQLException;

    long getLastInsertId();

    void setLastInsertId(long id);

    long getReturnedLastInsertId();

    void setReturnedLastInsertId(long id);

    List<Long> getGeneratedKeys();

    void setGeneratedKeys(List<Long> ids);

    ITransactionPolicy getTrxPolicy();

    void setTrxPolicy(ITransactionPolicy trxPolicy);

    BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds);

    void setBatchInsertPolicy(BatchInsertPolicy policy);

    void setEncoding(String encoding) throws SQLException;

    String getEncoding();

    void setSqlMode(String sqlMode);

    String getSqlMode();

    void setStressTestValid(boolean stressTestValid);

    boolean isStressTestValid();

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

    long getFoundRows();

    void setFoundRows(long foundRows);

    long getAffectedRows();

    void setAffectedRows(long affectedRows);

    String getUser();

    void setUser(String userName);

    void executeLater(String sql) throws SQLException;

    void flushUnsent() throws SQLException;

    default long getId() {
        return 0L;
    }

    default LockingFunctionHandle getLockHandle(Object object) {
        return null;
    }

    default ConnectionStats getConnectionStats() {
        return null;
    }

    default IConnection getRealConnection() {
        return this;
    }

    default int getReadViewId() {
        return -1;
    }

    default void setReadViewId(int readViewId) {
        throw new UnsupportedOperationException("Connection does not support read view id.");
    }
}
