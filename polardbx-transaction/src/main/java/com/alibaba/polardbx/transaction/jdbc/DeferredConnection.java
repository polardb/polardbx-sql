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

package com.alibaba.polardbx.transaction.jdbc;

import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ConnectionStats;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ReadViewConn;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DeferredConnection extends ReadViewConn {

    protected final boolean serverDiscards;
    protected final IConnection conn;

    protected List<String> deferSqls;
    protected int discardResults;

    public DeferredConnection(IConnection conn, final boolean serverDiscards) {
        this.conn = conn;
        this.serverDiscards = serverDiscards;
    }

    @Override
    public void executeLater(String sql) throws SQLException {
        if (deferSqls == null) {
            deferSqls = new ArrayList<>(2);
        }
        deferSqls.add(sql);
    }

    private static int findStart(String sql) {
        int length = sql.length();
        int pos = 0;

        for (; pos < length; pos++) {
            if (!Character.isWhitespace(sql.charAt(pos))) {
                break;
            }
        }
        if (sql.startsWith("/*", pos)) {
            int find = sql.indexOf("*/", pos + 2);
            if (find != -1) {
                pos = find + 2;
            }
        } else if (sql.startsWith("--", pos) || sql.startsWith("#", pos)) {
            int find = sql.indexOf('\n', pos);
            if (find == -1) {
                find = sql.indexOf('\r', pos);
            }
            if (find != -1) {
                pos = find + 2;
            }
        }

        for (; pos < length; pos++) {
            if (!Character.isWhitespace(sql.charAt(pos))) {
                break;
            }
        }
        return pos;
    }

    protected final String beforeExecuting(String sql) throws SQLException {
        if (deferSqls == null || deferSqls.isEmpty()) {
            return sql;
        }

        if (discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement hasn't executed.");
        }
        // Build a multi-query including deferred queries.
        int actualPos = findStart(sql);

        // Use pipeline in XConnection.
        if (conn.isWrapperFor(XConnection.class)) {
            final XConnection xconn = conn.unwrap(XConnection.class);
            final String prefix = sql.substring(0, actualPos);

            for (String deferSql : deferSqls) {
                // Run with prefix.
                xconn.execUpdate(prefix + deferSql, null, true);
                // No need to discard.
            }
            deferSqls.clear(); // Note we must remove them, because we have actually enqueued them.
            return sql;
        }

        StringBuilder builder = new StringBuilder(sql.length() + 64);
        builder.append(sql.substring(0, actualPos));
        if (serverDiscards) {
            builder.append("SET @@rds_result_skip_counter = ");
            builder.append(deferSqls.size());
            builder.append(';');
        }
        for (String deferSql : deferSqls) {
            builder.append(deferSql);
            builder.append(';');
            discardResults++;
        }
        builder.append(sql);

        return builder.toString();
    }

    protected final void afterExecuted() {
        if (deferSqls != null) {
            deferSqls.clear();
        }
        discardResults = 0;
    }

    @Override
    public void flushUnsent() throws SQLException {
        if (deferSqls == null || deferSqls.isEmpty()) {
            return;
        }

        if (discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement hasn't executed.");
        }

        // Use pipeline in XConnection.
        if (conn.isWrapperFor(XConnection.class)) {
            final XConnection xconn = conn.unwrap(XConnection.class);

            try {
                for (String deferSql : deferSqls) {
                    // Run with prefix.
                    xconn.execUpdate(deferSql, null, true);
                    // No need to discard.
                }
            } finally {
                deferSqls.clear(); // Note we must remove them, because we have actually enqueued them.
            }
            return;
        }

        StringBuilder builder = new StringBuilder();
        if (serverDiscards) {
            // Return 1 result for avoid blocking
            builder.append("SET @@rds_result_skip_counter = ");
            builder.append(deferSqls.size() - 1);
        }
        for (String deferSql : deferSqls) {
            if (builder.length() > 0) {
                builder.append(';');
            }
            builder.append(deferSql);
        }

        // Run deferred queries in separate statement.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(builder.toString());
        } finally {
            deferSqls.clear();
        }
    }

    @Override
    public void setEncoding(String encoding) throws SQLException {
        conn.setEncoding(encoding);
    }

    public void abort(Executor executor) throws SQLException {
        conn.abort(executor);
    }

    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    public void close() throws SQLException {
        conn.close();
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return conn.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        return new DeferredStatement(this, conn.createStatement());
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return new DeferredStatement(this,
            conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new DeferredStatement(this, conn.createStatement(resultSetType, resultSetConcurrency));
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return conn.createStruct(typeName, attributes);
    }

    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return conn.getClientInfo(name);
    }

    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return conn.getNetworkTimeout();
    }

    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    public boolean isValid(int timeout) throws SQLException {
        return conn.isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.isWrapperFor(iface);
    }

    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        flushUnsent();

        return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        flushUnsent();

        return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {

        flushUnsent();

        return conn.prepareCall(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this,
                conn.prepareStatement(deferredSql, resultSetType, resultSetConcurrency, resultSetHoldability));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this,
                conn.prepareStatement(deferredSql, resultSetType, resultSetConcurrency));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, autoGeneratedKeys));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, columnIndexes));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, columnNames));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        conn.releaseSavepoint(savepoint);
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        conn.rollback(savepoint);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        conn.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        conn.setClientInfo(name, value);
    }

    public void setHoldability(int holdability) throws SQLException {
        conn.setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        conn.setNetworkTimeout(executor, milliseconds);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return conn.setSavepoint(name);
    }

    public void setSchema(String schema) throws SQLException {
        conn.setSchema(schema);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        conn.setTypeMap(map);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.unwrap(iface);
    }

    @Override
    public void kill() throws SQLException {
        conn.kill();
    }

    @Override
    public long getLastInsertId() {
        return conn.getLastInsertId();
    }

    @Override
    public void setLastInsertId(long id) {
        conn.setLastInsertId(id);
    }

    @Override
    public long getReturnedLastInsertId() {
        return conn.getReturnedLastInsertId();
    }

    @Override
    public void setReturnedLastInsertId(long id) {
        conn.setReturnedLastInsertId(id);
    }

    @Override
    public List<Long> getGeneratedKeys() {
        return conn.getGeneratedKeys();
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
        conn.setGeneratedKeys(ids);
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        return conn.getTrxPolicy();
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) {
        conn.setTrxPolicy(trxPolicy);
    }

    @Override
    public BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds) {
        return conn.getBatchInsertPolicy(extraCmds);
    }

    @Override
    public void setBatchInsertPolicy(BatchInsertPolicy policy) {
        conn.setBatchInsertPolicy(policy);
    }

    @Override
    public String getEncoding() {
        return conn.getEncoding();
    }

    @Override
    public void setSqlMode(String sqlMode) {
        conn.setSqlMode(sqlMode);
    }

    @Override
    public String getSqlMode() {
        return conn.getSqlMode();
    }

    @Override
    public void setStressTestValid(boolean stressTestValid) {
        conn.setStressTestValid(stressTestValid);
    }

    @Override
    public boolean isStressTestValid() {
        return conn.isStressTestValid();
    }

    @Override
    public Map<String, Object> getServerVariables() {
        return conn.getServerVariables();
    }

    @Override
    public void setServerVariables(Map<String, Object> serverVariables) throws SQLException {
        conn.setServerVariables(serverVariables);
    }

    @Override
    public long getFoundRows() {
        return conn.getFoundRows();
    }

    @Override
    public void setFoundRows(long foundRows) {
        conn.setFoundRows(foundRows);
    }

    @Override
    public long getAffectedRows() {
        return conn.getAffectedRows();
    }

    @Override
    public void setAffectedRows(long affectedRows) {
        conn.setAffectedRows(affectedRows);
    }

    @Override
    public String getUser() {
        return conn.getUser();
    }

    @Override
    public void setUser(String userName) {
        conn.setUser(userName);
    }

    @Override
    public long getId() {
        return conn.getId();
    }

    @Override
    public ConnectionStats getConnectionStats() {
        return conn.getConnectionStats();
    }

    @Override
    public IConnection getRealConnection() {
        if (conn instanceof DeferredConnection) {
            return conn.getRealConnection();
        } else {
            return conn;
        }
    }
}
