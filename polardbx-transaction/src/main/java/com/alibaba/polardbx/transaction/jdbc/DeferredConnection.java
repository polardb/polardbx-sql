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

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ConnectionStats;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ReadViewConn;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.mysql.cj.x.protobuf.PolarxExecPlan;

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
    /**
     * For Auto-Savepoint.
     */
    protected String autoSavepointMark = null;
    public final static String INVALID_AUTO_SAVEPOINT = "INVALID";

    //    protected List<String> deferSqls;
    protected List<BytesSql> deferBytesSqls;
    protected int discardResults;
    private final boolean xProtoOptForAutoSp;

    public DeferredConnection(IConnection conn, final boolean serverDiscards) {
        this.conn = conn;
        this.serverDiscards = serverDiscards;
        this.xProtoOptForAutoSp = false;
    }

    public DeferredConnection(IConnection conn, final boolean serverDiscards, boolean xProtoOptForAutoSp) {
        this.conn = conn;
        this.serverDiscards = serverDiscards;
        this.xProtoOptForAutoSp = xProtoOptForAutoSp;
    }

    public String getAutoSavepointMark() {
        return autoSavepointMark;
    }

    public void setAutoSavepointMark(String autoSavepointMark) {
        this.autoSavepointMark = autoSavepointMark;
    }

    @Override
    public void executeLater(String sql) throws SQLException {
        if (deferBytesSqls == null) {
            deferBytesSqls = new ArrayList<>(2);
        }
        deferBytesSqls.add(BytesSql.buildBytesSql(sql));
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

    protected final void beforeExecuting(byte[] hint) throws SQLException {
        if (deferBytesSqls == null || deferBytesSqls.isEmpty()) {
            return;
        }

        if (discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement hasn't executed.");
        }
        if (conn.isWrapperFor(XConnection.class)) {
            final XConnection xconn = conn.unwrap(XConnection.class);
            for (BytesSql deferSql : deferBytesSqls) {
                // Run with prefix.
                xconn.execUpdate(deferSql, hint, null, true);
                // No need to discard.
            }
            deferBytesSqls.clear(); // Note we must remove them, because we have actually enqueued them.
        } else {
            // should not happen
            GeneralUtil.nestedException("wrong prcessing beforeExecuting path :" + conn + "," + deferBytesSqls);
        }
    }

    protected final String beforeExecuting(String sql) throws SQLException {
        if (deferBytesSqls == null || deferBytesSqls.isEmpty()) {
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

            for (BytesSql deferSql : deferBytesSqls) {
                // Run with prefix.
                xconn.execUpdate(prefix + deferSql, null, true);
                // No need to discard.
            }
            deferBytesSqls.clear(); // Note we must remove them, because we have actually enqueued them.
            return sql;
        }

        StringBuilder builder = new StringBuilder(sql.length() + 64);
        builder.append(sql.substring(0, actualPos));
        if (serverDiscards) {
            builder.append("SET @@rds_result_skip_counter = ");
            builder.append(deferBytesSqls.size());
            builder.append(';');
        }
        for (BytesSql deferSql : deferBytesSqls) {
            builder.append(deferSql.toString());
            builder.append(';');
            discardResults++;
        }
        builder.append(sql);

        return builder.toString();
    }

    protected final void afterExecuted() {
        if (deferBytesSqls != null) {
            deferBytesSqls.clear();
        }
        discardResults = 0;
    }

    @Override
    public void flushUnsent() throws SQLException {
        // Use pipeline in XConnection.
        if (conn.isWrapperFor(XConnection.class)) {
            if (deferBytesSqls == null || deferBytesSqls.size() == 0) {
                return;
            }

            if (discardResults > 0) {
                GeneralUtil.nestedException("A deferred PreparedStatement hasn't executed.");
            }

            final XConnection xconn = conn.unwrap(XConnection.class);

            try {
                for (BytesSql deferSql : deferBytesSqls) {
                    // Run with prefix.
                    xconn.execUpdate(deferSql, null, null, true);
                    // No need to discard.
                }
            } finally {
                deferBytesSqls.clear(); // Note we must remove them, because we have actually enqueued them.
            }
            return;
        }

        if (deferBytesSqls == null || deferBytesSqls.isEmpty()) {
            return;
        }

        if (discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement hasn't executed.");
        }

        StringBuilder builder = new StringBuilder();
        if (serverDiscards) {
            // Return 1 result for avoid blocking
            builder.append("SET @@rds_result_skip_counter = ");
            builder.append(deferBytesSqls.size() - 1);
        }
        for (BytesSql deferSql : deferBytesSqls) {
            if (builder.length() > 0) {
                builder.append(';');
            }
            builder.append(deferSql.toString());
        }

        // Run deferred queries in separate statement.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(builder.toString());
        } finally {
            deferBytesSqls.clear();
        }
    }

    @Override
    public void setEncoding(String encoding) throws SQLException {
        conn.setEncoding(encoding);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        conn.abort(executor);
    }

    @Override
    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        autoSavepointMark = null;
        super.close();
        conn.close();
    }

    @Override
    public void commit() throws SQLException {
        conn.commit();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return conn.createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new DeferredStatement(this, conn.createStatement());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return new DeferredStatement(this,
            conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new DeferredStatement(this, conn.createStatement(resultSetType, resultSetConcurrency));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return conn.createStruct(typeName, attributes);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return conn.getClientInfo(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return conn.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return conn.isValid(timeout);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.isWrapperFor(iface);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        flushUnsent();

        return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        flushUnsent();

        return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {

        flushUnsent();

        return conn.prepareCall(sql);
    }

    @Override
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

    @Override
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

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, autoGeneratedKeys));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, columnIndexes));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql, columnNames));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        String deferredSql = beforeExecuting(sql);

        try {
            return new DeferredPreparedStatement(this, conn.prepareStatement(deferredSql));
        } catch (Throwable e) {
            discardResults = 0;
            throw e;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        conn.releaseSavepoint(savepoint);
    }

    @Override
    public void rollback() throws SQLException {
        conn.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        conn.rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        conn.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        conn.setClientInfo(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        conn.setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        conn.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return conn.setSavepoint(name);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        conn.setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        conn.setTypeMap(map);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.unwrap(iface);
    }

    @Override
    public void kill() throws SQLException {
        conn.kill();
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
    public boolean isBytesSqlSupported() throws SQLException {
        return conn.isBytesSqlSupported();
    }

    @Override
    public PreparedStatement prepareStatement(BytesSql sql, byte[] hint) throws SQLException {
        if (conn.isBytesSqlSupported()) {
            beforeExecuting(hint);
            try {
                return new DeferredPreparedStatement(this, conn.prepareStatement(sql, hint));
            } catch (Throwable e) {
                discardResults = 0;
                throw e;
            }
        } else {
            throw new NotSupportException("bytes sql not supported in DeferredConnection:" + conn.getClass().getName());
        }
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

    @Override
    public void discard(Throwable t) {
        if (conn != null) {
            conn.discard(t);
        }
    }

    public void setAutoSavepoint(String spName, String schemaName) throws SQLException {
        long lastLogTime = TransactionAttribute.LAST_LOG_AUTO_SP_TIME.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_AUTO_SP_TIME.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.AUTO_SP, "Found use of auto savepoint set");
        }
        if (isSupportXOptForAutoSp(schemaName)) {
            lastLogTime = TransactionAttribute.LAST_LOG_AUTO_SP_OPT_TIME.get();
            if (TransactionManager.shouldWriteEventLog(lastLogTime)
                && TransactionAttribute.LAST_LOG_AUTO_SP_OPT_TIME.compareAndSet(lastLogTime, System.nanoTime())) {
                EventLogger.log(EventType.AUTO_SP_OPT, "Found use of auto savepoint opt set");
            }
            this.flushUnsent();
            this.unwrap(XConnection.class).handleAutoSavepoint(spName, PolarxExecPlan.AutoSp.Operation.SET, true);
        } else {
            this.executeLater("SAVEPOINT " + TStringUtil.backQuote(spName));
        }
    }

    public void releaseAutoSavepoint(String spName, String schemaName, boolean ignoreResult) throws SQLException {
        long lastLogTime = TransactionAttribute.LAST_LOG_AUTO_SP_RELEASE.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_AUTO_SP_RELEASE.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.AUTO_SP, "Found use of auto savepoint release");
        }
        if (isSupportXOptForAutoSp(schemaName)) {
            this.flushUnsent();
            this.unwrap(XConnection.class)
                .handleAutoSavepoint(spName, PolarxExecPlan.AutoSp.Operation.RELEASE, ignoreResult);
        } else {
            if (ignoreResult) {
                this.executeLater("RELEASE SAVEPOINT " + TStringUtil.backQuote(spName));
            } else {
                try (Statement stmt = createStatement()) {
                    stmt.execute("RELEASE SAVEPOINT " + TStringUtil.backQuote(spName));
                }
            }
        }
    }

    public void rollbackAutoSavepoint(String spName, String schemaName) throws SQLException {
        long lastLogTime = TransactionAttribute.LAST_LOG_AUTO_SP_ROLLBACK.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_AUTO_SP_ROLLBACK.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.AUTO_SP, "Found use of auto savepoint rollback");
        }
        if (isSupportXOptForAutoSp(schemaName)) {
            this.flushUnsent();
            this.unwrap(XConnection.class).handleAutoSavepoint(spName, PolarxExecPlan.AutoSp.Operation.ROLLBACK, true);
        } else {
            this.executeLater("ROLLBACK TO SAVEPOINT " + TStringUtil.backQuote(spName));
        }
    }

    private boolean isSupportXOptForAutoSp(String schemaName) throws SQLException {
        return this.isWrapperFor(XConnection.class)
            && xProtoOptForAutoSp
            && ExecutorContext.getContext(schemaName).getStorageInfoManager().supportXOptForAutoSp()
            && this.unwrap(XConnection.class).isXRPC();
    }

    @Override
    public void forceRollback() throws SQLException {
        if (conn != null) {
            conn.forceRollback();
        }
    }

    @Override
    public IConnection enableFlashbackArea(boolean enable) throws SQLException {
        conn.enableFlashbackArea(enable);
        return this;
    }

    @Override
    public void disableFlashbackArea() throws SQLException {
        conn.disableFlashbackArea();
    }
}
