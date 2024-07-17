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

package com.alibaba.polardbx.gms.metadb;

import com.alibaba.druid.pool.DruidConnectionHolder;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.rpc.pool.XConnection;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author chenghui.lch
 */
public class MetaDbConnectionProxy implements IConnection {

    private static final Logger log = LoggerFactory.getLogger(MetaDbConnectionProxy.class);

    protected Connection conn = null;
    private String encoding;
    private String sqlMode;
    private Map<String, Object> serverVariables = null;
    private boolean stressTestValid = false;
    private Statement currentStatement = null;

    public MetaDbConnectionProxy() throws SQLException {
        conn = createNewConnection(MetaDbDataSource.getInstance().getDataSource());
    }

    private Connection createNewConnection(DataSource metaDbDs) throws SQLException {
        return metaDbDs.getConnection();
    }

    @Override
    public void setEncoding(String encoding) throws SQLException {
        this.encoding = encoding;
    }

    public Connection getConn() {
        return conn;
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
        currentStatement = conn.createStatement();
        return currentStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        currentStatement = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        return currentStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        currentStatement = conn.createStatement(resultSetType, resultSetConcurrency);
        return currentStatement;
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
        return 1000;
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
        currentStatement = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return (CallableStatement) currentStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        currentStatement = conn.prepareCall(sql, resultSetType, resultSetConcurrency);
        return (CallableStatement) currentStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        currentStatement = conn.prepareCall(sql);
        return (CallableStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        currentStatement = conn.prepareStatement(sql,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability);
        return (PreparedStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        currentStatement = conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
        return (PreparedStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        currentStatement = conn.prepareStatement(sql, autoGeneratedKeys);
        return (PreparedStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        currentStatement = conn.prepareStatement(sql, columnIndexes);
        return (PreparedStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        currentStatement = conn.prepareStatement(sql, columnNames);
        return (PreparedStatement) currentStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        currentStatement = conn.prepareStatement(sql);
        return (PreparedStatement) currentStatement;
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
        Statement stmt = currentStatement;
        if (stmt != null && !stmt.isClosed()) {
            stmt.cancel();
        }
    }

    @Override
    public String getEncoding() {
        return this.encoding;
    }

    @Override
    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    @Override
    public String getSqlMode() {
        return this.sqlMode;
    }

    @Override
    public void setStressTestValid(boolean stressTestValid) {
        this.stressTestValid = stressTestValid;
    }

    @Override
    public boolean isStressTestValid() {
        return stressTestValid;
    }

    @Override
    public boolean isBytesSqlSupported() throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            return conn.unwrap(XConnection.class).supportRawString();
        }
        return false;
    }

    @Override
    public PreparedStatement prepareStatement(BytesSql sql, byte[] hint) throws SQLException {
        if (isBytesSqlSupported()) {
            return conn.unwrap(XConnection.class).prepareStatement(sql, hint);
        } else {
            // illegal metadb conn proxy
            throw new NotSupportException("bytes sql not supported in MetaDbConnectionProxy");
        }
    }

    @Override
    public Map<String, Object> getServerVariables() {
        return this.serverVariables;
    }

    @Override
    public void setServerVariables(Map<String, Object> serverVariables) throws SQLException {
    }

    @Override
    public void executeLater(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Override
    public void flushUnsent() throws SQLException {
        // Nothing to flush
    }

    @Override
    public void discard(Throwable error) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        try {
            if (conn.isWrapperFor(XConnection.class)) {
                conn.unwrap(XConnection.class).setLastException(new Exception("discard"), true);
            } else {
                // Discard pooled connection.
                DruidPooledConnection druidConn = conn.unwrap(DruidPooledConnection.class);
                // If druidConn is null, NPE is expected.
                DruidConnectionHolder holder = druidConn.getConnectionHolder();
                // Ignore if connection is already discard.
                if (holder != null && !holder.isDiscard() && !druidConn.isDisable()) {
                    holder.getDataSource().discardConnection(holder);
                    druidConn.disable(error);
                }
            }
        } catch (Throwable ex) {
            log.error("Failed to discard connection on group METADB", ex);
        }
    }

    @Override
    public void forceRollback() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ROLLBACK");
        } catch (Throwable e) {
            log.error("Cleanup readonly transaction branch failed on METADB", e);
            discard(e);
            throw e;
        }
    }

}
