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

package com.alibaba.polardbx.group.jdbc;

import com.alibaba.druid.pool.DruidConnectionHolder;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.polardbx.atom.TAtomConnectionProxy;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.utils.EncodingUtils;
import com.alibaba.polardbx.atom.utils.NetworkUtils;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ConnectionStats;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.biv.MockConnection;
import com.alibaba.polardbx.rpc.pool.XConnection;
import org.apache.commons.lang.StringUtils;

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
 * 只存在一个Atom的TGroupConnection，操作基本直接代理给Atom获取的连接，仅用于DRDS模式，不具备全链路压测功能
 *
 * @author 梦实 2017年11月7日 下午1:59:01
 * @since 5.0.0
 */
public class TGroupDirectConnection implements IConnection {

    private static final Logger log = LoggerFactory.getLogger(TGroupDirectConnection.class);

    protected Connection conn;
    protected TGroupDataSource groupDataSource;
    protected String userName = null;
    protected String password = null;
    private String encoding;
    private String sqlMode;
    private Map<String, Object> serverVariables = null;
    private Map<String, Object> globalServerVariables = null;
    private boolean stressTestValid;
    private Statement currentStatement;
    private ConnectionStats connectionStats = new ConnectionStats();
    private String dbKey;

    public static class SocketTimeoutExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    public static final Executor socketTimeoutExecutor = new SocketTimeoutExecutor();

    public TGroupDirectConnection(TGroupDataSource groupDataSource, MasterSlave master) throws SQLException {
        this(groupDataSource, master, null, null);
    }

    public TGroupDirectConnection(TGroupDataSource groupDataSource, Connection connection)
        throws SQLException {
        this.groupDataSource = groupDataSource;
        this.userName = null;
        this.password = null;
        setConn(connection);
    }

    public TGroupDirectConnection(TGroupDataSource groupDataSource, MasterSlave master, String userName,
                                  String password)
        throws SQLException {
        this.groupDataSource = groupDataSource;
        this.userName = userName;
        this.password = password;
        setConn(createNewConnection(master));
    }

    private Connection createNewConnection(MasterSlave master) throws SQLException {
        long start = System.nanoTime();
        long startTs = System.currentTimeMillis();
        // 这个方法只发生在第一次建立读/写连接的时候，以后都是复用了
        Connection conn;

        TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(master);
        if (userName != null) {
            conn = atomDataSource.getConnection(userName, password);
        } else {
            conn = atomDataSource.getConnection();
        }
        this.dbKey = atomDataSource.getDbKey();

        long cost = System.nanoTime() - start;
        doConnStats(conn, startTs, cost);
        return conn;
    }

    /**
     * Get MySQL Connection ID (Thread ID) of underlying connection
     */
    @Override
    public long getId() {
        try {
            if (conn.isWrapperFor(XConnection.class)) {
                return conn.unwrap(XConnection.class).getConnectionId();
            }
            return conn.unwrap(TAtomConnectionProxy.class).getMysqlConnectionId();
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e); // impossible
        }
    }

    @Override
    public void setEncoding(String encoding) throws SQLException {
        this.encoding = encoding;
        if (!ConfigDataMode.isFastMock() && StringUtils.isNotEmpty(encoding)) {
            String mysqlEncoding = EncodingUtils.mysqlEncoding(encoding);
            String javaEncoding = EncodingUtils.javaEncoding(encoding);

            if (conn instanceof XConnection) {
                final XConnection xConnection = conn.unwrap(XConnection.class);
                boolean compatibleEncoding = encoding.equalsIgnoreCase("utf8")
                    && xConnection.getSession().getRequestEncodingMySQL().equalsIgnoreCase("utf8mb4");
                if (!mysqlEncoding.equalsIgnoreCase(xConnection.getSession().getRequestEncodingMySQL())
                    && !compatibleEncoding) {
                    // Set lazy encoding.
                    xConnection.getSession().setDefalutEncodingMySQL(mysqlEncoding);
                } else {
                    xConnection.getSession().setDefalutEncodingMySQL(null);
                }
            } else {
                throw new NotSupportException("xproto required");
            }
        }

    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
        if (conn instanceof MockConnection) {
            ((MockConnection) conn).setLogicalSchema(this.getGroupDataSource().getAppName());
        }
    }

    public TGroupDataSource getGroupDataSource() {
        return groupDataSource;
    }

    public void setGroupDataSource(TGroupDataSource groupDataSource) {
        this.groupDataSource = groupDataSource;
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
        return conn.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency);
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
        if (conn instanceof XConnection) {
            return conn.getNetworkTimeout();
        }
        return NetworkUtils.getNetworkTimeout(conn);
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
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return conn.prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return new TGroupDirectPreparedStatement(this, conn.prepareStatement(sql,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability), sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return new TGroupDirectPreparedStatement(this,
            conn.prepareStatement(sql, resultSetType, resultSetConcurrency),
            sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new TGroupDirectPreparedStatement(this, conn.prepareStatement(sql, autoGeneratedKeys), sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new TGroupDirectPreparedStatement(this, conn.prepareStatement(sql, columnIndexes), sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new TGroupDirectPreparedStatement(this, conn.prepareStatement(sql, columnNames), sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {

        return new TGroupDirectPreparedStatement(this, conn.prepareStatement(sql), sql);
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
        if (conn instanceof XConnection) {
            conn.setNetworkTimeout(executor, milliseconds);
            return;
        }
        NetworkUtils.setNetworkTimeout(conn, executor, milliseconds);
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
        if (this.getConn() instanceof XConnection) {
            ((XConnection) this.getConn()).cancel();
        } else {
            throw new NotSupportException();
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
        if (conn instanceof XConnection) {
            return ((XConnection) conn).prepareStatement(sql, hint);
        } else {
            throw new NotSupportException(
                "bytes sql not supported in TGroupDirectConnection:" + conn.getClass().getName());
        }
    }

    @Override
    public Map<String, Object> getServerVariables() {
        return this.serverVariables;
    }

    @Override
    public void setServerVariables(Map<String, Object> serverVariables) throws SQLException {
        this.serverVariables = serverVariables;

        if (conn instanceof XConnection) {
            if (serverVariables != null) {
                conn.unwrap(XConnection.class).setSessionVariables(serverVariables);
            }
        } else {
            throw new NotSupportException("xproto required");
        }

        String groupName = this.getGroupDataSource().getDbGroupKey();
        if (groupName.equalsIgnoreCase(MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME) ||
            SystemDbHelper.INFO_SCHEMA_DB_GROUP_NAME.equalsIgnoreCase(groupName)) {
            // prevent setting global on GMS
            return;
        }

        if (ConfigDataMode.isPolarDbX()) {
            Map<String, Object> globalServerVariables =
                MetaDbVariableConfigManager.getInstance().getDnVariableConfigMap();
            setGlobalServerVariables(globalServerVariables);
        }
    }

    @Override
    public void setGlobalServerVariables(Map<String, Object> globalServerVariables) throws SQLException {
        this.globalServerVariables = globalServerVariables;
        if (globalServerVariables != null && !globalServerVariables.isEmpty()) {
            conn.unwrap(XConnection.class).setGlobalVariables(globalServerVariables);
        }
    }

    protected void setCurrentStatement(Statement stmt) {
        this.currentStatement = stmt;
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
    public ConnectionStats getConnectionStats() {
        return connectionStats;
    }

    public void setConnectionStats(ConnectionStats connectionStats) {
        this.connectionStats = connectionStats;
    }

    protected void doConnStats(Connection conn, long startGetTsMills, long getGroupConnNano) {

        this.connectionStats = new ConnectionStats();
        if (conn instanceof XConnection) {
            XConnection connection = (XConnection) conn;
            connectionStats.setGetConnectionTotalNano(getGroupConnNano);
            connectionStats.setCreateConnectionNano(connection.getConnectNano());
            connectionStats.setWaitConnectionNano(connection.getWaitNano());
            connectionStats.setInitConnectionNano(
                getGroupConnNano - connection.getConnectNano() - connection.getWaitNano());
        } else {
            throw new NotSupportException("xproto required");
        }
    }

    public String getDbKey() {
        return dbKey;
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
            log.error("Failed to discard connection on group " + groupDataSource.getDbGroupKey(), ex);
        }
    }

    @Override
    public void forceRollback() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ROLLBACK");
        } catch (Throwable e) {
            log.error("Cleanup readonly transaction branch failed on " + groupDataSource.getDbGroupKey(), e);
            discard(e);
            throw e;
        }
    }
}
