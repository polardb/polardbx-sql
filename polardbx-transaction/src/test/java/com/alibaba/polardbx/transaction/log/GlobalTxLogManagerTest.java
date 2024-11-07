package com.alibaba.polardbx.transaction.log;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.transaction.TransactionState;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class GlobalTxLogManagerTest {
    @Test
    public void testLegacyTrxLogAppend() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.TRX_LOG_METHOD, "0");
        PreparedStatement stmt = mock(PreparedStatement.class);
        MockConnection connection = new MockConnection(stmt);
        GlobalTxLogManager.appendWithSocketTimeout(1, TransactionType.XA, TransactionState.SUCCEED, null, connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testLegacyTrxLogAppendTso() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.TRX_LOG_METHOD, "0");
        PreparedStatement stmt = mock(PreparedStatement.class);
        MockConnection connection = new MockConnection(stmt);
        GlobalTxLogManager.appendWithSocketTimeout(1, TransactionType.XA, TransactionState.SUCCEED, null, 1L,
            connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testNewTrxLogAppend() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.TRX_LOG_METHOD, "1");
        PreparedStatement stmt = mock(PreparedStatement.class);
        MockConnection connection = new MockConnection(stmt);
        GlobalTxLogManager.appendV2WithSocketTimeout(1, 1L, connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testNewTrxLogAppendWithLockWaitTimeout() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.TRX_LOG_METHOD, "1");
        PreparedStatement stmt = mock(PreparedStatement.class);
        MockConnection connection = new MockConnection(stmt);
        GlobalTxLogManager.appendV2WithSocketTimeout(1, 1L, connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testAppendWithLockWaitTimeout() throws SQLException {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(any())).thenReturn(rs);
        MockConnection connection = new MockConnection(preparedStatement, stmt);
        GlobalTxLogManager.appendWithTimeout(1, TransactionType.XA, TransactionState.SUCCEED, null, connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testAppendV2WithLockWaitTimeout() throws Exception {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(any())).thenReturn(rs);
        MockConnection connection = new MockConnection(preparedStatement, stmt);
        GlobalTxLogManager.appendV2WithTimeout(1, 1, connection);
        assertTrue(connection.hasSetNetworkTimeout);
        assertEquals(0, connection.networkTimeout);
    }

    @Test
    public void testCreateTables() throws SQLException {
        try (MockedStatic<GlobalTxLogManager> mockedStatic = mockStatic(GlobalTxLogManager.class)) {
            mockedStatic.when(() -> GlobalTxLogManager.createGlobalTxLogTable(any(), anyLong()))
                .then(invocation -> null);
            mockedStatic.when(() -> GlobalTxLogManager.createTables(any(), anyLong(), any())).thenCallRealMethod();

            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            Statement stmt = mock(Statement.class);
            ResultSet rs = mock(ResultSet.class);
            when(stmt.executeQuery(any())).thenReturn(rs);
            MockConnection connection = new MockConnection(preparedStatement, stmt);
            TGroupDataSource dataSource = mock(TGroupDataSource.class);
            TGroupDirectConnection tGroupDirectConnection = new TGroupDirectConnection(dataSource, connection);
            when(dataSource.getConnection()).thenReturn(tGroupDirectConnection);
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            GlobalTxLogManager.createTables(dataSource, 1000L, new HashSet<>());
            assertEquals(0, connection.networkTimeout);
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static class MockConnection implements IConnection {
        public boolean hasSetNetworkTimeout = false;
        public int networkTimeout = 0;
        public PreparedStatement prepareStmt;
        public Statement stmt;

        MockConnection(PreparedStatement prepareStmt) {
            this.prepareStmt = prepareStmt;
        }

        MockConnection(PreparedStatement prepareStmt, Statement stmt) {
            this.prepareStmt = prepareStmt;
            this.stmt = stmt;
        }

        @Override
        public void setEncoding(String encoding) throws SQLException {

        }

        @Override
        public String getEncoding() {
            return null;
        }

        @Override
        public void setSqlMode(String sqlMode) {

        }

        @Override
        public String getSqlMode() {
            return null;
        }

        @Override
        public void setStressTestValid(boolean stressTestValid) {

        }

        @Override
        public boolean isStressTestValid() {
            return false;
        }

        @Override
        public boolean isBytesSqlSupported() throws SQLException {
            return false;
        }

        @Override
        public PreparedStatement prepareStatement(BytesSql sql, byte[] hint) throws SQLException {
            return prepareStmt;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return stmt;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return prepareStmt;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return null;
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return null;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {

        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;
        }

        @Override
        public void commit() throws SQLException {

        }

        @Override
        public void rollback() throws SQLException {

        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {

        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return false;
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {

        }

        @Override
        public String getCatalog() throws SQLException {
            return null;
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {

        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return 0;
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return stmt;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return prepareStmt;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return null;
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return null;
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

        }

        @Override
        public void setHoldability(int holdability) throws SQLException {

        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return null;
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return null;
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {

        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {

        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
            return stmt;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                  int resultSetHoldability) throws SQLException {
            return prepareStmt;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return prepareStmt;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return prepareStmt;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return prepareStmt;
        }

        @Override
        public Clob createClob() throws SQLException {
            return null;
        }

        @Override
        public Blob createBlob() throws SQLException {
            return null;
        }

        @Override
        public NClob createNClob() throws SQLException {
            return null;
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return false;
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {

        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {

        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return null;
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return null;
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return null;
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return null;
        }

        @Override
        public void setSchema(String schema) throws SQLException {

        }

        @Override
        public String getSchema() throws SQLException {
            return null;
        }

        @Override
        public void abort(Executor executor) throws SQLException {

        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            hasSetNetworkTimeout = true;
            networkTimeout = milliseconds;
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return networkTimeout;
        }

        @Override
        public Map<String, Object> getServerVariables() {
            return null;
        }

        @Override
        public void setServerVariables(Map<String, Object> serverVariables) throws SQLException {

        }

        @Override
        public void executeLater(String sql) throws SQLException {

        }

        @Override
        public void flushUnsent() throws SQLException {

        }

        @Override
        public void forceRollback() throws SQLException {

        }

        @Override
        public void discard(Throwable t) {

        }

        @Override
        public void kill() throws SQLException {

        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}
