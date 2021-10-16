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

package com.alibaba.polardbx.common.mock;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.mock.MockDataSource.ExecuteInfo;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class MockConnection implements Connection {

    private MockDataSource mockDataSource;

    private long timeToClose = 0;
    private int closeInvokingTimes = 0;
    private boolean isClosed = false;
    private boolean autoCommit = true;
    private int commitInvokingTimes = 0;
    private int getMetadataInvokingTimes = 0;
    private int transactionIsolation = -1;
    private boolean isReadOnly;
    private int rollbackInvotingTimes = 0;

    public void clearWarnings() throws SQLException {
        throw new NotSupportException("");
    }

    public MockConnection(String method, MockDataSource mockDataSource) {
        this.mockDataSource = mockDataSource;
        MockDataSource.record(new ExecuteInfo(this.mockDataSource, method, null, null));
    }

    public void close() throws SQLException {
        try {
            Thread.sleep(timeToClose);
        } catch (Exception e) {
        }
        closeInvokingTimes++;
        isClosed = true;
    }

    protected void checkClose() throws SQLException {
        if (isClosed) {
            throw new SQLException("closed");
        }
    }

    public void commit() throws SQLException {
        mockDataSource.checkState();
        checkClose();
        MockDataSource.record(new ExecuteInfo(mockDataSource, "commit", null, null));
        commitInvokingTimes++;
    }

    private static void checkPreException(String name) throws SQLException {
        SQLException e = MockDataSource.popPreException(name);
        if (e != null) {
            throw e;
        }
    }

    public Statement createStatement() throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_createStatement);
        return new MockStatement("createStatement", this.mockDataSource);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_createStatement);
        return new MockStatement("createStatement#int_int", this.mockDataSource);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_createStatement);
        return new MockStatement("createStatement#int_int_int", this.mockDataSource);
    }

    public boolean getAutoCommit() throws SQLException {
        mockDataSource.checkState();
        checkClose();
        return autoCommit;
    }

    public String getCatalog() throws SQLException {
        throw new NotSupportException("");
    }

    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        mockDataSource.checkState();
        checkClose();
        getMetadataInvokingTimes++;
        return new MockDataBaseMetaData();
    }

    public int getTransactionIsolation() throws SQLException {
        mockDataSource.checkState();
        checkClose();

        return transactionIsolation;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new NotSupportException("");
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new NotSupportException("");
    }

    public boolean isClosed() throws SQLException {
        mockDataSource.checkState();
        return isClosed;
    }

    public boolean isReadOnly() throws SQLException {
        mockDataSource.checkState();
        return isReadOnly;
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new NotSupportException("");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new NotSupportException("");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new NotSupportException("");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new NotSupportException("");
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement", this.mockDataSource, sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement#string_int", this.mockDataSource, sql);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement#string_int[", this.mockDataSource, sql);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement#string_String[", this.mockDataSource, sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement#string_int_int", this.mockDataSource, sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        mockDataSource.checkState();
        checkPreException(MockDataSource.m_prepareStatement);
        return new MockPreparedStatement("prepareStatement#string_int_int_int", this.mockDataSource, sql);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new NotSupportException();
    }

    public void rollback() throws SQLException {
        mockDataSource.checkState();
        checkClose();
        MockDataSource.record(new ExecuteInfo(mockDataSource, "rollback", null, null));
        rollbackInvotingTimes++;
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        mockDataSource.checkState();
        checkClose();
        MockDataSource.record(new ExecuteInfo(mockDataSource, "rollback#savepoint", null, new Object[] {savepoint}));
        rollbackInvotingTimes++;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        mockDataSource.checkState();
        checkClose();
        MockDataSource.record(new ExecuteInfo(mockDataSource, "setAutoCommit", null, new Object[] {autoCommit}));
        this.autoCommit = autoCommit;
    }

    public void setCatalog(String catalog) throws SQLException {
        throw new NotSupportException("");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new NotSupportException("");
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        mockDataSource.checkState();
        this.isReadOnly = true;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        mockDataSource.checkState();
        this.transactionIsolation = level;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new NotSupportException("");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new NotSupportException("");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException("");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public Clob createClob() throws SQLException {
        return null;
    }

    public Blob createBlob() throws SQLException {
        return null;
    }

    public NClob createNClob() throws SQLException {

        return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    public Properties getClientInfo() throws SQLException {
        return null;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    public int getCloseInvokingTimes() {
        return closeInvokingTimes;
    }

    public int getCommitInvokingTimes() {
        return commitInvokingTimes;
    }

    public int getGetMetadataInvokingTimes() {
        return getMetadataInvokingTimes;
    }

    public int getRollbackInvotingTimes() {
        return rollbackInvotingTimes;
    }

    public void setSchema(String schema) throws SQLException {
        throw new NotSupportException("setSchemas");
    }

    public String getSchema() throws SQLException {
        throw new NotSupportException("getSchema");
    }

    public void abort(Executor executor) throws SQLException {
        throw new NotSupportException("abort");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new NotSupportException("setNetworkTimeout");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new NotSupportException("getNetworkTimeout");
    }

}
