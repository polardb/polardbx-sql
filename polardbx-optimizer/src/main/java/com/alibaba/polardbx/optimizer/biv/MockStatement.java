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

package com.alibaba.polardbx.optimizer.biv;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class MockStatement implements Statement {

    private MockConnection mockConnection;
    private String sql;
    private MockCacheData mockCacheData;

    public MockStatement(MockConnection mockConnection) {
        this.mockConnection = mockConnection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        String traceId = MockUtils.stripStraceId(sql);
        sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        if (MockUtils.isSqlNeedTransparent(sql, mockConnection.getLogicalSchema(), null)) {
            return new MockResultSet(MockDataManager.buildCacheData(sql, mockConnection));
        }
        this.sql = sql;
        return new MockResultSet(this, MockUtils.buildMeta(sql, mockConnection.getLogicalSchema(), null),
            MockDataManager.isMockEnoughData(traceId + sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 1;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        this.sql = sql;
        if (MockUtils.isSqlNeedTransparent(sql, mockConnection.getLogicalSchema(), null)) {
            mockCacheData = MockDataManager.buildCacheData(sql, this.mockConnection);
            return true;
        }
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (mockCacheData != null) {
            return new MockResultSet(mockCacheData);
        } else {
            String traceId = MockUtils.stripStraceId(sql);
            return new MockResultSet(this, MockUtils.buildMeta(sql, mockConnection.getLogicalSchema(), null),
                MockDataManager.isMockEnoughData(traceId + MockUtils.stripSql(sql, mockConnection.getLogicalSchema(),
                    null)));
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        executeUpdate(sql);
    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {

        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        String traceId = MockUtils.stripStraceId(sql);
        return new MockResultSet(this, MockUtils.buildMeta(sql, mockConnection.getLogicalSchema(), null),
            MockDataManager.isMockEnoughData(traceId + MockUtils.stripSql(sql, mockConnection.getLogicalSchema(),
                null)));
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        ;
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        this.sql = MockUtils.stripSql(sql, mockConnection.getLogicalSchema(), null);
        ;
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
