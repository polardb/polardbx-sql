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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class MppMockConnection implements IConnection {

    /**
     * 保存 select sql_calc_found_rows 返回的结果
     */
    private long foundRows = 1;

    private String user;
    private String catalog;
    private String schema;

    private int socketTimeout = -1;
    private long lastInsertId;

    public MppMockConnection(String user, String catalog, String schema) {
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public void setUser(String userName) {
        this.user = userName;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return schema;
    }

    @Override
    public void kill() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastInsertId() {
        return lastInsertId;
//        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastInsertId(long id) {
        this.lastInsertId = id;
    }

    @Override
    public long getReturnedLastInsertId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReturnedLastInsertId(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Long> getGeneratedKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchInsertPolicy getBatchInsertPolicy(
        Map<String, Object> extraCmds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBatchInsertPolicy(BatchInsertPolicy policy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEncoding(String encoding) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getEncoding() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSqlMode(String sqlMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSqlMode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStressTestValid(boolean stressTestValid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStressTestValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        this.socketTimeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() {
        return socketTimeout;
    }

    @Override
    public Map<String, Object> getServerVariables() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setServerVariables(Map<String, Object> serverVariables) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFoundRows() {
        return foundRows;
    }

    @Override
    public void setFoundRows(long foundRows) {
        this.foundRows = foundRows;
    }

    @Override
    public long getAffectedRows() {
        return 0;
    }

    @Override
    public void setAffectedRows(long affectedRows) {
        // do nothing
    }

    @Override
    public void executeLater(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flushUnsent() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability
    ) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability
    ) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
