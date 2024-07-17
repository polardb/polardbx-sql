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

package com.alibaba.polardbx.atom;

import com.alibaba.druid.proxy.jdbc.ConnectionProxyImpl;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.polardbx.atom.utils.EncodingUtils;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

public class TAtomConnectionProxy extends ConnectionProxyImpl {

    private static final Logger logger = LoggerFactory.getLogger(TAtomConnectionProxy.class);
    private Map<String, Object> currentGlobalServerVariables;
    final private Map<String, Object> sessionVariables;
    final private Map<String, Object> sessionVariablesChanged = new HashMap<String, Object>();
    private String encoding;
    final private Connection connection;
    final private long mysqlConnectionId;

    public TAtomConnectionProxy(DataSourceProxy dataSource, Connection connection, Properties properties, long id,
                                Map<String, Object> serverVariables,
                                Map<String, Object> globalServerVariables,
                                long mysqlConnectionId) throws SQLException {
        super(dataSource, connection, properties, id);
        this.sessionVariables = serverVariables;
        this.currentGlobalServerVariables = globalServerVariables;
        this.encoding = EncodingUtils.getEncoding(connection);
        this.connection = connection;
        this.mysqlConnectionId = mysqlConnectionId;
    }

    private void setVariables(Map<String, Object> newVariables, boolean isGlobal) throws SQLException {

        Map<String, Object> serverVariables = null;
        Set<String> serverVariablesNeedToRemove = Sets.newHashSet();
        if (!isGlobal) {
            for (String key : sessionVariablesChanged.keySet()) {
                // 这个连接没设置过的变量，需要用global值复原
                if (newVariables.containsKey(key)) {
                    continue;
                }

                if (serverVariables == null) {
                    serverVariables = new HashMap<String, Object>(newVariables); // 已经全部小写
                }

                if (currentGlobalServerVariables.containsKey(key)) {
                    serverVariables.put(key, currentGlobalServerVariables.get(key));
                } else {
                    serverVariablesNeedToRemove.add(key);
                }
            }
        }
        if (serverVariables == null) {
            serverVariables = newVariables; // 已经全部小写
        }

        boolean first = true;
        List<Object> parmas = new ArrayList<Object>();
        StringBuilder query = new StringBuilder("SET ");

        // 对比要设的值和已有的值，相同则跳过，不同则设成新值
        Map<String, Object> tmpVariablesChanged = new HashMap<String, Object>();

        for (Entry<String, Object> entry : serverVariables.entrySet()) {
            String key = entry.getKey(); // 已经全部小写
            Object newValue = entry.getValue();

            // 不处理 DRDS 自定义的系统变量
            if (ServerVariables.extraVariables.contains(key)) {
                continue;
            }

            // 对于 DN 仅支持 set global 设置的变量，禁止 set session
            if (!isGlobal && ServerVariables.isMysqlGlobal(key)) {
                continue;
            }

            // 处理采用变量赋值的情况如 :
            // set sql_mode=@@sql_mode;
            // set sql_mode=@@global.sql_mode;
            // set sql_mode=@@session.sql_mode;
            if (newValue instanceof String) {
                String newValueStr = (String) newValue;

                if (newValueStr.startsWith("@@")) {
                    String newValueLC = newValueStr.toLowerCase();
                    String keyRef;
                    if (newValueLC.startsWith("@@session.")) {
                        keyRef = newValueLC.substring("@@session.".length());
                        newValue = this.sessionVariables.get(keyRef);
                    } else if (newValueLC.startsWith("@@global.")) {
                        keyRef = newValueLC.substring("@@global.".length());
                        newValue = this.currentGlobalServerVariables.get(keyRef);
                    } else {
                        keyRef = newValueLC.substring("@@".length());
                        newValue = this.currentGlobalServerVariables.get(keyRef);
                    }

                } else if ("default".equalsIgnoreCase(newValueStr)) {
                    newValue = this.currentGlobalServerVariables.get(key);
                }
            }

            String newValueStr = String.valueOf(newValue);
            Object oldValue = isGlobal ? this.currentGlobalServerVariables.get(key) : this.sessionVariables.get(key);
            if (oldValue != null) {
                String oldValuesStr = String.valueOf(oldValue);
                if (TStringUtil.equalsIgnoreCase(newValueStr, oldValuesStr)) {
                    // skip same value
                    continue;
                } else if (StringUtils.isEmpty(newValueStr) && "NULL".equalsIgnoreCase(oldValuesStr)) {
                    // skip same value
                    continue;
                } else if (StringUtils.isEmpty(oldValuesStr) && "NULL".equalsIgnoreCase(newValueStr)) {
                    // skip same value
                    continue;
                }
            }

            if (!first) {
                query.append(" , ");
            } else {
                first = false;
            }

            boolean isBoth = ServerVariables.isMysqlBoth(key);
            if (isGlobal) {
                query.append(" GLOBAL ");
            }

            StringBuilder tmpQuery = new StringBuilder(" , ");

            query.append("`").append(key).append("`=");
            tmpQuery.append("`").append(key).append("`=");
            if (TStringUtil.isParsableNumber(newValueStr)) { // 纯数字类型
                query.append(newValueStr);
                tmpQuery.append(newValueStr);
            } else if ("NULL".equalsIgnoreCase(newValueStr) ||
                newValue == null ||
                StringUtils.isEmpty(newValueStr)) { // NULL或空字符串类型
                if (ServerVariables.canExecByBoth.contains(key) || ServerVariables.canOnlyExecByNullVariables
                    .contains(key)) {
                    query.append("NULL");
                    tmpQuery.append("NULL");
                } else if (ServerVariables.canOnlyExecByEmptyStrVariables.contains(key)) {
                    query.append("''");
                    tmpQuery.append("''");
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_VARIABLE_CAN_NOT_SET_TO_NULL_FOR_NOW, key);
                }
            } else { // 字符串兼容类型
                query.append("?");
                parmas.add(newValue);
                if (isBoth) {
                    tmpQuery.append("?");
                    parmas.add(newValue);
                }
            }

            if (isBoth) {
                query.append(tmpQuery);
            }

            tmpVariablesChanged.put(key, newValue);
        }

        for (String key : serverVariablesNeedToRemove) {
            if (!first) {
                query.append(", ");
            }
            if (key.equalsIgnoreCase("sql_log_bin")) {
                query.append(key).append("=").append("'ON'");
            } else {
                query.append("@").append(key).append("=").append("NULL");
            }
        }

        if (!first) { // 需要确保SET指令是完整的, 而不是只有一个SET前缀.
            PreparedStatement ps = null;
            try {
                ps = this.getConnectionRaw().prepareStatement(query.toString());
                if (!GeneralUtil.isEmpty(parmas)) {
                    for (int i = 0; i < parmas.size(); i++) {
                        ps.setObject(i + 1, parmas.get(i));
                    }
                }
                ps.executeUpdate();
                ps.close();
                ps = null;
                if (!isGlobal) {
                    for (String key : serverVariablesNeedToRemove) {
                        sessionVariables.remove(key);
                        sessionVariablesChanged.remove(key);
                    }
                    for (Entry<String, Object> e : tmpVariablesChanged.entrySet()) {
                        sessionVariables.put(e.getKey(), e.getValue());
                        sessionVariablesChanged.put(e.getKey(), e.getValue());
                    }
                } else {
                    if (!tmpVariablesChanged.isEmpty()) {
                        // copy on write
                        this.currentGlobalServerVariables = new HashMap<>(currentGlobalServerVariables);
                    }
                    for (Entry<String, Object> e : tmpVariablesChanged.entrySet()) {
                        currentGlobalServerVariables.put(e.getKey(), e.getValue());
                    }
                }
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (Throwable e) {
                        logger.error("", e);
                    }
                }
            }
        }
    }

    public void setSessionVariables(Map<String, Object> newServerVariables)
        throws SQLException {
        setVariables(newServerVariables, false);
    }

    public void setGlobalVariables(Map<String, Object> globalVariables) throws SQLException {
        setVariables(globalVariables, true);
    }

    public String getEncoding() {
        return encoding;
    }

    /**
     * IMPORTANT! must keep encoding filed in TAtomConnectionProxy identical with
     * encoding used in mysql
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public void clearWarnings() throws SQLException {
        connection.clearWarnings();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connection.createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connection.createStruct(typeName, attributes);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return connection.isValid(timeout);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return connection.prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return connection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return connection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return connection.prepareStatement(sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        connection.setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        connection.setReadOnly(readOnly);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connection.setTransactionIsolation(level);
    }

    public Map<String, Object> getCurrentGlobalServerVariables() {
        return currentGlobalServerVariables;
    }

    public Map<String, Object> getSessionVariables() {
        return sessionVariables;
    }

    public long getMysqlConnectionId() {
        return mysqlConnectionId;
    }
}
