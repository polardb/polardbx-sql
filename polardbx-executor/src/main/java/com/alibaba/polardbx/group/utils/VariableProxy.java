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

package com.alibaba.polardbx.group.utils;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.variable.IVariableProxy;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * @author dylan
 */
public class VariableProxy implements IVariableProxy {

    private static final Logger logger = LoggerFactory.getLogger(VariableProxy.class);

    private TGroupDataSource tGroupDataSource;

    public VariableProxy(TGroupDataSource tGroupDataSource) {
        this.tGroupDataSource = tGroupDataSource;
    }

    public void resetDataSource(TGroupDataSource dataSource) {
        if (dataSource == null) {
            logger.error("resetDataSource dataSource is null");
        }
        this.tGroupDataSource = dataSource;
    }

    @Override
    public ImmutableMap<String, Object> getSessionVariables() {
        if (ConfigDataMode.getInstanceRole() == InstanceRole.COLUMNAR_SLAVE) {
            return getSessionVariablesForColumnarRO();
        }

        if (!ConfigDataMode.needDNResource()) {
            return ImmutableMap.<String, Object>builder().build();
        }
        TGroupDirectConnection tGroupDirectConnection = null;
        try {
            tGroupDirectConnection = tGroupDataSource.getConnection();
            Connection conn = tGroupDirectConnection.getConn();

            if (conn.isWrapperFor(XConnection.class)) {
                final XClient client = conn.unwrap(XConnection.class).getSession().getClient();
                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                builder.putAll(client.getSessionVariablesL());
                return builder.build();
            } else {
                throw new AssertionError();
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        } finally {
            JdbcUtils.close(tGroupDirectConnection);
        }
    }

    private ImmutableMap<String, Object> getSessionVariablesForColumnarRO() {
        // get session variables from GMS.

        try (Connection conn = MetaDbUtil.getConnection()) {
            if (conn.isWrapperFor(XConnection.class)) {
                // X-protocol Mode.
                final XClient client = conn.unwrap(XConnection.class).getSession().getClient();
                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                builder.putAll(client.getSessionVariablesL());
                return builder.build();
            } else {
                // JDBC Mode
                ImmutableMap.Builder<String, Object> variables = ImmutableMap.builder();

                String sql = "SHOW SESSION VARIABLES";
                Statement statement = null;
                ResultSet resultSet = null;
                try {
                    statement = conn.createStatement();
                    resultSet = statement.executeQuery(sql);

                    while (resultSet.next()) {
                        String variableName = resultSet.getString("Variable_name");
                        String variableValue = resultSet.getString("Value");
                        variables.put(variableName, variableValue);
                    }

                } finally {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                }

                return variables.build();
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    @Override
    public ImmutableMap<String, Object> getGlobalVariables() {
        if (!ConfigDataMode.needDNResource()) {
            return ImmutableMap.<String, Object>builder().build();
        }
        TGroupDirectConnection tGroupDirectConnection = null;
        try {
            tGroupDirectConnection = tGroupDataSource.getConnection();
            Connection conn = tGroupDirectConnection.getConn();
            if (conn.isWrapperFor(XConnection.class)) {
                final XClient client = conn.unwrap(XConnection.class).getSession().getClient();
                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                builder.putAll(client.getGlobalVariablesL());
                return builder.build();
            } else {
                throw new AssertionError();
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        } finally {
            JdbcUtils.close(tGroupDirectConnection);
        }
    }

}
