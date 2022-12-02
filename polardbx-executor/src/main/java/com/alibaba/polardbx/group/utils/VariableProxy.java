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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.variable.IVariableProxy;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;

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

    public ImmutableMap<String, Object> getSessionVariables() {
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

    @Override
    public ImmutableMap<String, Object> getGlobalVariables() {
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
