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

import com.alibaba.druid.filter.FilterAdapter;
import com.alibaba.druid.filter.FilterChain;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.proxy.jdbc.ConnectionProxy;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.MysqlIO;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class TAtomDataSourceFilter extends FilterAdapter {

    private final static Logger logger = LoggerFactory.getLogger(TAtomDataSourceFilter.class);

    protected static final int MAX_IO_BUFFER_TO_CACHE = 1024 * 10;                                           // don't
    // cache
    // buffer
    // above
    // 10KB
    protected static final int INITIAL_PACKET_SIZE = 1024;

    @Override
    public void init(DataSourceProxy dataSource) {
    }

    @Override
    public ConnectionProxy connection_connect(FilterChain chain, Properties info) throws SQLException {
        final ConnectionProxy conn = chain.connection_connect(info);

        try {
            return new TAtomConnectionProxy(conn.getDirectDataSource(),
                conn.getRawObject(),
                conn.getProperties(),
                conn.getId(),
                Maps.newHashMap(loadSystemVariables(conn)),
                loadSystemGlobalVariables(conn),
                ((ConnectionImpl) conn.getRawObject()).getId()
            );
        } catch (SQLException e) {
            GeneralUtil.close(conn);
            throw e;
        }
    }

    private Map<String, Object> loadSystemVariables(final ConnectionProxy conn) throws SQLException {
        return loadSystemFromCache(CacheVariables.cache, conn, "SHOW VARIABLES");
    }

    private Map<String, Object> loadSystemGlobalVariables(final ConnectionProxy conn) throws SQLException {
        return loadSystemFromCache(CacheVariables.globalCache, conn, "SHOW GLOBAL VARIABLES");
    }

    private Map<String, Object> loadSystemFromCache(final Cache<String, Map<String, Object>> cache,
                                                    final ConnectionProxy conn, final String sql) throws SQLException {
        try {
            DataSourceProxy dataSourceProxy = conn.getDirectDataSource();
            if (!(dataSourceProxy instanceof DruidDataSource)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ATOM_NOT_AVALILABLE, "Only support DruidDataSource");
            }
            DruidDataSource druidDs = (DruidDataSource) dataSourceProxy;
            String dsName = druidDs.getName();

            return cache.get(dsName, new Callable<Map<String, Object>>() {

                @Override
                public Map<String, Object> call() throws Exception {
                    return loadServerVariables(conn, sql);
                }

            });
        } catch (ExecutionException | UncheckedExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            } else {
                throw new SQLException(e);
            }
        } catch (Throwable throwable) {
            logger.error(throwable.getMessage());
            throw new TddlException(ErrorCode.ERR_ATOM_DB_DOWN, throwable, "load variables error,may be db is down");
        }
    }

    public static Map<String, Object> loadServerVariables(Connection conn, String query) throws SQLException {
        Map<String, Object> serverVariables = new HashMap<String, Object>();
        ResultSet results = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            results = stmt.executeQuery(query);
            while (results.next()) {
                /**
                 * 这个地方全部用string存储,具体执行到后端mysql的时候再去区别是否为纯数字类型.
                 */
                String key = results.getString(1);
                String val = results.getString(2);
                if (results.wasNull() || StringUtils.isEmpty(val)) {
                    val = "NULL";
                }
                serverVariables.put(key.toLowerCase(), val);
            }

            results.close();
            results = null;

            return serverVariables;
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException sqlE) {
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlE) {
                }
            }
        }

    }

    static Field sendPacketField = null;
    static Field reusablePacketField = null;
    static Field sharedSendPacketField = null;

    static {
        try {
            sendPacketField = MysqlIO.class.getDeclaredField("sendPacket");
            sendPacketField.setAccessible(true);

            reusablePacketField = MysqlIO.class.getDeclaredField("reusablePacket");
            reusablePacketField.setAccessible(true);

            sharedSendPacketField = MysqlIO.class.getDeclaredField("sharedSendPacket");
            sharedSendPacketField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    // MysqlIO DO NOT shrink the buffer, so if it is too large, we should clean
    // it.
    @Override
    public void dataSource_releaseConnection(FilterChain chain, DruidPooledConnection connection) throws SQLException {
        try {
            ConnectionImpl mysqlConnection = connection.unwrap(ConnectionImpl.class);
            if (mysqlConnection == null) {
                return;
            }
            MysqlIO io = mysqlConnection.getIO();
            if (io == null) {
                return;
            }
            Buffer sendPacket = null;
            Buffer reusablePacket = null;
            Buffer sharedSendPacket = null;
            try {
                sendPacket = (Buffer) sendPacketField.get(io);
                reusablePacket = (Buffer) reusablePacketField.get(io);
                sharedSendPacket = (Buffer) sharedSendPacketField.get(io);
            } catch (IllegalArgumentException e) {
                logger.error(e);
                return;
            } catch (IllegalAccessException e) {
                logger.error(e);
                return;
            }

            if (sendPacket != null && sendPacket.getByteBuffer().length > MAX_IO_BUFFER_TO_CACHE) {
                try {
                    sendPacketField.set(io, null);
                } catch (IllegalArgumentException e) {
                    logger.error(e);
                } catch (IllegalAccessException e) {
                    logger.error(e);
                } catch (Throwable e) {
                    // make sure that can catch all exception
                    logger.error(e);
                }
            }

            if (reusablePacket != null && reusablePacket.getByteBuffer().length > MAX_IO_BUFFER_TO_CACHE) {
                try {
                    reusablePacketField.set(io, new Buffer(new byte[INITIAL_PACKET_SIZE]));
                } catch (IllegalArgumentException e) {
                    logger.error(e);
                } catch (IllegalAccessException e) {
                    logger.error(e);
                } catch (Throwable e) {
                    // make sure that can catch all exception
                    logger.error(e);
                }
            }

            if (sharedSendPacket != null && sharedSendPacket.getByteBuffer().length > MAX_IO_BUFFER_TO_CACHE) {
                try {
                    sharedSendPacketField.set(io, new Buffer(new byte[INITIAL_PACKET_SIZE]));
                } catch (IllegalArgumentException e) {
                    logger.error(e);
                } catch (IllegalAccessException e) {
                    logger.error(e);
                } catch (Throwable e) {
                    // make sure that can catch all exception
                    logger.error(e);
                }
            }

        } finally {
            chain.dataSource_recycle(connection);
        }
    }
}
