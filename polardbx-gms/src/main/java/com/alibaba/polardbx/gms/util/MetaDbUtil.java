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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MetaDbUtil {

    public static Connection getConnection() {
        return MetaDbDataSource.getInstance().getConnection();
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public static void executeDDL(String ddl, Connection connection) throws SQLException {
        validate(ddl);
        execute(ddl, null, connection);
    }

    public static <T extends SystemTableRecord>
    List<T> query(String selectSql, Class<T> clazz, Connection connection) throws Exception {
        return query(selectSql, null, clazz, connection);
    }

    public static <T extends SystemTableRecord>
    List<T> query(String selectSql, Map<Integer, ParameterContext> params, Class<T> clazz, Connection connection)
        throws Exception {
        validate(selectSql);
        List<T> records = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(selectSql)) {
            if (params != null && params.size() > 0) {
                for (ParameterContext param : params.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    records.add(clazz.newInstance().fill(rs));
                }
            }
        }
        return records;
    }

    public static int insert(String insertSql, Map<Integer, ParameterContext> params, Connection connection)
        throws SQLException {
        validate(insertSql, params);
        return execute(insertSql, params, connection);
    }

    public static int[] insert(String insertSql, List<Map<Integer, ParameterContext>> paramsBatch,
                               Connection connection) throws SQLException {
        return executeBatch(insertSql, paramsBatch, connection);
    }

    public static Long insertAndRetureLastInsertId(String insertSql, List<Map<Integer, ParameterContext>> paramsBatch,
                                                   Connection connection) throws SQLException {
        return executeInsertAndRetureLastInsertId(insertSql, paramsBatch, connection);
    }

    public static int update(String updateSql, Map<Integer, ParameterContext> params, Connection connection)
        throws SQLException {
        validate(updateSql, params);
        return execute(updateSql, params, connection);
    }

    public static int[] update(String updateSql, List<Map<Integer, ParameterContext>> paramsBatch,
                               Connection connection) throws SQLException {
        return executeBatch(updateSql, paramsBatch, connection);
    }

    public static int truncate(String truncateSql, Connection connection) throws SQLException {
        validate(truncateSql);
        return execute(truncateSql, null, connection);
    }

    public static int delete(String deleteSql, Connection connection) throws SQLException {
        validate(deleteSql);
        return execute(deleteSql, null, connection);
    }

    public static int delete(String deleteSql, Map<Integer, ParameterContext> params, Connection connection)
        throws SQLException {
        validate(deleteSql, params);
        return execute(deleteSql, params, connection);
    }

    public static int[] delete(String deleteSql, List<Map<Integer, ParameterContext>> paramsBatch,
                               Connection connection) throws SQLException {
        return executeBatch(deleteSql, paramsBatch, connection);
    }

    public static int execute(String sql, Map<Integer, ParameterContext> params, Connection connection)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (params != null && params.size() > 0) {
                for (ParameterContext param : params.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
            }
            return ps.executeUpdate();
        }
    }

    public static int[] executeBatch(String sql, List<Map<Integer, ParameterContext>> paramsBatch,
                                     Connection connection) throws SQLException {
        validate(sql, paramsBatch);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Map<Integer, ParameterContext> params : paramsBatch) {
                for (ParameterContext param : params.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    public static Long executeInsertAndRetureLastInsertId(String sql, List<Map<Integer, ParameterContext>> paramsBatch,
                                                          Connection connection) throws SQLException {
        validate(sql, paramsBatch);
        try (PreparedStatement ps = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            for (Map<Integer, ParameterContext> params : paramsBatch) {
                for (ParameterContext param : params.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet resultSet = ps.getGeneratedKeys();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            } else {
                return null;
            }
        }
    }

    public static Map<Integer, ParameterContext> buildStringParameters(final String[] values) {
        return buildParameters(ParameterMethod.setString, values);
    }

    public static Map<Integer, ParameterContext> buildParameters(final ParameterMethod method, final Object[] values) {
        final Map<Integer, ParameterContext> params = new HashMap<>(values.length);
        setParameters(params, method, values);
        return params;
    }

    public static void setParameters(final Map<Integer, ParameterContext> params, final ParameterMethod method,
                                     final Object[] values) {
        for (int i = 0; i < values.length; i++) {
            setParameter(i + 1, params, method, values[i]);
        }
    }

    public static void setParameter(final int index, final Map<Integer, ParameterContext> params,
                                    final ParameterMethod method, final Object value) {
        params.put(index, new ParameterContext(method, new Object[] {index, value}));
    }

    public static void validate(String sql) {
        if (TStringUtil.isBlank(sql)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                "the input SQL statement is null or empty");
        }
    }

    public static void validate(String sql, Object param) {
        validate(sql);
        if (param == null || (param instanceof String && TStringUtil.isBlank((String) param))
            || (param instanceof List && ((List) param).size() == 0)
            || (param instanceof Map && ((Map) param).size() == 0)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS, "input arguments are null or empty");
        }
    }

    public static void beginTransaction(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public static void endTransaction(Connection conn, Logger logger) {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            String errMsg = "Failed to turn AutoCommit on. Caused by: " + e.getMessage();
            if (logger != null) {
                logger.error(errMsg, e);
            } else {
                MetaDbLogUtil.META_DB_LOG.error(errMsg, e);
            }
        }
    }

    public static void commit(Connection conn) throws SQLException {
        conn.commit();
    }

    public static void rollback(Connection conn, Exception e, Logger logger, String action) {
        rollback(conn, e, logger, null, null, action);
    }

    public static void rollback(Connection conn, Exception e, Logger logger, String schemaName, String action) {
        rollback(conn, e, logger, schemaName, null, action);
    }

    public static void rollback(Connection conn, Exception e, Logger logger, String schemaName, String tableName,
                                String action) {
        String errMsg;
        try {
            conn.rollback();
        } catch (SQLException ex) {
            errMsg = "Failed to rollback due to " + ex.getMessage();
            if (logger != null) {
                logger.error(errMsg, ex);
            } else {
                MetaDbLogUtil.META_DB_LOG.error(errMsg, ex);
            }
            if(conn instanceof XConnection){
                try {
                    ((XConnection) conn).setLastException(ex);
                } catch (SQLException throwables) {
                    if (logger != null) {
                        logger.error("setLastException error", throwables);
                    } else {
                        MetaDbLogUtil.META_DB_LOG.error("setLastException error", throwables);
                    }
                } finally {
                    throw new TddlNestableRuntimeException(ex);
                }
            }
        }
        if (TStringUtil.isNotBlank(tableName)) {
            errMsg = "Failed to " + action + " for " + schemaName + "." + tableName;
            if (logger != null) {
                logger.error(errMsg, e);
            } else {
                MetaDbLogUtil.META_DB_LOG.error(errMsg, e);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, e, action, e.getMessage());
        } else if (TStringUtil.isNotBlank(schemaName)) {
            errMsg = "Failed to " + action + " in " + schemaName;
            if (logger != null) {
                logger.error(errMsg, e);
            } else {
                MetaDbLogUtil.META_DB_LOG.error(errMsg, e);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, e, action, e.getMessage());
        }
    }

    /**
     * Create a connection to metadb and execute query
     */
    public static <T> T queryMetaDbWrapper(Connection metaDbConn, Function<Connection, T> func) {
        T result = null;

        if (metaDbConn == null) {
            try (Connection conn = getConnection()) {
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                result = func.apply(conn);

                conn.setAutoCommit(true);
                return result;
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
        } else {
            try {
                result = func.apply(metaDbConn);
                return result;
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
        }
    }
}
