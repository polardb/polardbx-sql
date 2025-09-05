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
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigWithIndexNameRecord;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Function;

public class MetaDbUtil {

    public static final String POLARDB_VERSION_SQL =
        "SELECT @@polardbx_engine_version as version, @@polardbx_release_date as release_date";

    public static Connection getConnection() {
        if (MetaDbDataSource.getInstance() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "acquire meta db error");
        }
        Connection c = MetaDbDataSource.getInstance().getConnection();
        if (c == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "acquire meta db conn error");
        }
        return c;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public static boolean execute(String sql, Connection connection) throws SQLException {
        validate(sql);
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }

    public static int executeDDL(String ddl, Connection connection) throws SQLException {
        validate(ddl);
        try (Statement statement = connection.createStatement()) {
            return statement.executeUpdate(ddl);
        }
    }

    public static List<Map<String, Object>> queryCount(String sql, Connection connection) throws SQLException {
        validate(sql);
        return executeCount(sql, null, connection);
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

    public static Long insertAndReturnLastInsertId(String insertSql, List<Map<Integer, ParameterContext>> paramsBatch,
                                                   Connection connection) throws SQLException {
        return executeInsertAndReturnLastInsertId(insertSql, paramsBatch, connection);
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

    public static List<Map<String, Object>> executeCount(String sql, Map<Integer, ParameterContext> params,
                                                         Connection connection)
        throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (params != null && params.size() > 0) {
                for (ParameterContext param : params.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        }
        return results;
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

    public static Long executeInsertAndReturnLastInsertId(String sql, List<Map<Integer, ParameterContext>> paramsBatch,
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

    public static <T> void setParameters(final Map<Integer, ParameterContext> params, final ParameterMethod method,
                                         final Iterable<T> values) {
        final Iterator<T> iterator = values.iterator();
        int i = 1;
        while (iterator.hasNext()) {
            setParameter(i, params, method, iterator.next());
            i++;
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
            if (conn instanceof XConnection) {
                try {
                    ((XConnection) conn).setLastException(ex, true);
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
                int iso = conn.getTransactionIsolation();
                try {
                    conn.setAutoCommit(false);
                    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                    result = func.apply(conn);

                } finally {
                    conn.setTransactionIsolation(iso);
                    conn.setAutoCommit(true);
                }

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

    /**
     * version: select @@polardbx_engine_version as version
     * releaseDate: select @@polardbx_release_date as release_date
     *
     * @return {Version}-{ReleaseDate}
     */
    public static String getGmsPolardbVersion() throws Exception {
        String sql = POLARDB_VERSION_SQL;
        String dnPolardbxVersion = null;
        String dnReleaseDate = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                dnPolardbxVersion = rs.getString(1);
                dnReleaseDate = rs.getString(2);
            }
            return String.format("%s-%s", dnPolardbxVersion, dnReleaseDate);
        }
    }

    public static boolean hasColumn(String tableName, String columnName) throws SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + tableName)) {
            while (rs.next()) {
                String field = rs.getString(1);
                if (field.equalsIgnoreCase(columnName)) {
                    return true;
                }
            }
            return false;
        } catch (SQLException ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw ex;
        }
    }

    public static void setGlobal(Properties properties) throws SQLException {
        try (Connection metaDbConn = getConnection()) {
            InstConfigAccessor instConfigAccessor = new InstConfigAccessor();
            instConfigAccessor.setConnection(metaDbConn);
            instConfigAccessor.updateInstConfigValue(InstIdUtil.getInstId(), properties);
        }
    }

    public static boolean hasTable(String tableName) throws SQLException {
        String sql = String.format("SHOW TABLES LIKE '%s'", tableName);

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return true;
            }
            return false;
        } catch (SQLException ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw ex;
        }
    }

    public static boolean tryGetLock(Connection conn, String lockObj, long timeout) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery(
                String.format("SELECT GET_LOCK('" + lockObj + "', %d) ", timeout))) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Throwable e) {
            MetaDbLogUtil.META_DB_LOG.error("tryGetLock error", e);
            return false;
        }
    }

    public static boolean releaseLock(Connection conn, String lockObj) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT RELEASE_LOCK('" + lockObj + "') ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("releaseLock error", e);
            return false;
        }
    }

    /**
     * Generate columnar config for all columnar indexes of a single logical table.
     */
    public static void generateColumnarConfig(String schemaName, String tableName,
                                              Map<String, Map<String, String>> records,
                                              Map<String, String> globalConfig) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            generateColumnarConfig(connection, schemaName, tableName, records, globalConfig);
        } catch (Throwable t) {
            MetaDbLogUtil.META_DB_LOG.error("Get columnar config error.", t);
        }
    }

    public static void generateColumnarConfig(Connection connection, String schemaName, String tableName,
                                              Map<String, Map<String, String>> records,
                                              Map<String, String> globalConfig) {
        ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
        accessor.setConnection(connection);
        for (ColumnarConfigWithIndexNameRecord record : accessor.query(schemaName, tableName)) {
            if (0 == record.tableId) {
                globalConfig.put(record.configKey.toUpperCase(), record.configValue);
                continue;
            }
            records.computeIfAbsent(record.indexName, k -> new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER))
                .put(record.configKey.toUpperCase(), record.configValue);
        }
    }
}
