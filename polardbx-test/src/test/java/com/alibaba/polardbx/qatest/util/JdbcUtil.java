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

package com.alibaba.polardbx.qatest.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.CrudBasedLockTestCase.CREATE_TABLE_LIKE_SQL;
import static com.alibaba.polardbx.qatest.CrudBasedLockTestCase.DROP_TABLE_SQL;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isStrictType;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

public class JdbcUtil {

    private static final Log log = LogFactory.getLog(JdbcUtil.class);

    public static final String READ_ONLY_EXCEPTION_MSG = "Could not retrieve transation read-only status server";

    /**
     * 关闭statment
     */
    public static void close(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            String errorMs = "[Close statement] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
    }

    /**
     * 关闭resultset
     */
    public static void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            String errorMs = "[Close resultSet] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
    }

    /**
     * 关闭connection
     */
    public static void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            String errorMs = "[Close conn] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
    }

    /**
     * createStatement, 如果失败则退出
     */
    public static Statement createStatement(Connection conn) {
        Statement statement = null;
        try {
            statement = conn.createStatement();
        } catch (SQLException e) {
            String errorMs = "[Create statement] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return statement;
    }

    /**
     * 执行sql,如果失败则assertError
     */
    public static void executeSuccess(Connection conn, String sql) {
        Statement statement = createStatement(conn);
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            String errorMs = "[Statement execute] failed:" + sql;
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            close(statement);
        }
    }

    public static void executeAndRetry(Connection conn, String sql, int retryTime) {
        String errorMs = null;
        for (int count = 0; count < retryTime; count++) {
            Statement statement = createStatement(conn);
            try {
                statement.execute(sql);
                return;
            } catch (SQLException e) {
                errorMs = "[Statement execute] failed:" + sql;
                log.error(errorMs, e);
            } finally {
                close(statement);
            }
        }
        Assert.fail(errorMs + " \n, retry time is : " + retryTime);
    }

    /**
     * 执行sql,如果失败则assertError
     */
    public static void executeSuccess(Statement statement, String sql) {
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            String errorMs = "[Statement executeSuccess] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            close(statement);
        }
    }

    /**
     * 执行sql,如果失败则assertError
     */
    public static void executeSuccess(PreparedStatement preparedStatement) {
        try {
            preparedStatement.execute();
        } catch (SQLException e) {
            String errorMs = "[PreparedStatement executeSuccess] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
    }

    /**
     * 执行查询,如果失败则assertError
     */
    public static ResultSet executeQuery(String sql, Statement statement) {
        ResultSet rs = null;
        try {
            rs = statement.executeQuery(sql.trim());
        } catch (SQLException e) {
            String errorMs = "[Execute statement query] failed and sql is : " + sql;
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return rs;
    }

    /**
     * 执行查询,如果失败则assertError
     */
    public static ResultSet executeQuery(String sql, PreparedStatement ps) {
        ResultSet rs = null;
        try {
            rs = ps.executeQuery();
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n" + e);
        }
        return rs;
    }

    /**
     * 执行查询,如果失败则assertError
     */
    public static ResultSet executeQuery(String sql, Connection c) {
        Statement ps = createStatement(c);
        ResultSet rs = null;
        try {
            rs = ps.executeQuery(sql);
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n" + e);
        }
        return rs;
    }

    /**
     * 执行查询,拿到第一列String类型的结果
     */
    public static String executeQueryAndGetFirstStringResult(String sql, Connection c) {
        return executeQueryAndGetStringResult(sql, c, 1);
    }

    /**
     * 执行查询,拿到第一列String类型的结果
     */
    public static String executeQueryAndGetStringResult(String sql, Connection c, int columnIndex) {
        Statement ps = createStatement(c);
        ResultSet rs = null;
        String result = "";
        try {
            rs = ps.executeQuery(sql);
            if (rs.next()) {
                result = rs.getString(columnIndex);
            }
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
//            Assert.fail(errorMs + " \n" + e);
        }
        return result;
    }

    /**
     * 执行查询,拿到第N列类型的全部结果
     */
    public static List<String> executeQueryAndGetColumnResult(String sql, Connection c, int columnIndex) {
        Statement ps = createStatement(c);
        ResultSet rs = null;
        List<String> result = new ArrayList<>();
        try {
            rs = ps.executeQuery(sql);
            while (rs.next()) {
                result.add(rs.getString(columnIndex));
            }
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
//            Assert.fail(errorMs + " \n" + e);
        }
        return result;
    }

    /**
     * 执行查询,拿到指定列名称的全部结果
     */
    public static List<String> executeQueryAndGetColumnResult(String sql, Connection c, String columnName) {
        Statement ps = createStatement(c);
        ResultSet rs = null;
        List<String> result = new ArrayList<>();
        try {
            rs = ps.executeQuery(sql);
            while (rs.next()) {
                result.add(rs.getString(columnName));
            }
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
//            Assert.fail(errorMs + " \n" + e);
        }
        return result;
    }

    /**
     * 执行更新
     */
    public static int executeUpdate(String sql, PreparedStatement ps) {
        try {
            int i = ps.executeUpdate();
            return i;
        } catch (SQLException e) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n" + e);
        }
        return -1;
    }

    /**
     * 执行executeUpdate
     */
    public static int executeUpdate(PreparedStatement preparedStatement) {
        int effectCount = 0;
        try {
            effectCount = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            String errorMs = "[PreparedStatement executeSuccess] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return effectCount;
    }

    /**
     * 更新数据
     */
    public static int updateData(Connection conn, String sql, List<Object> param) {
        PreparedStatement preparedStatement = preparedStatementSet(sql, param, conn);
        int updateCount = executeWithUpdateCount(preparedStatement);
        close(preparedStatement);
        return updateCount;
    }

    /**
     * 更新数据
     */
    public static int updateData(PreparedStatement preparedStatement, String sql, List<Object> param) {
        preparedStatementSet(preparedStatement, param);
        int updateCount = executeWithUpdateCount(preparedStatement);
        return updateCount;
    }

    /**
     * 批量更新数据
     */
    public static int[] updateDataBatch(Connection conn, String sql, List<List<Object>> params) {
        int[] effectCount = new int[10];
        PreparedStatement ps = preparedStatementBatch(sql, conn);
        if (params == null) {
            int affect = executeUpdate(ps);
            JdbcUtil.close(ps);
            return new int[] {affect};
        } else {
            for (List<Object> param : params) {
                ps = preparedStatementSet(ps, param);
                preparedStatementAddBatch(ps);
            }

            try {
                effectCount = preparedStatementExecuteBatch(ps);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                Assert.fail("sql 执行失败: " + sql);

            }
            JdbcUtil.close(ps);
            return effectCount;
        }
    }

    /**
     * 向 CN 发送 set global 语句
     */
    public static void setGlobal(Connection tddlConn, String sql) {
        executeUpdate(tddlConn, "set enable_set_global=true;" + sql);
    }

    public static void disableAutoForceIndex(Connection tddlConn) {
        executeUpdate(tddlConn, "set " + ConnectionProperties.ENABLE_AUTO_FORCE_INDEX + " =false;");
    }

    /**
     * 批量更新数据
     */
    public static int[] updateDataBatchIgnoreErr(Connection conn, String sql, List<List<Object>> params) {
        int[] effectCount = new int[10];
        PreparedStatement ps = preparedStatementBatch(sql, conn);
        if (params == null) {
            int affect = executeUpdate(ps);
            JdbcUtil.close(ps);
            return new int[] {affect};
        } else {
            for (List<Object> param : params) {
                ps = preparedStatementSet(ps, param);
                preparedStatementAddBatch(ps);
            }

            try {
                effectCount = preparedStatementExecuteBatch(ps);
            } catch (SQLException e) {
            }
            JdbcUtil.close(ps);
            return effectCount;
        }
    }

    public static int[] updateDataBatchReturnKeys(Connection conn, String sql, List<List<Object>> params,
                                                  List<Long> keys) {
        int[] effectCount = new int[10];
        PreparedStatement ps = preparedStatementBatch(sql, conn);
        if (params == null) {
            int affect = executeUpdate(ps);
            JdbcUtil.close(ps);
            return new int[] {affect};
        } else {
            for (List<Object> param : params) {
                ps = preparedStatementSet(ps, param);
                preparedStatementAddBatch(ps);
            }

            try {
                effectCount = preparedStatementExecuteBatch(ps);
                ResultSet rs = ps.getGeneratedKeys();
                while (rs.next()) {
                    long key = rs.getLong(1);
                    keys.add(key);
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                Assert.fail("sql 执行失败: " + sql);

            }
            JdbcUtil.close(ps);
            return effectCount;
        }
    }

    /**
     * 批量更新数据
     */
    public static void updateDataBatchFailed(Connection conn, String sql, List<List<Object>> params, String errMsg) {
        PreparedStatement ps = preparedStatementBatch(sql, conn);
        if (params == null) {
            try {
                ps.executeUpdate();
                Assert.fail("sql语句应该实行失败: " + sql);
            } catch (Exception e) {
                // log.error(e.getMessage(), e);
                Assert.assertTrue(e.getMessage().contains(errMsg));
            }
            JdbcUtil.close(ps);
        } else {
            for (List<Object> param : params) {
                ps = preparedStatementSet(ps, param);
                preparedStatementAddBatch(ps);
            }

            try {
                preparedStatementExecuteBatch(ps);
                Assert.fail("sql语句应该实行失败: " + sql);
            } catch (Exception e) {
                // log.error(e.getMessage(), e);
                Assert.assertTrue(e.getMessage().contains(errMsg));
            }
            JdbcUtil.close(ps);
        }
    }

    /**
     * 批量更新数据
     */
    public static int[] updateDataBatch(PreparedStatement ps, String sql, List<List<Object>> params) {
        int[] effectCount = new int[10];
        if (params == null) {
            int affect = executeUpdate(ps);
            return new int[] {affect};
        } else {
            for (List<Object> param : params) {
                ps = preparedStatementSet(ps, param);
                preparedStatementAddBatch(ps);
            }
            try {
                effectCount = preparedStatementExecuteBatch(ps);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                Assert.fail("sql 执行失败: " + sql);

            }
            return effectCount;
        }
    }

    /**
     * 更新数据，获取更新数量
     */
    private static int executeWithUpdateCount(PreparedStatement preparedStatement) {
        executeUpdate(preparedStatement);
        int updateCount = 0;
        try {
            updateCount = preparedStatement.getUpdateCount();
        } catch (SQLException e) {
            String errorMs = "[preparedStatement getUpdateCount] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return updateCount;
    }

    /**
     * tddl 更新数据
     */
    public static int updateDataTddl(Connection conn, String sql, List<Object> param) {
        PreparedStatement preparedStatement = preparedStatementSet(sql, param, conn, true);
        int updateCount = executeWithUpdateCount(preparedStatement);
        close(preparedStatement);
        return updateCount;
    }

    /**
     * Test if preparedStatement(sql, columnNames) passes RETURN_GENERATED_KEYS
     * to server.
     *
     * @return first result of getGeneratedKeys()
     */
    public static long updateDataTddlAutoGen(Connection conn, String sql, List<Object> param, String colName) {
        PreparedStatement preparedStatement = preparedStatementAutoGen(sql, param, conn, colName);
        executeWithUpdateCount(preparedStatement);
        long genValue = 0;
        try {
            ResultSet rs = preparedStatement.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            genValue = rs.getLong(1);
        } catch (Exception e) {
            String errorMs = "[preparedStatement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        close(preparedStatement);
        return genValue;
    }

    public static String getSqlMode(Connection mysqlConnection) {
        String sql = "select @@sql_mode";
        String originMySqlMode = null;
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, null, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            mysqlRs.next();
            originMySqlMode = mysqlRs.getString(1);
//            log.info("*********SQLMODE*********" + originMySqlMode);

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(mysqlRs);
        }
        return originMySqlMode;
    }

    public static String getTimeZone(Connection mysqlConnection) {
        String sql = "show variables like \"time_zone\"";
        return executeQueryAndGetStringResult(sql, mysqlConnection, 2);
    }

    public static void setTimeZone(Connection mysqlConnection, String timeZone) throws SQLException {
        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("SET time_zone = '" + timeZone + "'");
        }
    }

    /**
     * preparedStatement Set
     */
    public static PreparedStatement preparedStatementSet(String sql, List<Object> param, Connection conn) {
        return preparedStatementSet(sql, param, conn, false);
    }

    /**
     * preparedStatement Set
     */
    public static PreparedStatement preparedStatementSet(String sql, List<Object> param, Connection conn,
                                                         boolean isTddl) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            preparedStatementSet(preparedStatement, param, isTddl);

        } catch (SQLException e) {
            log.error("[Create preparedStatement] failed, begin to rollback , sql is " + sql, e);
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException e1) {
                String errorMs = "[Create preparedStatement] failed and [Transaction rollback] failed, sql is: " + sql;
                log.error(errorMs, e1);
                Assert.fail(errorMs + " \n " + e1);
            }
            Assert.fail("[Create preparedStatement] failed, sql is :" + sql);
        }

        return preparedStatement;
    }

    /**
     * preparedStatement Set
     */
    private static PreparedStatement preparedStatementSet(PreparedStatement preparedStatement, List<Object> param,
                                                          boolean isTddl) {
        if (param != null) {
            for (int i = 0; i < param.size(); i++) {
                try {
                    if (isTddl && param.get(i) == null) {
                        preparedStatement.setObject(i + 1, java.sql.Types.NULL);
                    } else {
                        preparedStatement.setObject(i + 1, param.get(i));
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    String errorMs = "[preparedStatement] failed ";
                    log.error(errorMs, ex);
                    Assert.fail(errorMs + " \n " + ex);
                }
            }
        }
        return preparedStatement;
    }

    /**
     * preparedStatement,Batch
     */
    public static PreparedStatement preparedStatementBatch(String sql, Connection conn) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql, 1);
        } catch (SQLException e) {
            log.error("[Create preparedStatement] failed, begin to rollback , sql is " + sql, e);
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException e1) {
                String errorMs = "[Create preparedStatement] failed and [Transaction rollback] failed, sql is: " + sql;
                log.error(errorMs, e1);
                Assert.fail(errorMs + " \n " + e);
            }
            Assert.fail("[Create preparedStatement] failed, sql is :" + sql);
        }

        return preparedStatement;
    }

    public static PreparedStatement preparedStatementAutoGen(String sql, List<Object> param, Connection conn,
                                                             String autoGenCol) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql, new String[] {autoGenCol});
            preparedStatementSet(preparedStatement, param, true);

        } catch (SQLException e) {
            log.error("[Create preparedStatement] failed, begin to rollback , sql is " + sql, e);
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException e1) {
                String errorMs = "[Create preparedStatement] failed and [Transaction rollback] failed, sql is: " + sql;
                log.error(errorMs, e1);
                Assert.fail(errorMs + " \n " + e1);
            }
            Assert.fail("[Create preparedStatement] failed, sql is :" + sql);
        }

        return preparedStatement;
    }

    /**
     * preparedStatement Set
     */
    private static PreparedStatement preparedStatementSet(PreparedStatement preparedStatement, List<Object> param) {
        return preparedStatementSet(preparedStatement, param, false);
    }

    /**
     * preparedStatement
     */
    public static PreparedStatement preparedStatement(String sql, Connection conn) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
        } catch (SQLException e) {
            log.error("[Create preparedStatement] failed, begin to rollback , sql is " + sql, e);
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException e1) {
                String errorMs = "[Create preparedStatement] failed and [Transaction rollback] failed, sql is: " + sql;
                log.error(errorMs, e1);
                Assert.fail(errorMs + " \n " + e);
            }
            Assert.fail("[Create preparedStatement] failed, sql is :" + sql);
        }

        return preparedStatement;
    }

    private static int[] preparedStatementExecuteBatch(PreparedStatement ps) throws SQLException {
        int[] effectCount = new int[10];
        effectCount = ps.executeBatch();
        return effectCount;
    }

    private static void preparedStatementAddBatch(PreparedStatement ps) {
        try {
            ps.addBatch();
        } catch (SQLException e) {
            String errorMs = "[preparedStatement add batch] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
    }

    public static List<String> getColumnNameList(ResultSet rs) {
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        List<String> cloumnNameList = new ArrayList<String>();

        for (int i = 1; i < columnCount + 1; i++) {
            String cloumnName = null;
            cloumnName = getColumnLabel(metaData, i);
            cloumnNameList.add(cloumnName.toLowerCase());
        }

        return cloumnNameList;
    }

    public static List<String> getColumnNameListToLowerCase(ResultSet rs) {
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        List<String> cloumnNameList = new ArrayList<String>();

        for (int i = 1; i < columnCount + 1; i++) {
            String cloumnName = null;
            cloumnName = getColumnLabel(metaData, i);
            cloumnNameList.add(cloumnName.toLowerCase());
        }

        return cloumnNameList;
    }

    public static String getColumnLabel(ResultSetMetaData metaData, int i) {
        String cloumnName = null;
        try {
            cloumnName = metaData.getColumnLabel(i);
        } catch (SQLException e) {
            String errorMs = "[metaData getColumnLabel] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return cloumnName;
    }

    public static String getColumnType(ResultSetMetaData metaData, int i) {
        String cloumnType = null;
        try {
            cloumnType = metaData.getColumnTypeName(i);
        } catch (SQLException e) {
            String errorMs = "[metaData getColumnType] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return cloumnType;

    }

    public static int getColumnCount(ResultSetMetaData metaData) {
        int columnLength = 0;
        try {
            columnLength = metaData.getColumnCount();
        } catch (SQLException e) {
            String errorMs = "[metaData getColumnCount] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return columnLength;
    }

    public static ResultSetMetaData getMetaData(ResultSet rs) {
        ResultSetMetaData metaData = null;
        if (rs != null) {
            try {
                metaData = rs.getMetaData();
            } catch (SQLException e) {
                String errorMs = "[getMetaData] failed";
                log.error(errorMs, e);
                Assert.fail(errorMs + " \n " + e);
            }
        }
        return metaData;
    }

    public static List<List<Object>> getAllResult(ResultSet rs) {
        return getAllResult(rs, false, null, true, isStrictType());
    }

    public static List<List<Object>> getAllResult(ResultSet rs, boolean ignoreException) {
        return getAllResult(rs, ignoreException, null, true, isStrictType());
    }

    public static List<String> getAllTypeName(ResultSet rs, boolean ignoreException) {
        List<String> typeNames = new ArrayList<>();
        ResultSetMetaData resultSetMetaData;
        try {
            resultSetMetaData = rs.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            for (int i = 0; i < colCount; i++) {
                typeNames.add(resultSetMetaData.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
        }
        return typeNames;
    }

    public static List<List<String>> getStringResult(ResultSet rs, boolean ignoreException) {
        return getAllStringResult(rs, ignoreException, null);
    }

    public static List<List<Object>> getAllResultIgnoreColumn(ResultSet rs, List<Integer> ignoreColumn) {
        return getAllResult(rs, false, ignoreColumn, true, false);
    }

    public static List<List<Object>> getAllResult(ResultSet rs, boolean ignoreException, List<Integer> ignoreColumn,
                                                  boolean assertDateTypeMatch, boolean strictType) {
        List<List<Object>> allResults = new ArrayList<List<Object>>();
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        try {
            while (rs.next()) {
                List<Object> oneResult = new ArrayList<Object>();
                for (int i = 1; i < columnCount + 1; i++) {
                    try {
                        if (ignoreColumn == null || ignoreColumn.size() == 0 || !ignoreColumn.contains(i)) {
                            if (metaData.getColumnTypeName(i).toLowerCase().contains("tinyint")) {
                                if (rs.getObject(i) == null) {
                                    oneResult.add(null);
                                } else {
                                    oneResult.add(dataConversion(rs.getInt(i), assertDateTypeMatch, strictType));
                                }
                            } else {
                                oneResult.add(getObject(rs, i, assertDateTypeMatch, strictType));
                            }
                        } else {
                            log.info("Ignore Column :" + metaData.getColumnName(i));
                        }

                        // }
                    } catch (Exception ex) {
                        if (ignoreException) {
                            oneResult.add("null");
                        } else {
                            oneResult.add(ex.getMessage());
                        }
                    }
                }
                allResults.add(oneResult);
            }
        } catch (SQLException e) {
            String errorMs = "[ResultSet next] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        return allResults;
    }

    public static List<List<String>> getAllStringResultWithColumnNames(ResultSet rs, boolean ignoreException,
                                                                       List<Integer> ignoreColumn) {
        List<List<String>> allResults = new ArrayList<>();
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        try {
            List<String> columnName = new ArrayList<>();
            for (int i = 1; i < columnCount + 1; i++) {
                if (ignoreColumn == null || ignoreColumn.size() == 0 ||
                    !ignoreColumn.contains(i)) {
                    columnName.add(metaData.getColumnName(i));
                }
            }
            allResults.add(columnName);

            while (rs.next()) {
                List<String> oneResult = new ArrayList<>();
                for (int i = 1; i < columnCount + 1; i++) {

                    try {
                        if (ignoreColumn == null || ignoreColumn.size() == 0 || !ignoreColumn.contains(i)) {
                            oneResult.add(rs.getString(i));
                        }
                    } catch (Exception ex) {
                        if (ignoreException) {
                            oneResult.add("null");
                        } else {
                            oneResult.add(ex.getMessage());
                        }
                    }
                }
                allResults.add(oneResult);
            }
        } catch (SQLException e) {
            String errorMs = "[ResultSet next] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        return allResults;
    }

    public static List<List<String>> getAllStringResult(ResultSet rs, boolean ignoreException,
                                                        List<Integer> ignoreColumn) {
        List<List<String>> allResults = new ArrayList<>();
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        try {
            while (rs.next()) {
                List<String> oneResult = new ArrayList<>();
                for (int i = 1; i < columnCount + 1; i++) {

                    try {
                        if (ignoreColumn == null || ignoreColumn.size() == 0 || !ignoreColumn.contains(i)) {
                            oneResult.add(rs.getString(i));
                        }
                    } catch (Exception ex) {
                        if (ignoreException) {
                            oneResult.add("null");
                        } else {
                            oneResult.add(ex.getMessage());
                        }
                    }
                }
                allResults.add(oneResult);
            }
        } catch (SQLException e) {
            String errorMs = "[ResultSet next] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        return allResults;
    }

    public static List<List<Object>> getAllLowerCaseStringResult(ResultSet rs) {
        List<List<Object>> allResults = new ArrayList<List<Object>>();
        ResultSetMetaData metaData = getMetaData(rs);
        int columnCount = getColumnCount(metaData);
        try {
            while (rs.next()) {
                List<Object> oneResult = new ArrayList<Object>();
                for (int i = 1; i < columnCount + 1; i++) {
                    try {
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("tinyint")) {
                            oneResult.add(dataConversion(rs.getInt(i)));
                        } else if (metaData.getColumnTypeName(i).equalsIgnoreCase("VARCHAR")) {
                            oneResult.add(dataConversion(rs.getString(i)).toString().toLowerCase());
                        } else if (metaData.getColumnTypeName(i).equalsIgnoreCase("CHAR")) {
                            oneResult.add(dataConversion(rs.getString(i)).toString().toLowerCase());
                        } else {
                            oneResult.add(getObject(rs, i));
                        }
                        // }
                    } catch (Exception ex) {
                        oneResult.add(ex.getMessage());
                    }
                }
                allResults.add(oneResult);
            }
        } catch (SQLException e) {
            String errorMs = "[ResultSet next] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        return allResults;
    }

    public static Object getObject(ResultSet rs, int i) throws SQLException {
        return getObject(rs, i, true);
    }

    public static Object getObject(ResultSet rs, int i, boolean assertDateTypeMatch) throws SQLException {
        Object data = null;
        try {
            data = rs.getObject(i);
        } catch (Exception e) {
            try {
                data = rs.getString(i);
            } catch (Exception e1) {
                data = rs.getBytes(i);
            }
        }
        data = dataConversion(data, assertDateTypeMatch, isStrictType());
        return data;
    }

    public static Object getObject(ResultSet rs, int i, boolean assertDateTypeMatch, boolean strictTypeMatch)
        throws SQLException {
        Object data = null;
        try {
            data = rs.getObject(i);
        } catch (Exception e) {
            try {
                data = rs.getString(i);
            } catch (Exception e1) {
                data = rs.getBytes(i);
            }
        }
        data = dataConversion(data, assertDateTypeMatch, strictTypeMatch);
        return data;
    }

    public static Object getObject(ResultSet rs, String columnName) throws SQLException {
        Object data = rs.getObject(columnName);
        data = dataConversion(data);
        return data;
    }

    private static Object dataConversion(Object data) {
        return dataConversion(data, true);
    }

    private static Object dataConversion(Object data, boolean assertDateTypeMatch) {
        return dataConversion(data, assertDateTypeMatch, false);
    }

    private static Object dataConversion(Object data, boolean assertDateTypeMatch, boolean strictTypeMatch) {
        if (strictTypeMatch) {
            if (data instanceof Double) {
                data = new BigDecimal((Double) data);
            }
            return data;
        }
        if (data instanceof Long) {
            data = new BigDecimal((Long) data);
        } else if (data instanceof Short) {
            data = new BigDecimal((Short) data);
        } else if (data instanceof Integer) {
            data = new BigDecimal((Integer) data);
        } else if (data instanceof Float) {
            data = new BigDecimal((Float) data);
        } else if (data instanceof Double) {
            data = new BigDecimal((Double) data);
        } else if (data instanceof BigDecimal) {
            // data = data;
        } else if (data instanceof Timestamp) {
            data = new MyDate((Date) data, assertDateTypeMatch);
        } else if (data instanceof Date) {
            // data = ((Date) data).getTime() / 1000;
            data = new MyDate((Date) data, assertDateTypeMatch);
        } else if (data instanceof byte[]) {
            data = GeneralUtil.printBytes((byte[]) data);
        } else if (data instanceof BigInteger) {
            data = new BigDecimal((BigInteger) data);
        }

        if (data instanceof BigDecimal) {
            data = new MyNumber((BigDecimal) data);
        }
        return data;
    }

    public static List<List<Object>> loadDataFromPhysical(final Connection tddlConnection, String tableName,
                                                          Function<Pair<String, String>, String> sqlBuilder) {
        final List<Pair<String, String>> physicalTables = getTopology(tddlConnection, tableName);

        List<List<Object>> result = new ArrayList<>();
        physicalTables.forEach(physical -> {
            final ResultSet resultSet = executeQuery(sqlBuilder.apply(physical), tddlConnection);
            result.addAll(getAllResult(resultSet, false, null, false, false));
        });

        return result;
    }

    public static List<Pair<String, String>> getTopology(Connection tddlConnection, String tableName) {
        final List<Pair<String, String>> physicalTables = new ArrayList<>();
        try (ResultSet topology = executeQuery("SHOW TOPOLOGY FROM `" + tableName + "`", tddlConnection)) {
            while (topology.next()) {
                physicalTables.add(Pair.of(topology.getString(2), topology.getString(3)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get topology for " + tableName, e);
        }
        return physicalTables;
    }

    public static List<Pair<String, String>> getTopologyWithHint(Connection tddlConnection, String tableName,
                                                                 String hint) {
        final List<Pair<String, String>> physicalTables = new ArrayList<>();
        try (ResultSet topology = executeQuery(hint + "SHOW TOPOLOGY FROM `" + tableName + "`", tddlConnection)) {
            while (topology.next()) {
                physicalTables.add(Pair.of(topology.getString(2), topology.getString(3)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get topology for " + tableName, e);
        }
        return physicalTables;
    }

    /**
     * 获取三节点的LeaderIP
     */
    public static String getLeaderIP(Connection conn) {
        log.info("获取Leader IP");
        if (conn != null) {
            ResultSet resultSet =
                JdbcUtil.executeQuerySuccess(conn, "SELECT * FROM information_schema.ALISQL_CLUSTER_GLOBAL");
            try {
                while (resultSet.next()) {
                    if (resultSet.getString("ROLE").equalsIgnoreCase("Leader")) {
                        return resultSet.getString("IP_PORT");
                    }

                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);

            } finally {
                JdbcUtil.close(resultSet);
            }

        }
        log.error("获取不到Leader IP，请检查环境是否正常");
        return null;
    }

    public static void setTxPolicy(ITransactionPolicy trxPolicy, Connection conn) throws SQLException {
        if (conn instanceof TConnection) {
            ((TConnection) conn).setTrxPolicy(trxPolicy, true);
        } else {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set drds_transaction_policy = '" + trxPolicy.toString() + "'");
            }
        }
    }

    public static void setShareReadView(boolean shareReadView, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("set share_read_view = %s", shareReadView ? "ON" : "OFF"));
        }
    }

    public static boolean supportShareReadView(Connection conn) throws SQLException {
        if (isShareReadView(conn)) {
            return true;
        }
        try {
            conn.setAutoCommit(false);
            // 尝试开启来确认DN是否支持共享ReadView
            setShareReadView(true, conn);
            return true;
        } catch (SQLException e) {
            return false;
        } finally {
            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    public static boolean isShareReadView(Connection conn) {
        final ResultSet rs = executeQuerySuccess(conn, "show variables like 'share_read_view'");
        try {
            Assert.assertTrue(rs.next());
            boolean res = rs.getBoolean(2);
            Assert.assertFalse(rs.next());
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean supportXA(Connection tddlConnection) throws SQLException {
        final ResultSet rs = executeQuerySuccess(tddlConnection, "SELECT @@version");
        rs.next();
        String version = rs.getString(1);

        return !version.startsWith("5.6") && !version.startsWith("5.5");
    }

    public static void assertRouteCorrectness(String hint, String tableName, List<List<Object>> selectedData,
                                              List<String> columnNames, List<String> shardingKeys,
                                              Connection connection) throws SQLException {
        List<Integer> shardingColumnIndexes = shardingKeys.stream()
            .map(columnNames::indexOf)
            .collect(Collectors.toList());
        List<List<Object>> shardingValues = new ArrayList<>(selectedData.size());
        for (List<Object> row : selectedData) {
            List<Object> shardingValuesInRow = new ArrayList<>(shardingColumnIndexes.size());
            for (int index : shardingColumnIndexes) {
                Object value = row.get(index);
                if (value instanceof MyDate) {
                    value = ((MyDate) value).getDate();
                } else if (value instanceof MyNumber) {
                    value = ((MyNumber) value).getNumber();
                }
                shardingValuesInRow.add(value);
            }
            shardingValues.add(shardingValuesInRow);
        }

        for (List<Object> row : shardingValues) {
            StringBuilder sql = new StringBuilder(
                String.format(Optional.ofNullable(hint).orElse("") + "select * from %s where ", tableName));
            final List<Object> values = new ArrayList<>();
            for (int i = 0; i < shardingKeys.size(); i++) {
                String shardingKeyName = shardingKeys.get(i);
                final Object value = row.get(i);
                if (null == value) {
                    sql.append(shardingKeyName).append(" is null");
                } else {
                    sql.append(shardingKeyName).append("=?");
                    values.add(value);
                }
                if (i != shardingKeys.size() - 1) {
                    sql.append(" and ");
                }
            }
            PreparedStatement tddlPs = preparedStatementSet(sql.toString(), values, connection);
            ResultSet tddlRs = executeQuery(sql.toString(), tddlPs);
            Assert.assertTrue(tddlRs.next());
        }
    }

    public static List<String> showDatabases(Connection conn) {
        List<String> dbs = new ArrayList<>();
        ResultSet resultSet = executeQuery("show databases", conn);
        try {
            while (resultSet.next()) {
                String db = resultSet.getString(1);
                dbs.add(db.toLowerCase());
            }
        } catch (Throwable e) {
            com.alibaba.polardbx.common.utils.Assert.fail();
        }

        com.alibaba.polardbx.common.utils.Assert.assertTrue(dbs.contains("information_schema"));
        dbs.remove("information_schema");
        if (dbs.contains("polardbx_performance_schema")) {
            dbs.remove("polardbx_performance_schema");
        }
        return dbs;
    }

    public static boolean dbExists(Connection conn, String db) {
        List<String> dbs = showDatabases(conn);
        return dbs.contains(db.toLowerCase());
    }

    public static class MyNumber {

        BigDecimal number;

        public MyNumber(BigDecimal number) {
            super();
            this.number = number;
        }

        public BigDecimal getNumber() {
            return number;
        }

        public String toString() {
            return this.number == null ? null : this.number.toString();
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                if (obj instanceof String) {
                    return toString().equals(obj);
                }
                String errorMS = "[类型不一致]" + this.getClass() + "  " + obj.getClass();
                log.error(errorMS);
                Assert.fail(errorMS);
            }
            MyNumber other = (MyNumber) obj;
            if (number == null) {
                return other.number == null;
            } else {
                BigDecimal o = this.number;
                BigDecimal o2 = other.number;

                return o.subtract(o2).abs().compareTo(new BigDecimal(0.0001)) < 0;
            }
        }
    }

    // 时间类型的比较不需要特别准确，
    public static class MyDate {

        public Date date;
        public final boolean assertTypeMatch;

        public MyDate(Date date) {
            this(date, true);
        }

        public MyDate(Date date, boolean assertTypeMatch) {
            super();
            this.date = date;
            this.assertTypeMatch = assertTypeMatch;
        }

        public Date getDate() {
            return date;
        }

        public String toString() {
            return this.date == null ? null : String.valueOf(this.date.getTime());
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                if (obj instanceof String) {
                    return toString().equals(obj);
                }
                String errorMS = "[类型不一致]" + this.getClass() + "  " + obj.getClass();
                log.error(errorMS);
                if (assertTypeMatch) {
                    Assert.fail(errorMS);
                } else {
                    return false;
                }
            }
            MyDate other = (MyDate) obj;
            if (date == null) {
                if (other.date != null) {
                    return false;
                }
            } else {
                Date o = this.date;
                Date o2 = other.date;

                return ((o.getTime() - o2.getTime()) / 1000 >= -10) && ((o.getTime() - o2.getTime()) / 1000 <= 10);
            }
            return false;
        }
    }

    /**
     * 关闭连接
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 本应该放到最基础类里的方法,但由于框架要整体更换,写在这区分一下
     */
    public static void executeUpdateSuccess(Connection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
//            log.info("Executing update: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            assertWithMessage("语句并未按照预期执行成功:" + sql + e.getMessage()).fail();
        } finally {
            close(stmt);
        }

    }

    public static void executeUpdateSuccessInTrx(Connection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("begin");
            stmt.execute(sql);
            stmt.execute("commit");
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            assertWithMessage("语句并未按照预期执行成功:" + sql + e.getMessage()).fail();
        } finally {
            try {
                stmt.execute("rollback");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            close(stmt);
        }

    }

    /**
     * 执行 update, 忽略指定异常
     *
     * @param errIgnored 忽略包含此信息的异常
     * @return 是否有异常被忽略
     */
    public static boolean executeUpdateSuccessIgnoreErr(Connection conn, String sql, Set<String> errIgnored) {
        boolean result = false;

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);

            for (String err : errIgnored) {
                if (e.getMessage().contains(err)) {
                    result = true;
                    break;
                }
            }

            assertWithMessage(sql + "\n语句并未按照预期执行成功: " + e.getMessage()).that(result).isTrue();
        } finally {
            close(stmt);
        }

        return result;
    }

    public static void executeUpdateSuccessWithWarning(Connection conn, String sql, String expectedWarning) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            assertWithMessage("语句并未按照预期执行成功:" + e.getMessage()).fail();
        }
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("show warnings")) {
            String warning = "";
            if (rs.next()) {
                warning = rs.getString("Message");
            }
            Assert.assertTrue(warning.toLowerCase().contains(expectedWarning.toLowerCase()));
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            assertWithMessage("语句并未按照预期执行成功:" + e.getMessage()).fail();
        }
    }

    public static void executeUpdate(Connection conn, String sql) {
        executeUpdate(conn, sql, false, false);
    }

    public static void executeUpdate(Connection conn, String sql, boolean liteLog, boolean ignoreError) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            if (liteLog) {
                log.error(e.getMessage());
            } else {
                log.error(e.getMessage(), e);
            }
            if (!ignoreError) {
                Assert.fail(e.getMessage());
            }
        } finally {
            close(stmt);
        }

    }

    public static int executeUpdateAndGetEffectCount(Connection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            return stmt.executeUpdate(sql);
        } catch (Exception e) {
            log.error(e.getMessage() + ":\n " + sql, e);
        } finally {
            close(stmt);
        }

        return -1;

    }

    /**
     * 本应该放到最基础类里的方法,但由于框架要整体更换,写在这区分一下
     */
    public static void executeUpdateFailed(Connection conn, String sql, String errMessage, String errMessage2) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            assertWithMessage("语句并未按照预期执行失败,sql is :" + sql).fail();
        } catch (Exception e) {
            Throwable t = e;
            if (e.getMessage().equals(READ_ONLY_EXCEPTION_MSG)) {
                t = e.getCause();
            }
            log.error(t.getMessage(), t);
            final ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder().add(errMessage.toLowerCase());
            if (TStringUtil.isNotBlank(errMessage2)) {
                builder.add(errMessage2.toLowerCase());
            }
            final ImmutableSet<String> errMsgSet = builder.build();
            Assert.assertTrue(t.getMessage(), errMsgSet.stream().anyMatch(t.getMessage().toLowerCase()::contains));
        } finally {
            close(stmt);
        }
    }

    public static void executeUpdateFailed(Connection conn, String sql, String errMessage) {
        executeUpdateFailed(conn, sql, errMessage, null);
    }

    /**
     * 执行错误，返回错误信息
     */
    public static String executeUpdateFailedReturn(Connection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            assertWithMessage("语句并未按照预期执行失败,sql is :" + sql).fail();
        } catch (Exception e) {
            return e.getMessage();
        } finally {
            close(stmt);
        }
        return "";
    }

    public static void executeUpdateWithException(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
    }

    /**
     *
     */
    public static void havePrivToExecute(Connection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            if (e.getMessage().contains("Doesn't have")) {
                assertWithMessage(String.format("没有权限执行语句%s", sql)).fail();
            }
            if (e.getMessage().contains("You have an error in your SQL syntax")) {
                assertWithMessage(String.format("语法错误%s", sql)).fail();
            }
        } finally {
            close(stmt);
        }

    }

    /**
     * 查询语句执行成功
     */
    public static ResultSet executeQuerySuccess(Connection conn, String sql) {
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            assertWithMessage("语句并未按照预期执行成功:" + e.getMessage()).fail();
        }
        return resultSet;
    }

    /**
     * 查询语句执行失败
     */
    public static void executeQueryFaied(Connection conn, String sql, String[] errorMsg) {
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sql);
            assertWithMessage("语句并未按照预期执行失败").fail();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            String message = e.getMessage().toLowerCase();
            boolean contains = false;
            for (String s : errorMsg) {
                if (TStringUtil.isNotBlank(s)) {
                    contains = message.contains(s.toLowerCase());
                    if (contains) {
                        break;
                    }
                }
            }
            if (!contains) {
                // Print error message
                Truth.assertThat(errorMsg).asList().contains(message);
            }
        } finally {
            close(resultSet);
            close(stmt);
        }
    }

    /**
     * 普通语句执行失败
     */
    public static void executeFailed(Connection conn, String sql, String[] errorMsg) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            assertWithMessage("语句并未按照预期执行失败").fail();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            String message = e.getMessage().toLowerCase();
            boolean contains = false;
            for (String s : errorMsg) {
                if (TStringUtil.isNotBlank(s)) {
                    contains = message.contains(s.toLowerCase());
                    if (contains) {
                        break;
                    }
                }
            }
            Assert.assertTrue(contains);
        } finally {
            close(stmt);
        }
    }

    public static void executeQueryFaied(Connection conn, String sql, String errorMsg) {
        executeQueryFaied(conn, sql, new String[] {errorMsg});
    }

    public static void executeFailed(Connection conn, String sql, String errorMsg) {
        executeFailed(conn, sql, new String[] {errorMsg});
    }

    public static DruidDataSource getDruidDataSource(String url, String user, String password) {
        DruidDataSource druidDs = new DruidDataSource();
        druidDs.setUrl(url);
        druidDs.setUsername(user);
        druidDs.setPassword(password);
        druidDs.setRemoveAbandoned(false);
        druidDs.setMaxActive(30);
        // //druidDs.setRemoveAbandonedTimeout(18000);
        try {
            druidDs.init();
            druidDs.getConnection();
        } catch (SQLException e) {
            String errorMs = "[DruidDataSource getConnection] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs);
        }
        return druidDs;
    }

    /**
     * 返回结果集的条数
     */
    public static int resultsSize(ResultSet rs) {
        int row = 0;
        try {
            while (rs.next()) {
                row++;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return row;

    }

    /**
     * 选择出自增列
     */
    public static List<Long> selectIds(String sql, String columnName, Connection conn) {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, sql);
            List<Long> resultId = new ArrayList<Long>();
            while (rs.next()) {
                resultId.add(rs.getLong(columnName));
            }
            return resultId;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }

        return null;

    }

    /**
     * 判断select出的Id是否一致
     */
    public static List<Double> selectDoubleIds(String sql, String columnName, Connection conn) throws Exception {
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, sql);
            List<Double> resultId = new ArrayList<Double>();
            while (rs.next()) {
                resultId.add(rs.getDouble(columnName));
            }

            return resultId;
        } finally {
            if (rs != null) {
                JdbcUtil.close(rs);
            }

        }

    }

    public static String resultsStr(ResultSet rs) {
        StringBuilder sb = new StringBuilder();
        try {
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return sb.toString();

    }

    public static Long resultLong(ResultSet rs) {
        Long ret = null;
        try {
            while (rs.next()) {
                ret = (Long) rs.getObject(1);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return ret;
    }

    public static String showFullCreateTable(Connection conn, String tbName) throws Exception {
        String sql = "show full create table `" + tbName + "`";

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getString("Create Table");
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public static String showCreateTable(Connection conn, String tbName) throws Exception {
        String sql = "show create table `" + tbName + "`";

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getString(2);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public static List<String> showTables(Connection conn) throws Exception {
        List<String> tables = new ArrayList<>();
        ResultSet resultSet = JdbcUtil.executeQuery("show tables", conn);
        try {
            while (resultSet.next()) {
                String table = resultSet.getString(1);
                tables.add(table);
            }
        } finally {
            JdbcUtil.close(resultSet);
        }

        return tables;
    }

    public static String selectVersion(Connection conn) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select version()");
        try {
            while (rs.next()) {
                return rs.getString(1);
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }
        return null;
    }

    public static void useDb(Connection connection, String db) {
        JdbcUtil.executeQuery("use " + db, connection);
    }

    public static String createLikeTable(
        String tableName, String polardbxOneDB, String mysqlOneDB, String suffixName, boolean bMysql)
        throws SQLException {
        String originTableName = tableName;
        String tempTableName = originTableName + suffixName;

        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            useDb(tddlConnection, polardbxOneDB);
            JdbcUtil.executeUpdate(tddlConnection, String.format(DROP_TABLE_SQL, tempTableName), false, false);
            JdbcUtil.executeUpdate(
                tddlConnection, String.format(CREATE_TABLE_LIKE_SQL, tempTableName, originTableName), false, false);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            if (bMysql) {
                useDb(mysqlConnection, mysqlOneDB);
                JdbcUtil.executeUpdate(mysqlConnection, String.format(DROP_TABLE_SQL, tempTableName), false, false);
                JdbcUtil.executeUpdate(
                    mysqlConnection, String.format(CREATE_TABLE_LIKE_SQL, tempTableName, originTableName), false,
                    false);
            }
        }
        return tempTableName;
    }

    public static void dropTable(
        String tableName, String polardbxOneDB, String mysqlOneDB, String suffixName, boolean bMysql)
        throws SQLException {
        String originTableName = tableName;
        String tempTableName = originTableName + suffixName;
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            useDb(tddlConnection, polardbxOneDB);
            JdbcUtil.executeUpdate(tddlConnection, String.format(DROP_TABLE_SQL, tempTableName), false, false);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            if (bMysql) {
                useDb(mysqlConnection, mysqlOneDB);
                JdbcUtil.executeUpdate(mysqlConnection, String.format(DROP_TABLE_SQL, tempTableName), false, false);
            }
        }
    }

    public static void createPartDatabase(Connection polarxConn, String logDb) {
        String createDbSql =
            String.format(
                "/*+TDDL:AUTO_PARTITION_PARTITIONS=3*/create database if not exists %s charset utf8mb4 collate utf8mb4_general_ci mode='auto';",
                logDb);
        JdbcUtil.executeUpdate(polarxConn, createDbSql);
        useDb(polarxConn, logDb);
    }

    public static void createDatabase(Connection polarxConn, String logDb, String hint) {
        String createDbSql = String.format("%s create database if not exists %s ;", hint, logDb);
        JdbcUtil.executeUpdate(polarxConn, createDbSql);
        useDb(polarxConn, logDb);
    }

    public static void dropDatabase(Connection polarxConn, String logDb) {
        String dropDbSql = "drop database if exists " + logDb;
        JdbcUtil.executeUpdate(polarxConn, dropDbSql);
    }

    public static void dropDatabaseWithRetry(Connection polarxConn, String logDb, int retryNum) {
        String dropDbSql = "drop database if exists " + logDb;
        while (retryNum > 0 && dbExists(polarxConn, logDb)) {
            retryNum--;
            JdbcUtil.executeUpdate(polarxConn, dropDbSql, true, true);
        }
    }

    public static void dropTable(Connection polarxConn, String table) {
        String dropTableSql = "drop table if exists " + table;
        JdbcUtil.executeUpdate(polarxConn, dropTableSql);
    }

    public static void analyzeTable(Connection tddlConnection, String tableName) {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("analyze table " + tableName);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static int getDriverMajorVersion(Connection tddlConnection) {
        try {
            String driverVersion = tddlConnection.getMetaData().getDriverVersion();
            Pattern compile = Pattern.compile("\\d\\.\\d\\.\\d+");
            Matcher matcher = compile.matcher(driverVersion);
            if (!matcher.find()) {
                Assert.fail("unrecognized driver version: " + driverVersion);
            }
            String version = matcher.group();
            return Integer.parseInt(version.substring(0, 1));
        } catch (Exception e) {
            log.error("Failed to get driver version");
            Assert.fail(e.getMessage());
        }
        return -1;
    }

    public static void createUser(Connection tddlConnection, String username, String host, String password) {
        String sql = String.format("create user %s@'%s' identified by '%s'", username, host, password);
        executeUpdateSuccess(tddlConnection, sql);
    }

    public static void grantAllPrivOnDb(Connection tddlConnection, String username, String host, String db) {
        String sql = String.format("grant all on %s.* to '%s'@'%s';", db, username, host);
        executeUpdateSuccess(tddlConnection, sql);
    }

    public static void dropUser(Connection tddlConnection, String username, String host) {
        String sql = String.format("drop user if exists %s@'%s'", username, host);
        executeUpdateSuccess(tddlConnection, sql);
    }

    public static String getInsertAllTypePrepareSql(String tableName, List<ColumnEntity> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
        StringBuilder values = new StringBuilder(" VALUES ( ");

        for (ColumnEntity column : columns) {
            String columnName = column.getName();
            sql.append(columnName).append(",");
            values.append(" ?,");
        }
        sql.setLength(sql.length() - 1);
        values.setLength(values.length() - 1);
        sql.append(") ");
        values.append(") ");
        sql.append(values);
        return sql.toString();
    }

    public static String getExplainResult(Connection tddlConnection, String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql);
        try {
            return JdbcUtil.resultsStr(rs);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public static List<String> getTableColumns(Connection conn, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        ResultSet resultSet = JdbcUtil.executeQuery("desc " + tableName, conn);
        try {
            while (resultSet.next()) {
                String column = resultSet.getString(1);
                columns.add(column);
            }
        } finally {
            JdbcUtil.close(resultSet);
        }

        return columns;
    }

    public static void createColumnarIndex(Connection tddlConnection, String indexName, String tableName,
                                           String sortKey, String partKey, int partCount) {
        final String createColumnarIdx = "create clustered columnar index %s on %s(%s) "
            + " engine='EXTERNAL_DISK' partition by hash(%s) partitions %s";
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(createColumnarIdx, indexName, tableName, sortKey, partKey, partCount));
    }

    public static void waitUntilVariableChanged(Connection conn, String varKey, String expectedVal, int timeoutSec)
        throws SQLException, InterruptedException {
        long time = 0;
        while (true) {
            ResultSet rs = executeQuerySuccess(conn,
                String.format("show variables like '%s'", varKey));
            if (rs.next() && rs.getString("Value").equalsIgnoreCase(expectedVal)) {
                break;
            }
            if (time++ > timeoutSec * 10L) {
                throw new RuntimeException("Timeout.");
            }
            Thread.sleep(100);
        }
    }

    public static void executeIgnoreErrors(Connection conn, String sql) {
        try (Statement statement = createStatement(conn)) {
            statement.execute(sql);
        } catch (SQLException e) {
            String errorMs = "[Statement execute] failed:" + sql;
            log.error(errorMs, e);
        }
    }

    public static void createPartDatabaseUsingUtf8(Connection polarxConn, String logDb) {
        String createDbSql =
            String.format(
                "/*+TDDL:AUTO_PARTITION_PARTITIONS=3*/create database if not exists %s mode='auto' DEFAULT CHARSET = utf8;",
                logDb);
        JdbcUtil.executeUpdate(polarxConn, createDbSql);
        useDb(polarxConn, logDb);
    }

}
