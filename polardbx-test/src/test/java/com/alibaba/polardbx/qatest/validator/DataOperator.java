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

package com.alibaba.polardbx.qatest.validator;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.hamcrest.CoreMatchers.is;

/**
 * 数据操作类 1.数据执行异常校验 2.mysql和tddl数据同时执行数据操作 3.批量在mysql和tddl数据同时执行数据操作
 *
 * @author zhuoxue.yll 2016年12月12日
 */
public class DataOperator {

    private static final Log log = LogFactory.getLog(DataOperator.class);

    /**
     * 数据执行异常校验
     */
    public static void executeErrorAssert(Connection tddlConnection, String sql, List<Object> param,
                                          String expectErrorMess) {
        PreparedStatement preparedStatement = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        try {
            preparedStatement.execute();
            Assert.fail(sql);
        } catch (Exception e) {
            Throwable t = e;
            if (e.getMessage().equals(JdbcUtil.READ_ONLY_EXCEPTION_MSG)) {
                t = e.getCause();
            }
            try {
                assertThat(t.getMessage()).contains(expectErrorMess);
            } catch (Throwable e2) {
                e2.printStackTrace();
                // 统一大小写，再检查一次
                assertThat(t.getMessage().toUpperCase()).contains(expectErrorMess.toUpperCase());
            }
        } finally {
            JdbcUtil.close(preparedStatement);
        }

    }

    /**
     * 数据执行异常校验
     */
    public static void executeErrorAssert(Connection tddlConnection, String sql, List<Object> param,
                                          String... expectErrors) {
        PreparedStatement preparedStatement = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        try {
            preparedStatement.execute();
            Assert.fail(sql);
        } catch (Exception e) {
            // log.error(e.getMessage(), e);
            final Set<String> errorSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            errorSet.addAll(ImmutableList.copyOf(expectErrors));

            final boolean asExpected =
                errorSet.stream().anyMatch(s -> e.getMessage().toLowerCase().contains(s.toLowerCase()));

            Assert.assertThat("Unexpected error: " + e.getMessage(), asExpected, is(true));
        } finally {
            JdbcUtil.close(preparedStatement);
        }

    }

    /**
     * mysql和tddl数据同时执行数据操作，不校验执行返回条数是否一致
     */
    public static void executeOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection, String sql,
                                             List<Object> param) {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, false);
    }

    /**
     * mysql和tddl数据同时执行set操作，不校验执行返回条数是否一致
     */
    public static void setOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection, String sql) {
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    /**
     * mysql和tddl数据同时执行数据操作 isAssertCount 为是否校验返回条数是否一致
     */
    public static void executeOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection, String sql,
                                             List<Object> param, boolean isAssertCount) {
        String sqlForMysql = (sql.replaceAll(PropertiesUtil.polardbXDBName1(false), PropertiesUtil.mysqlDBName1()))
            .replaceAll(PropertiesUtil.polardbXDBName2(false), PropertiesUtil.mysqlDBName2());

        sqlForMysql = (sqlForMysql.replaceAll(PropertiesUtil.polardbXDBName1(true), PropertiesUtil.mysqlDBName1()))
            .replaceAll(PropertiesUtil.polardbXDBName2(true), PropertiesUtil.mysqlDBName2());
        int mysqlEffectCount = JdbcUtil.updateData(mysqlConnection, sqlForMysql, param);
        int tddlEffectCount = JdbcUtil.updateData(tddlConnection, sql, param);
        if (isAssertCount) {
            Assert.assertEquals("sql 为 " + sql + " 更新条数结果不一致", mysqlEffectCount, tddlEffectCount);
        }

    }

    /**
     * mysql和tddl数据同时执行数据操作 isAssertCount 为是否校验返回条数是否一致
     */
    public static void executeOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection,
                                             String mysqlSql, String tddlSql,
                                             List<Object> param, boolean isAssertCount) {
        int mysqlEffectCount = JdbcUtil.updateData(mysqlConnection, mysqlSql, param);
        // int tddlEffectCount = JdbcUtil.updateDataTddl(tddlConnection, sql,
        int tddlEffectCount = JdbcUtil.updateData(tddlConnection, tddlSql, param);
        if (isAssertCount) {
            Assert.assertEquals("sql 为 " + tddlSql + " 更新条数结果不一致", mysqlEffectCount, tddlEffectCount);
        }

    }

    private static int updateDataGetEffectCount(Connection conn, String sql, List<Object> params) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
        }
        ps.executeUpdate();
        return ps.getUpdateCount();
    }

    public static void executeOnMysqlAndTddlAssertErrorAtomic(Connection mysqlConnection, Connection tddlConnection,
                                                              String mysqlSql, String tddlSql, List<Object> param,
                                                              boolean isAssertCount) {
        int mysqlEffectCount = -1;
        boolean mysqlErr = false;
        boolean tddlErr = false;
        try {
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "begin");
            mysqlEffectCount = updateDataGetEffectCount(mysqlConnection, mysqlSql, param);
        } catch (Throwable e) {
            // System.out.println(e);
            mysqlErr = true;
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "rollback");
        }

        if (!mysqlErr) {
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "commit");
        }

        int tddlEffectCount = -1;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
            tddlEffectCount = updateDataGetEffectCount(tddlConnection, tddlSql, param);
        } catch (Throwable e) {
            // System.out.println(e);
            tddlErr = true;
            JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback");
        }

        if (!tddlErr) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");
        }

        Assert.assertEquals("sql 为 " + tddlSql + " 执行结果不一致", mysqlErr, tddlErr);

        if (isAssertCount) {
            Assert.assertEquals("sql 为 " + tddlSql + " 更新条数结果不一致", mysqlEffectCount, tddlEffectCount);
        }

    }

    /**
     * 批量在mysql和tddl数据同时执行数据操作，不校验执行返回条数是否一致
     */
    public static void executeMutilValueOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection,
                                                       String sql,
                                                       List<List<Object>> params) {
        executeMutilValueOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params, false);
    }

    /**
     * 批量在mysql和tddl数据同时执行数据操作，不校验执行返回条数是否一致
     */
    public static void executeBatchOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection, String sql,
                                                  List<List<Object>> params) {
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params, false);
    }

    public static void executeBatchOnConn(Connection connection, String sql,
                                          List<List<Object>> params) {
        try {
            JdbcUtil.updateDataBatch(connection, sql, params);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("sql: " + sql + ", params: " + outputParams(params));
            throw new RuntimeException(e);
        }
    }

    private static String outputParams(List<List<Object>> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < params.size(); i++) {
            List<Object> param = params.get(i);
            sb.append(outputParam(param));
            if (i != param.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    private static String outputParam(List<Object> param) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < param.size(); i++) {
            Object item = param.get(i);
            sb.append(item.toString());

            if (i != param.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * mysql和tddl数据同时执行 isAssertCount 为是否校验返回条数是否一致
     */
    public static void executeBatchOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection, String sql,
                                                  List<List<Object>> params, boolean isAssertCount) {
        int[] mysqlEffectCount = null;
        int[] tddlEffectCount = null;
        try {
            mysqlEffectCount = JdbcUtil.updateDataBatch(mysqlConnection, sql, params);
            tddlEffectCount = JdbcUtil.updateDataBatch(tddlConnection, sql, params);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("sql: " + sql + ", params: " + outputParams(params));
            throw new RuntimeException(e);
        }

        if (isAssertCount) {
            assertWithMessage(" 顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql).that(mysqlEffectCount)
                .isEqualTo(tddlEffectCount);
        }

    }

    /**
     * mysql和tddl数据同时执行 isAssertCount 为是否校验返回条数是否一致
     */
    public static void executeMutilValueOnMysqlAndTddl(Connection mysqlConnection, Connection tddlConnection,
                                                       String sql,
                                                       List<List<Object>> params, boolean isAssertCount) {
        int mysqlEffectCount = -1;
        int tddlEffectCount = -1;
        StringBuffer sqlBuffer = new StringBuffer(sql);
        if (params != null && params.size() != 0) {
            for (List<Object> param : params) {
                if (param != null && param.size() != 0) {
                    sqlBuffer.append("(");
                    for (int i = 0; i < param.size(); i++) {
                        try {
                            Object paramSingle = param.get(i);
                            if (paramSingle == null) {
                                sqlBuffer.append("NULL");

                            } else if ((paramSingle instanceof char[]) || (paramSingle instanceof Byte)
                                || (paramSingle instanceof String)) {
                                sqlBuffer.append("'").append(paramSingle).append("'");
                            } else {
                                sqlBuffer.append(paramSingle);
                            }

                            sqlBuffer.append(",");
                        } catch (Exception ex) {
                            String errorMs = "[preparedStatement] failed ";
                            log.error(errorMs, ex);
                            Assert.fail(errorMs + " \n " + ex);
                        }
                    }
                    sqlBuffer.deleteCharAt(sqlBuffer.length() - 1);
                    sqlBuffer.append("),");
                }
            }
            sqlBuffer.deleteCharAt(sqlBuffer.length() - 1);
        }
        String execSql = sqlBuffer.toString();
        try {
            mysqlEffectCount = JdbcUtil.executeUpdateAndGetEffectCount(mysqlConnection, execSql);
            tddlEffectCount = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, execSql);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("sql: " + sql + ", params: " + outputParams(params));
            throw new RuntimeException(e);
        }

        if (isAssertCount) {
            assertWithMessage(" 顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + execSql).that(mysqlEffectCount)
                .isEqualTo(tddlEffectCount);
        }
    }

    public static String getShowCreateTableString(Connection conn, String tableName) {
        String sql = "show create table " + tableName;
        Statement sm = JdbcUtil.createStatement(conn);
        ResultSet rs = JdbcUtil.executeQuery(sql, sm);
        String retString = null;
        try {
            while (rs.next()) {
                retString = (String) JdbcUtil.getObject(rs, 2);
            }
        } catch (SQLException e) {
            Assert.fail("failed " + sql);
        }
        return retString;
    }

    public static void executeOnMysqlOrTddl(Connection tddlConnection, String sql, List<Object> param) {
        JdbcUtil.updateData(tddlConnection, sql, param);
    }

    public static void queryOnMysqlOrTddl(Connection conn, String sql) {
        JdbcUtil.executeQuery(sql, conn);
    }
}
