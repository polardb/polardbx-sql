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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Iterators;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * 查询数据正确校验类 包含一下功能 1.查询异常校验 2.查询结果顺序一致 3.查询结果非顺序一致
 */
public class DataValidator {

    private static final Log log = LogFactory.getLog(DataValidator.class);

    /**
     * 查询异常校验
     */
    public static void selectErrorAssert(String sql, List<Object> param, Connection tddlConnection,
                                         String expectErrorMess) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        try {
            ResultSet rs = tddlPs.executeQuery();
            rs.next();
            Assert.fail("sql 应该执行失败：" + sql);
        } catch (Exception e) {
            // e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
        Assert.assertTrue("expect error is " + expectErrorMess + "\n but actual is " + errorMess,
            errorMess.contains(expectErrorMess));

    }

    public static void explainResultMatchAssert(String sql, List<Object> param, Connection tddlConnection,
                                                String expectPattern) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        String actualExplainResult = "";
        try {
            ResultSet rs = tddlPs.executeQuery();
            rs.next();
            actualExplainResult = rs.getString(1);

        } catch (Exception e) {
            e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
        Assert.assertTrue("expect pattern is " + expectPattern + "\n but actual is " + actualExplainResult,
            actualExplainResult.matches(expectPattern));

    }

    public static void explainAllResultNotMatchAssert(String sql, List<Object> param, Connection tddlConnection,
                                                      String expectPattern) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        String actualExplainResult = "";
        try {
            ResultSet rs = tddlPs.executeQuery();
            while (rs.next()) {
                actualExplainResult = actualExplainResult + "\n" + rs.getString(1);

            }

        } catch (Exception e) {
            e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
        System.out.println(actualExplainResult);
        Assert.assertTrue("expect not exists pattern is " + expectPattern + "\n but actual is " + actualExplainResult,
            !actualExplainResult.matches(expectPattern));

    }

    public static void traceAllResultMatchAssert(String sql, List<Object> param, Connection tddlConnection,
                                                 Predicate<String> checker) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        String actualExplainResult = "";
        try {
            ResultSet rs = tddlPs.executeQuery();
            while (rs.next()) {
                actualExplainResult = actualExplainResult + "\n" + rs.getString("STATEMENT");

            }

        } catch (Exception e) {
            e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
        System.out.println(actualExplainResult);
        Assert.assertTrue("trace result: " + actualExplainResult, checker.test(actualExplainResult));

    }

    public static void explainAllResultMatchAssert(String sql, List<Object> param, Connection tddlConnection,
                                                   String expectPattern) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        String actualExplainResult = "";
        try {
            ResultSet rs = tddlPs.executeQuery();
            while (rs.next()) {
                actualExplainResult = actualExplainResult + "\n" + rs.getString(1);

            }

        } catch (Exception e) {
            e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
//        System.out.println(actualExplainResult);
        Assert.assertTrue(
            "expect pattern is " + expectPattern + "\n but actual is " + actualExplainResult + "\n sql is " + sql,
            actualExplainResult.matches(expectPattern));

    }

    /**
     * 验证查询结果非顺序一致，校验返回结果不允许为空
     */
    public static List<List<Object>> selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                             Connection tddlConnection) {
        return selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, false);
    }

    /**
     * 验证查询结果非顺序一致，校验返回结果不允许为空
     */
    public static void selectContentSameStringIgnoreCaseAssert(String sql, List<Object> param,
                                                               Connection mysqlConnection,
                                                               Connection tddlConnection,
                                                               boolean part) {
        selectStringContentIgnoreCaseSameAssert(sql, param, mysqlConnection, tddlConnection, false, part);
    }

    /**
     * 验证查询结果返回长度一致，用于group_concat场景对结果校验
     */
    public static void selectContentLengthSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                     Connection tddlConnection) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);

            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            int mysqlLength = mysqlResults.toString().length();
            int tddlLength = tddlResults.toString().length();
            Assert.assertEquals("sql is :" + sql, mysqlLength, tddlLength);

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    public static List<List<Object>> selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                             Connection tddlConnection, boolean allowEmptyResultSet) {
        return selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, allowEmptyResultSet, false);
    }

    public static void selectContentSameAssertIgonreMySQLWarnings(String sql, List<Object> param,
                                                                  Connection mysqlConnection,
                                                                  Connection tddlConnection,
                                                                  boolean allowEmptyResultSet) {
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, allowEmptyResultSet, false, true);
    }

    public static void selectContentSameAssert1(String sql, List<Object> param, Connection mysqlConnection,
                                                Connection tddlConnection, boolean allowEmptyResultSet) {
        selectContentSameAssert1(sql, param, mysqlConnection, tddlConnection, allowEmptyResultSet, false);
    }

    /**
     * 验证查询结果非顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectContentSameAssert1(String sql, List<Object> param, Connection mysqlConnection,
                                                Connection tddlConnection, boolean allowEmptyResultSet,
                                                boolean ignoreException) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        ResultSet mysqlRs1 = null;
        PreparedStatement mysqlPs1 = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);

//            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs, ignoreException);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs, ignoreException);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            final String show_warnings = "show warnings";
            mysqlPs1 = JdbcUtil.preparedStatementSet(show_warnings, param, mysqlConnection);
            mysqlRs1 = JdbcUtil.executeQuery(show_warnings, mysqlPs1);
            while (mysqlRs1.next()) {
                final String message = mysqlRs1.getString("Message");
                if (StringUtils.isNotEmpty(message)) {
                    if (message.contains("Incorrect") && message.contains("value:") && message.contains("for column")) {
                        return;
                    }
                }
            }
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql + " 参数为 :" + param).that(mysqlResults)
                .containsExactlyElementsIn(tddlResults);

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
            JdbcUtil.close(mysqlPs1);
            JdbcUtil.close(mysqlRs1);
        }
    }

    /**
     * 验证查询结果非顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static List<List<Object>> selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                             Connection tddlConnection, boolean allowEmptyResultSet,
                                                             boolean ignoreException) {
        return selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, allowEmptyResultSet,
            ignoreException,
            false);
    }

    public static List<List<Object>> selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                             Connection tddlConnection, boolean allowEmptyResultSet,
                                                             boolean ignoreException,
                                                             boolean ignoreMySQLWarnings) {
        return selectContentSameAssertWithDiffSql(sql, sql, param, mysqlConnection, tddlConnection, allowEmptyResultSet,
            ignoreException, ignoreException);
    }

    /**
     * 验证查询结果非顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static List<List<Object>> selectContentSameAssertWithDiffSql(String tddlSql, String mysqlSql,
                                                                        List<Object> param,
                                                                        Connection mysqlConnection,
                                                                        Connection tddlConnection,
                                                                        boolean allowEmptyResultSet,
                                                                        boolean ignoreException,
                                                                        boolean ignoreMySQLWarnings) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(mysqlSql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(mysqlSql, mysqlPs);

            if (ignoreMySQLWarnings && mysqlRs.getWarnings() != null) {
                String warningMessage = mysqlRs.getWarnings().getMessage();
                if ((!StringUtils.isEmpty(warningMessage)) && warningMessage.contains("Incorrect") && warningMessage
                    .contains("value") && warningMessage.contains("for column")) {
                    return new ArrayList<>();
                }
            }
            tddlPs = JdbcUtil.preparedStatementSet(tddlSql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(tddlSql, tddlPs);
            if (PropertiesUtil.isStrictType()) {
                List<String> mysqlColumnTypeNames = JdbcUtil.getAllTypeName(mysqlRs, ignoreException);
                List<String> tddlColumnTypeNmaes = JdbcUtil.getAllTypeName(tddlRs, ignoreException);
                System.out.println("mysql:" + mysqlColumnTypeNames);
                System.out.println("tddl:" + tddlColumnTypeNmaes);
                assertWithMessage(" #datatype#非顺序情况下：mysql 返回类型与tddl 返回类型不一致 \n sql 语句为：" + tddlSql + " 参数为 :" + param)
                    .that(
                        tddlColumnTypeNmaes).containsExactlyElementsIn(mysqlColumnTypeNames);
            }

            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs, ignoreException);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs, ignoreException);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + tddlSql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + tddlSql + " 参数为 :" + param)
                .that(tddlResults)
                .containsExactlyElementsIn(mysqlResults);

            return mysqlResults;
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 验证查询结果非顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectStringContentIgnoreCaseSameAssert(
        String sql, List<Object> param, Connection mysqlConnection, Connection tddlConnection,
        boolean allowEmptyResultSet, boolean part) {
        ResultSet mysqlRs = null;
        ResultSet tddlRs = null;

        try {
            String mySql = sql.replaceAll(PropertiesUtil.polardbXDBName1(part), PropertiesUtil.mysqlDBName1());
            mysqlRs = JdbcUtil.executeQuery(mySql, mysqlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlConnection);

            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllLowerCaseStringResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllLowerCaseStringResult(tddlRs);
            // Convert.
            for (List<Object> l0 : mysqlResults) {
                for (int i = 0; i < l0.size(); ++i) {
                    if (l0.get(i) instanceof String && ((String) l0.get(i))
                        .equalsIgnoreCase(PropertiesUtil.mysqlDBName1())) {
                        l0.set(i, PropertiesUtil.polardbXDBName1(part).toLowerCase());
                    }
                }
            }
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql + " 参数为 :" + param).that(tddlResults)
                .containsExactlyElementsIn(mysqlResults);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 验证查询结果非顺序一致，支持mysql和tddl不同sql 查询语句返回结果不允许为空
     */
    public static void selectContentSameAssert(String mysqlSql, String tddlSql, List<Object> param,
                                               Connection mysqlConnection,
                                               Connection tddlConnection) {
        selectContentSameAssert(mysqlSql, tddlSql, param, mysqlConnection, tddlConnection, false);
    }

    /**
     * 验证查询结果非顺序一致，支持mysql和tddl不同sql 查询语句返回结果不允许为空,但可以忽略某些列的对比
     */
    public static void selectContentSameAssertIgnoreColumn(String mysqlSql, String tddlSql, List<Object> param,
                                                           Connection mysqlConnection,
                                                           Connection tddlConnection, List<Integer> columnName) {
        selectContentSameAssertIgnoreColumn(mysqlSql, tddlSql, param, mysqlConnection, tddlConnection, false,
            columnName);
    }

    /**
     * 验证查询结果非顺序一致，支持mysql和tddl不同sql allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectContentSameAssert(String mysqlSql, String tddlSql, List<Object> param,
                                               Connection mysqlConn,
                                               Connection tddlConn, boolean allowEmptyResultSet) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(mysqlSql, param, mysqlConn);
            mysqlRs = JdbcUtil.executeQuery(mysqlSql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(tddlSql, param, tddlConn);
            tddlRs = JdbcUtil.executeQuery(tddlSql, tddlPs);

            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + mysqlSql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + mysqlSql + " 参数为 :" + param)
                .that(tddlResults).containsExactlyElementsIn(mysqlResults); //.containsExactlyElementsIn(tddlResults);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 验证查询结果非顺序一致，支持mysql和tddl不同sql allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectContentSameAssertIgnoreColumn(String mysqlSql, String tddlSql, List<Object> param,
                                                           Connection mysqlConn,
                                                           Connection tddlConn, boolean allowEmptyResultSet,
                                                           List<Integer> ignoreColumnIndex) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(mysqlSql, param, mysqlConn);
            mysqlRs = JdbcUtil.executeQuery(mysqlSql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(tddlSql, param, tddlConn);
            tddlRs = JdbcUtil.executeQuery(tddlSql, tddlPs);

            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResultIgnoreColumn(mysqlRs, ignoreColumnIndex);
            List<List<Object>> tddlResults = JdbcUtil.getAllResultIgnoreColumn(tddlRs, ignoreColumnIndex);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + mysqlSql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + mysqlSql + " 参数为 :" + param)
                .that(tddlResults).containsExactlyElementsIn(mysqlResults); //.containsExactlyElementsIn(tddlResults);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 验证查询结果顺序一致，查询语句返回结果不允许为空
     */
    public static void selectOrderAssert(String sql, List<Object> param, Connection mysqlConnection,
                                         Connection tddlConnection) {
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection, false);
    }

    /**
     * 验证查询结果顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectOrderAssert(String sql, List<Object> param, Connection mysqlConnection,
                                         Connection tddlConnection,
                                         boolean allowEmptyResultSet) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
            metaInfoCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            assertWithMessage(" 顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql + " 参数为 :" + param).that(tddlResults)
                .containsExactlyElementsIn(mysqlResults)
                .inOrder();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 按照某个字段（非主键）顺序一致校验，查询语句返回结果不允许为空
     */
    public static void selectOrderByNotPrimaryKeyAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                        Connection tddlConnection, String orderKey) {
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderKey, false);
    }

    /**
     * 按照某个字段（非主键）顺序一致校验， allowEmptyResultSet 查询语句返回结果是否允许为空
     */
    public static void selectOrderByNotPrimaryKeyAssert(String sql, List<Object> param, Connection mysqlConnection,
                                                        Connection tddlConnection, String orderByNotPrimaryColumn,
                                                        boolean allowEmptyResultSet) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
            metaInfoCheckSame(mysqlRs, tddlRs);

            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + sql + " ,param 为：" + param + "查询的结果集为空，请修改sql语句，保证有结果集",
                    mysqlResults.size() != 0);
            }

            assertWithMessage(sql + " 非排序情况下mysql 返回结果与tddl 返回结果不一致").that(tddlResults)
                .containsExactlyElementsIn(mysqlResults);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    public static void resultSetContentSameAssert(ResultSet rs1, ResultSet rs2, boolean allowEmptyResultSet) {
        metaInfoCheckSame(rs1, rs2);
        List<List<Object>> rs1Results = JdbcUtil.getAllResult(rs1);
        List<List<Object>> rs2Results = JdbcUtil.getAllResult(rs2);
        // 不允许为空结果集合
        if (!allowEmptyResultSet) {
            Assert.assertTrue("查询的结果集为空，请修改sql语句，保证有结果集", rs1Results.size() != 0);
        }
        assertWithMessage("返回结果不一致").that(rs1Results).containsExactlyElementsIn(rs2Results).inOrder();
    }

    /**
     * Ignore order
     */
    public static void resultSetContentSameAssert(List<String> columns, List<List<Object>> actual,
                                                  List<List<Object>> expected,
                                                  boolean allowEmptyResultSet) {
        // 不允许为空结果集合
        if (!allowEmptyResultSet) {
            Assert.assertTrue("查询的结果集为空，请修改sql语句，保证有结果集", actual.size() != 0);
        }

        System.out.println("Row count actual[" + actual.size() + "] expected[" + expected.size() + "]");

        final Iterator<List<Object>> actualIter = actual.iterator();
        final Iterator<List<Object>> requiredIter = expected.iterator();

        while (actualIter.hasNext() && requiredIter.hasNext()) {
            final List<Object> actualElement = actualIter.next();
            final List<Object> requiredElement = requiredIter.next();

            if (!Objects.equals(requiredElement, actualElement)) {
                // Missing elements; elements that are not missing will be removed as we
                // iterate.
                final Collection<List<Object>> missing = new ArrayList<>();
                missing.add(requiredElement);
                Iterators.addAll(missing, requiredIter);

                // Extra elements that the subject had but shouldn't have.
                final Collection<List<Object>> extra = new ArrayList<>();

                // Remove all actual elements from missing, and add any that weren't in missing
                // to extra.
                if (!missing.remove(actualElement)) {
                    extra.add(actualElement);
                }
                while (actualIter.hasNext()) {
                    List<Object> item = actualIter.next();
                    if (!missing.remove(item)) {
                        extra.add(item);
                    }
                }

                if (!missing.isEmpty() || !extra.isEmpty()) {
                    if (!missing.isEmpty()) {
                        System.out.println("Missing rows: ");
                        missing.forEach(row -> {
                            for (int i = 0; i < columns.size(); ++i) {
                                System.out.print(columns.get(i) + ":" + row.get(i));
                                if (i != columns.size() - 1) {
                                    System.out.print(",");
                                }
                            }
                            System.out.println();
                        });
                    }
                    if (!extra.isEmpty()) {
                        System.out.println("Unexpected rows: ");
                        extra.forEach(row -> {
                            for (int i = 0; i < columns.size(); ++i) {
                                System.out.print(columns.get(i) + ":" + row.get(i));
                                if (i != columns.size() - 1) {
                                    System.out.print(",");
                                }
                            }
                            System.out.println();
                        });
                    }

                    throw new AssertionError(
                        "Result not match, miss[" + missing.size() + "] unexpected[" + extra.size() + "]");
                }

                break;
            }
        }

        if (actualIter.hasNext()) {
            final List<Object> actualElement = actualIter.next();

            System.out.println("Unexpected rows: ");
            for (int i = 0; i < columns.size(); ++i) {
                System.out.print(columns.get(i) + ":" + actualElement.get(i));
                if (i != columns.size() - 1) {
                    System.out.print(",");
                }
            }
            System.out.println();
            throw new AssertionError("Result not match, unexpected.");
        } else if (requiredIter.hasNext()) {
            final List<Object> requiredElement = requiredIter.next();

            System.out.println("Missing rows: ");
            for (int i = 0; i < columns.size(); ++i) {
                System.out.print(columns.get(i) + ":" + requiredElement.get(i));
                if (i != columns.size() - 1) {
                    System.out.print(",");
                }
            }
            System.out.println();
            throw new AssertionError("Result not match, missing.");
        }
    }

    public static void metaInfoCheckSame(ResultSet mysqlRs, ResultSet tddlRs) {
        List<String> mysqlColumnName = JdbcUtil.getColumnNameListToLowerCase(mysqlRs);
        List<String> tddlColumnName = JdbcUtil.getColumnNameListToLowerCase(tddlRs);
        assertWithMessage("mysql 表返回的列名和tddl返回的列名不一致").that(tddlColumnName)
            .containsExactlyElementsIn((mysqlColumnName))
            .inOrder();
    }

    public static void selectConutAssert(String sql, List<Object> param, Connection mysqlConnection,
                                         Connection tddlConnection) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        try {

            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
            Assert.assertEquals(
                "sql 为:" + sql + " 返回条数不一致，mysql为：" + resultsSize(mysqlRs) + " tddl 为：" + resultsSize(tddlRs),
                resultsSize(mysqlRs),
                resultsSize(tddlRs));
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    /**
     * 验证tddl和mysql数据的结果集包含的内容长度相同，保证顺序, 主要针对TEXT或者BLOB字段
     */
    public static void assertContentLengthSame(String sql, List<Object> param, String[] columnParam,
                                               Connection mysqlConnection, Connection tddlConnection) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
        mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
        tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        tddlRs = JdbcUtil.executeQuery(sql, tddlPs);

        metaInfoCheckSame(mysqlRs, tddlRs);
        try {
            while (mysqlRs.next() && tddlRs.next()) {
                for (int i = 0; i < columnParam.length; i++) {
                    Object mysqlObject = JdbcUtil.getObject(mysqlRs, columnParam[i]);
                    Object tddlObject = JdbcUtil.getObject(tddlRs, columnParam[i]);
                    if (null == mysqlObject || null == tddlObject) {
                        Assert.assertNull(mysqlObject);
                        Assert.assertNull(tddlObject);
                    } else {
                        Assert.assertEquals(mysqlObject.toString().length(), tddlObject.toString().length());
                    }

                }
            }
        } catch (SQLException e) {
            String errorMs = "[ preparedStatement next] failed ";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);

        }
        try {
            Assert.assertFalse(tddlRs.next());
            Assert.assertFalse(mysqlRs.next());
        } catch (SQLException e) {
            String errorMs = "[ preparedStatement next] failed ";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(tddlRs);
        }

    }

    /**
     * 查询blob类型正确行校验
     */
    public static void selectBlobContentSameAssert(String sql, List<Object> param, String[] columnParam,
                                                   Connection mysqlConnection, Connection tddlConnection) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
        mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
        tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
        metaInfoCheckSame(mysqlRs, tddlRs);
        try {
            assertBlobContentSame(mysqlRs, tddlRs, columnParam);
        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(tddlPs);
        }
    }

    /**
     * 更新异常校验
     */
    public static void updateErrorAssert(String sql, List<Object> param, Connection tddlConnection,
                                         String expectErrorMess) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        String errorMess = null;
        try {
            tddlPs.executeUpdate();
            Assert.fail("sql 应该执行失败：" + sql);
        } catch (Exception e) {
            // e.printStackTrace();
            errorMess = e.getMessage();
        } finally {
            JdbcUtil.close(tddlPs);
        }
        Assert.assertTrue("expect error is " + expectErrorMess + "\n but actual is " + errorMess,
            errorMess.contains(expectErrorMess));
    }

    /**
     * 对blob类型特别的处理，把blob类型放在第一个比较的字段
     */
    public static void assertBlobContentSame(ResultSet mysqlRs, ResultSet tddlRs, String[] columnParam) {
        boolean same = false;
        List<Object> multiMysqlResult = new ArrayList<Object>();
        List<Object> multiResult = new ArrayList<Object>();
        try {
            while (mysqlRs.next()) {
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> Result = new ArrayList<Object>();
                tddlRs.next();
                for (int i = 0; i < columnParam.length; i++) {
                    if (i == 0) {
                        Blob b1 = mysqlRs.getBlob(columnParam[i]);
                        Blob b2 = tddlRs.getBlob(columnParam[i]);
                        if (b1 != null) {
                            int mysqlLength = (int) b1.length();
                            byte[] mysqlBl = b1.getBytes(1, mysqlLength);
                            for (int j = 0; j < mysqlLength; j++) {
                                mysqlResult.add(mysqlBl[j]);
                            }
                        }
                        if (b2 != null) {
                            int length = (int) b2.length();
                            byte[] bl = b2.getBytes(1, length);
                            for (int j = 0; j < length; j++) {
                                Result.add(bl[j]);
                            }
                        }
                    } else {
                        mysqlResult.add(mysqlRs.getObject(columnParam[i]));
                        Result.add(tddlRs.getObject(columnParam[i]));
                    }
                }
                multiMysqlResult.add(mysqlResult);
                multiResult.add(Result);
            }
            if (multiMysqlResult.size() != multiResult.size()) {
                Assert.fail();
            }
            for (int i = 0; i < multiMysqlResult.size(); i++) {
                Object mResult = multiMysqlResult.get(i);
                multiResult.remove(mResult);
            }
            if (multiResult.size() == 0 && tddlRs.next() == false) {
                same = true;
            }
            if (same != true) {
                Assert.fail("Results not same!");
            }
        } catch (SQLException e) {
            String errorMs = "[ assertBlobContentSame ] failed ";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {

        }
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
            String errorMs = "[ResultSet get next] failed ";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }
        return row;
    }

    private void orderByColumnIndexCheck(String sql, String orderKey, List<List<Object>> mysqlResults,
                                         List<List<Object>> tddlResults, int columnIndex) {
        Object orderClounmValue = null;
        List<List<Object>> mysqlResultsByOrder = new ArrayList<List<Object>>();
        List<List<Object>> tddlResultsByOrder = new ArrayList<List<Object>>();

        for (int i = 0; i < mysqlResults.size(); i++) {
            List<Object> mysqlResList = mysqlResults.get(i);
            Object nextClounmValue = mysqlResList.get(columnIndex);
            // 对null的特殊处理
            if (nextClounmValue == null) {
                nextClounmValue = "null_value_flag";
            }
            if (i == 0) {
                orderClounmValue = nextClounmValue;
            }
            if (orderClounmValue.equals(nextClounmValue)) {
                mysqlResultsByOrder.add(mysqlResults.get(i));
                tddlResultsByOrder.add(tddlResults.get(i));
            } else {
                assertWithMessage(sql + " 按照column " + orderKey + " 排序情况下，排序值为 " + orderClounmValue
                    + "结果集：mysql 返回结果与tddl 返回结果不一致").that(tddlResultsByOrder)
                    .containsExactlyElementsIn(mysqlResultsByOrder);

                mysqlResultsByOrder.clear();
                tddlResultsByOrder.clear();
                mysqlResultsByOrder.add(mysqlResults.get(i));
                tddlResultsByOrder.add(tddlResults.get(i));
                orderClounmValue = nextClounmValue;

                if (i == mysqlResults.size() - 1) {
                    assertWithMessage(" 按照column " + orderKey + " 排序情况下，排序值为 " + orderClounmValue
                        + "结果集：mysql 返回结果与tddl 返回结果不一致").that(tddlResultsByOrder)
                        .containsExactlyElementsIn(mysqlResultsByOrder);
                    return;
                }
            }
            if (i == mysqlResults.size() - 1) {

                assertWithMessage(" 按照column " + orderKey + " 排序情况下，排序值为 " + orderClounmValue
                    + "结果集：mysql 返回结果与tddl 返回结果不一致").that(tddlResultsByOrder)
                    .containsExactlyElementsIn(mysqlResultsByOrder);
            }
        }
    }

    public boolean isPrimaryIndexExist(String tableName, String columnName, Connection conn) {
        String sql = String.format("show index from %s", tableName);
        boolean isExist = false;
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, sql);
            while (rs.next()) {
                String keyName = rs.getString("Key_name");
                String column = rs.getString("Column_name");

                if (keyName.equals("PRIMARY") && column.equals(columnName)) {
                    isExist = true;
                    break;
                }
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);

        }
        return isExist;
    }

    public static void assertNotExistsBroadcastTable(String tableName, Connection conn) {
        String sql = String.format("/*TDDL: enable_broadcast_random_read=false*/ select * from %s", tableName);
        JdbcUtil.executeQueryFaied(conn, sql, new String[] {"doesn't exist", "does not exist", "Executor error"});
    }

    public static void assertNotExistsTable(String tableName, Connection conn) {
        String sql = String.format("select * from %s", tableName);
        JdbcUtil.executeQueryFaied(conn, sql, new String[] {"doesn't exist", "does not exist", "Executor error"});
        // TODO: select * from xx if use XPlan, and this will throw 'Executor error'
    }

    public static void assertExistsTable(String tableName, Connection conn) {
        String sql = String.format("select * from %s limit 1", tableName);
        JdbcUtil.executeQuerySuccess(conn, sql);
    }

    /**
     * assert表存在，通过select * 是否出错来判断
     */
    public static boolean isIndexExist(String tableName, String indexName, Connection conn) {
        String sql = String.format("show index from %s", tableName);
        boolean isExist = false;
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, sql);
            while (rs.next()) {
                String keyName = rs.getString("Key_name");
                if (keyName.equals(indexName)) {
                    isExist = true;
                    break;
                }
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }
        return isExist;
    }

    public static boolean isIndexExistByInfoSchema(String schemaName, String tableName, String indexName,
                                                   Connection conn) {
        String sql = String.format(
            "select count(*) from information_schema.statistics where table_schema = '%s' and table_name = '%s' and index_name = '%s'",
            schemaName, tableName, indexName);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        return false;
    }

    /**
     * 确认所有库上index都不存在
     */
    public static void assertShardDbIndexNotExist(List<Connection> dbConnList, String tableName, String indexName) {
        for (Connection conn : dbConnList) {
            Assert.assertFalse(isIndexExist(tableName, indexName, conn));
        }
    }

    /**
     * 确认所有库上index都存在
     */
    public static void assertShardDbIndexExist(List<Connection> dbConnList, String tableName, String indexName) {
        for (Connection conn : dbConnList) {
            Assert.assertTrue(isIndexExist(tableName, indexName, conn));
        }
    }

    /**
     * 确认所有库上表都存在
     */
    public static void assertShardDbTableExist(List<Connection> dbConnList, String tableName, boolean ignoreSingleGroup,
                                               int dbNum) {
        if (ignoreSingleGroup) {
            for (int i = 0; i < dbConnList.size(); i++) {
                if (i < dbNum) {
                    assertExistsTable(tableName, dbConnList.get(i));
                }
            }
        } else {
            for (Connection conn : dbConnList) {
                assertExistsTable(tableName, conn);
            }
        }
    }

    public static void assertShardDbTableExist(List<Connection> dbConnList, String tableName) {
        assertShardDbTableExist(dbConnList, tableName, false, 0);
    }

    /**
     * 确认所有库上表都不存在，适用于不分表，只分库的情况
     */
    public static void assertShardDbTableNotExist(List<Connection> dbConnList, String tableName) {
        for (Connection conn : dbConnList) {
            assertNotExistsTable(tableName, conn);
        }
    }

    /**
     * 通过show table查看表是否存在
     */
    public static boolean isShowTableExist(String tableName, Connection conn) {
        boolean exists = false;
        String sql = String.format("show tables like \"%s\"", tableName);
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, sql);
            if (rs.next()) {
                exists = true;
            }
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        } finally {
            JdbcUtil.close(rs);

        }
        return exists;
    }

    /**
     * assert表存在
     */
    @SuppressWarnings("resource")
    public static void assertExistsColumn(String tableName, String column, Connection conn) {
        String sql = String.format("select * from %s limit 1", tableName);
        ResultSet rs = null;
        PreparedStatement prepareStatement = null;
        try {
            prepareStatement = conn.prepareStatement(sql);
            rs = prepareStatement.executeQuery();
            ResultSetMetaData rsMetadata = rs.getMetaData();
            for (int i = 1; i <= rsMetadata.getColumnCount(); i++) {
                if (rsMetadata.getColumnLabel(i).equalsIgnoreCase(column)) {
                    return;
                }
            }
            Assert.fail(String.format("table[%s] not exist this column[%s]", tableName, column));
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(prepareStatement);
        }
    }

    public static void assertVariable(String variableName, String expectValue, Connection tddlConnection,
                                      boolean ignoreCase) {
        String sql = "SHOW VARIABLES LIKE '" + variableName + "'";
        ResultSet rs = null;
        PreparedStatement prepareStatement = null;
        try {
            prepareStatement = tddlConnection.prepareStatement(sql);
            rs = prepareStatement.executeQuery();
            rs.next();
            if (ignoreCase) {
                Assert.assertEquals(expectValue.toLowerCase(), rs.getString(2).toLowerCase());
            } else {
                Assert.assertEquals(expectValue, rs.getString(2));
            }
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(prepareStatement);
        }
    }

    public static void assertShardCount(Connection tddlConnection, String sql, int shardCount) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql);
        try {
            boolean error = true;

            while (rs.next()) {
                final String plan = rs.getString(1);

                if (shardCount == 1) {
                    Assert.assertFalse("unexpected shard count in plan:\n " + plan,
                        TStringUtil.contains(plan, "shardCount="));
                } else {
                    Pattern p = Pattern.compile(", shardCount=(\\d+), ");
                    final Matcher matcher = p.matcher(plan);
                    if (matcher.find()) {
                        Assert.assertEquals("unexpected shard count in plan:\n" + plan,
                            Integer.valueOf(shardCount),
                            Integer.valueOf(matcher.group(1)));
                        error = false;
                    }
                }
            } // end of while

            if (shardCount != 1) {
                Assert.assertFalse("unexpected shard count 1, " + shardCount + " is expected", error);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assert.fail("explain exception: explain " + sql + " , messsage is " + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public static void assertShardCount(Connection tddlConnection, String sql, Map<String, Integer> tableShardCounts) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain sharding " + sql);
        try {
            boolean allSingleTable = true;
            Set<String> tableNames = new HashSet<>();
            while (rs.next()) {
                final String tableName = rs.getString(1);
                final Integer shardCount = rs.getInt(3);

                if (tableShardCounts.containsKey(tableName)) {
                    Assert.assertEquals(tableShardCounts.get(tableName), shardCount);
                    tableNames.add(tableName);
                }

                allSingleTable = false;
            } // end of while

            if (allSingleTable) {
                Assert.assertTrue("unexpected empty explain sharding result. sql: explain sharding " + sql,
                    tableShardCounts.entrySet().stream().allMatch(s -> s.getValue() == 1));
            }

            if (!allSingleTable && tableNames.size() < tableShardCounts.size()) {
                final Set<String> excepted = new HashSet<>(tableShardCounts.keySet());
                excepted.removeAll(tableNames);

                Assert.fail("cannot find table shard count for " + StringUtils.join(excepted, ","));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assert.fail("explain exception: explain sharding " + sql + " , messsage is " + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }

    }

    /**
     * 验证查询结果非顺序一致，校验返回结果不允许为空
     */
    public static void selectContentIgnoreJsonFormatSameAssert(String sql, List<Object> param,
                                                               Connection mysqlConnection,
                                                               Connection tddlConnection) {
        selectStringContentIgnoreJsonFormatSameAssert(sql, param, mysqlConnection, tddlConnection, false);
    }

    /**
     * 验证查询结果非顺序一致 allowEmptyResultSet 查询语句返回结果是否允许为空
     * drds返回的json 格式{"a":{"b":false,"c":{"k":{"k1":false},"b1":false}}}
     * 和MySQL返回的json 格式{"a": {"b": false, "c": {"k": {"k1": false}, "b1": false}}}
     * 我们认为这样的结果集是一致的
     */
    public static void selectStringContentIgnoreJsonFormatSameAssert(String sql, List<Object> param,
                                                                     Connection mysqlConnection,
                                                                     Connection tddlConnection,
                                                                     boolean allowEmptyResultSet) {
        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);

            metaInfoIgnoreJsonFormatCheckSame(mysqlRs, tddlRs);
            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            // 不允许为空结果集合
            if (!allowEmptyResultSet) {
                Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);
            }
            List<List<Object>> mysqlResultsFastJsonFormat = convertToFastJsonFormat(mysqlResults);
            List<List<Object>> tddlResultsFastJsonFormat = convertToFastJsonFormat(tddlResults);
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql + " 参数为 :" + param)
                .that(tddlResultsFastJsonFormat)
                .containsExactlyElementsIn(mysqlResultsFastJsonFormat);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }

    public static void metaInfoIgnoreJsonFormatCheckSame(ResultSet mysqlRs, ResultSet tddlRs) {
        List<String> mysqlColumnName = JdbcUtil.getColumnNameListToLowerCase(mysqlRs);
        List<String> tddlColumnName = JdbcUtil.getColumnNameListToLowerCase(tddlRs);
        List<String> mysqlColumnNameFastJsonFormat = ignoreJsonFormatForColumns(mysqlColumnName);
        List<String> tddlColumnNameFastJsonFormat = ignoreJsonFormatForColumns(tddlColumnName);

        assertWithMessage("mysql 表返回的列名和tddl返回的列名不一致").that(tddlColumnNameFastJsonFormat)
            .containsExactlyElementsIn((mysqlColumnNameFastJsonFormat))
            .inOrder();
    }

    private static List<String> ignoreJsonFormatForColumns(List<String> columnName) {
        List<String> columnNamewithoutFormat = new ArrayList<String>();
        for (int i = 0; i < columnName.size(); i++) {
            columnNamewithoutFormat.add(columnName.get(i).replaceAll(" \\(", "(")
                .replaceAll(" \\)", ")")
                .replaceAll("\\( ", "(")
                .replaceAll(", ", ",")
                .replaceAll("\"", "'"));
        }
        return columnNamewithoutFormat;
    }

    private static List<List<Object>> convertToFastJsonFormat(List<List<Object>> resultset) {
        List<List<Object>> resultsFastJsonFormat = new ArrayList<List<Object>>();
        for (int i = 0; i < resultset.size(); i++) {
            List<Object> newRow = new ArrayList<Object>();
            List<Object> row = resultset.get(i);
            for (int j = 0; j < row.size(); j++) {
                Object cell = null;
                try {
                    cell = JSON.parse(row.get(i).toString());
                } catch (Exception e) {
                    cell = row.get(j);
                }
                if (cell == null) {
                    newRow.add("NULL");
                } else if (cell instanceof byte[]) {
                    newRow.add(new String((byte[]) cell));
                } else if (cell instanceof Number) {
                    newRow.add(cell);
                } else {
                    newRow.add(cell.toString());
                }
            }
            resultsFastJsonFormat.add(newRow);
        }
        return resultsFastJsonFormat;
    }
}
