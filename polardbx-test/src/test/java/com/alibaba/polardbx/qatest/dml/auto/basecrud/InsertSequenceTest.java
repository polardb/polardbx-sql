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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import com.google.common.truth.Truth;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Insert测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class InsertSequenceTest extends AutoCrudBasedLockTestCase {
    private static final Log log = LogFactory.getLog(InsertSequenceTest.class);
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays
            .asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE_AUTONIC));

    }

    public InsertSequenceTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        log.info("开始执行用例InsertSequenceTest");
        if (baseOneTableName.startsWith("broadcast")) {
            //JdbcUtil.setTxPolicy(ITransactionPolicy.FREE, tddlConnection);
        }

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    @After
    public void resetSqlMode() {
        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode=''");
    }

    @Test
    public void insertFromDualAllFieldTest() throws Exception {

        String sql = "insert into " + baseOneTableName
            + "(pk,integer_test,varchar_test) select AUTO_SEQ_" + baseOneTableName
            + ".nextval,?,? from dual";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        JdbcUtil.updateDataTddl(tddlConnection, sql, param);

        sql = "select last_insert_id()";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null,
            tddlConnection);
        ResultSet rc = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rc.next());

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithMutil_sequenceTest() throws Exception {
        String sql = "insert into " + baseOneTableName
            + "(pk,integer_test,varchar_test) values(AUTO_SEQ_" + baseOneTableName
            + ".nextval,?,?),(AUTO_SEQ_" + baseOneTableName + ".nextval,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValueTwo);
        JdbcUtil.updateDataTddl(tddlConnection, sql, param);

        sql = "select last_insert_id()";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null,
            tddlConnection);
        ResultSet rc = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rc.next());
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insert_SequenceTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,varchar_test,integer_test) values(AUTO_SEQ_"
            + baseOneTableName + ".nextval,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.integer_testValue);
        long genValue = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, param, "pk");

        sql = "select pk from " + baseOneTableName;
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet rc = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rc.next());
        long pk = rc.getLong(1);

        Assert.assertTrue("getGeneratedKeys() not equal to actual pk", genValue == pk);
    }

    @Test
    public void insert_SingleTableTest() throws Exception {
        if (!baseOneTableName.contains("one_db_one_tb")) {
            return;
        }

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        String sql = "insert into " + baseOneTableName + " (varchar_test) values(?)";
        param.add(columnDataGenerator.varchar_testValue);
        JdbcUtil.updateDataTddl(tddlConnection, sql, param);

        List<Long> pks = new ArrayList<>();
        sql = "select pk from " + baseOneTableName + " where varchar_test=?";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        while (rs.next()) {
            pks.add(rs.getLong(1));
        }
        rs.close();
        tddlPs.close();

        sql = "select last_insert_id()";
        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());

        long lastInsertId = rs.getLong(1);
        Assert.assertTrue(pks.contains(lastInsertId));
    }

    /**
     * Test insert with functions.
     */
    @Test
    public void insertFuncFields() throws Exception {
        String sql = String.format("insert into %s(integer_test, varchar_test) values(1, 'a')", baseOneTableName);
        long lastInsertId = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, Lists.newArrayList(), "pk");

        sql = String.format("replace into %s(integer_test, varchar_test) values(last_insert_id(), 'a')",
            baseOneTableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select varchar_test from %s where integer_test=%d", baseOneTableName, lastInsertId);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());

        sql = String.format("insert into %s(bigint_test, varchar_test) values(1, schema())", baseOneTableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select varchar_test from %s where bigint_test=%d", baseOneTableName, 1);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
        String schema = rs.getString(1);
        Assert.assertTrue(schema.endsWith("_APP") || schema.endsWith("_app"));
    }

    /**
     * Test insert with auto-increment column inside transactions
     */
    @Test
    public void insertAutoIncrementInTransactionTest() throws Exception {
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdate(tddlConnection, "set drds_transaction_policy='2PC'");

            String sql = String.format("insert into %s(integer_test, varchar_test) values(1, 'a')", baseOneTableName);
            long lastInsertId = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, Lists.newArrayList(), "pk");

            sql = String.format("replace into %s(integer_test, varchar_test) values(last_insert_id(), 'a')",
                baseOneTableName);
            JdbcUtil.executeUpdate(tddlConnection, sql);

            sql = String.format("select varchar_test from %s where integer_test=%d", baseOneTableName, lastInsertId);
            ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
            Assert.assertTrue(rs.next());

            sql = String.format("insert into %s(bigint_test, varchar_test) values(1, schema())", baseOneTableName);
            JdbcUtil.executeUpdate(tddlConnection, sql);

            sql = String.format("select varchar_test from %s where bigint_test=%d", baseOneTableName, 1);
            rs = JdbcUtil.executeQuery(sql, tddlConnection);
            Assert.assertTrue(rs.next());

            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
    }

    /**
     * Test insert single row in multi threads.
     */
    @Test
    public void insertSingleTestConcurrency() throws Exception {
        StringBuilder stringBuilder = new StringBuilder("insert into ").append(baseOneTableName)
            .append("(integer_test, varchar_test) values(?, ?)");
        final String insertSql = stringBuilder.toString();
        final int INSERT_NUM = 500;
        final int THREAD_NUM = 4;

        final ConcurrentLinkedDeque<AssertionError> errors = new ConcurrentLinkedDeque<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < THREAD_NUM; i++) {
            Thread thread = new Thread(new Runnable() {

                public void run() {
                    Connection connection = null;
                    try {
                        connection = getPolardbxConnection();
                        for (int i = 0; i < INSERT_NUM; i++) {
                            JdbcUtil.updateData(connection, insertSql, Lists.newArrayList(i, i + ""));
                        }

                        Long lastInsertId = JdbcUtil.updateDataTddlAutoGen(connection,
                            insertSql,
                            Lists.newArrayList(INSERT_NUM, INSERT_NUM + ""),
                            "pk");

                        String selectSql = "select pk from " + baseOneTableName + " where integer_test=" + INSERT_NUM;
                        ResultSet rs = JdbcUtil.executeQuery(selectSql, connection);
                        List<Long> pks = JdbcUtil.getAllResult(rs)
                            .stream()
                            .map(list -> ((JdbcUtil.MyNumber) list.get(0)).getNumber().longValue())
                            .collect(Collectors.toList());
                        rs.close();

                        Assert.assertTrue(pks.contains(lastInsertId));

                    } catch (SQLException se) {
                        throw GeneralUtil.nestedException(se);
                    } catch (AssertionError ae) {
                        errors.add(ae);
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        AssertionError error = errors.peekLast();
        if (error != null) {
            throw error;
        }
    }

    /**
     * sequence value is a string '0'
     */
    @Test
    public void insertSequenceString() throws Exception {
        String insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values('0', 'a')";
        Statement st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = st.getGeneratedKeys();
        List<List<Object>> autoKeys = JdbcUtil.getAllResult(rs);
        st.close();
        rs.close();

        String selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        List<List<Object>> keys = JdbcUtil.getAllResult(rs);
        Truth.assertThat(autoKeys).containsExactlyElementsIn(keys);
        st.close();
        rs.close();
    }

    /**
     * Sequence value is assigned explicitly
     */
    @Test
    public void insertExplicitSequenceValue() throws Exception {
        String insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(0, 'a')";
        Statement st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = st.getGeneratedKeys();
        List<List<Object>> autoKeys = JdbcUtil.getAllResult(rs);
        st.close();
        rs.close();

        String selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        List<List<Object>> keys = JdbcUtil.getAllResult(rs);
        Truth.assertThat(autoKeys).containsExactlyElementsIn(keys);
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(1, 'a')";
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        autoKeys = JdbcUtil.getAllResult(rs);
        st.close();
        rs.close();

        selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        keys = JdbcUtil.getAllResult(rs);
        Truth.assertThat(autoKeys).containsExactlyElementsIn(keys);
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(0, 'a'), (0, 'a')";
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        autoKeys = JdbcUtil.getAllResult(rs);
        st.close();
        rs.close();

        selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        keys = JdbcUtil.getAllResult(rs);
        Truth.assertThat(autoKeys).containsExactlyElementsIn(keys);
        st.close();
        rs.close();
    }

    /**
     * sql_mode = NO_AUTO_VALUE_ON_ZERO
     */
    @Test
    public void insertNoAutoValueOnZeroTest() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode='NO_AUTO_VALUE_ON_ZERO'");
        String insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(0, 'a')";
        Statement st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = st.getGeneratedKeys();
        Assert.assertTrue(!rs.next());
        st.close();
        rs.close();

        String selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        long pk = rs.getLong(1);
        Assert.assertTrue(pk == 0);
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode=''");
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        pk = rs.getLong(1);
        Assert.assertTrue(pk > 0);
        st.close();
        rs.close();

        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        long pk1 = rs.getLong(1);
        Assert.assertTrue(pk == pk1);
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode='no_auto_value_on_zero'");
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        Assert.assertTrue(!rs.next());
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(0, 'a'), (1, 'b')";
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        st.close();

        selectSql = "select pk from " + baseOneTableName + " where pk=0";
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        // string
        insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values('0', 'a')";
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        st.close();

        selectSql = "select pk from " + baseOneTableName + " where pk=0";
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        insertSql = "insert into " + baseOneTableName + " (varchar_test) values('a')";
        st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        pk = rs.getLong(1);
        Assert.assertTrue(pk > 0);
        st.close();
        rs.close();

        selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        pk1 = rs.getLong(1);
        Assert.assertTrue(pk == pk1);
        st.close();
        rs.close();
    }

    /**
     * sql_mode = NO_AUTO_VALUE_ON_ZERO in multi-thread
     */
    @Test
    public void multiThreadNoAutoValueOnZeroTest() throws Exception {
        final int threadNum = 4;
        // broadcast table insertion is too slow
        final int rounds = baseOneTableName.endsWith("broadcast") ? 100 : 1000;

        String sql = "insert ignore into " + baseOneTableName + " (pk, varchar_test) values(0, 'a')";
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            String sqlMode = i < threadNum / 2 ? "" : "NO_AUTO_VALUE_ON_ZERO";
            Thread runner = new SequenceSqlRunner(sql, sqlMode, rounds);
            runner.start();
            threads[i] = runner;
        }

        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }

        sql = "select count(1) from " + baseOneTableName;
        Statement st = tddlConnection.createStatement();
        ResultSet rs = st.executeQuery(sql);
        rs.next();
        Assert.assertEquals((threadNum / 2) * rounds + 1, rs.getLong(1));
        st.close();
        rs.close();
    }

    /**
     * sql_mode = NO_AUTO_VALUE_ON_ZERO
     * explicit value is null
     */
    //FIXME mocheng
    @Ignore
    public void insertNoAutoValueOnZeroTest1() throws Exception {
        final ResultSet rs0 = JdbcUtil.executeQuery("select AUTO_SEQ_" + baseOneTableName + ".nextVal", tddlConnection);
        Assert.assertTrue(rs0.next());
        final long startSeqVal = rs0.getLong(1);
        rs0.close();

        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode='NO_AUTO_VALUE_ON_ZERO'");
        String insertSql = "insert into " + baseOneTableName + " (pk, varchar_test) values(null, 'a')";
        Statement st = tddlConnection.createStatement();
        st.execute(insertSql, Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = st.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        final long lastInsertId = rs.getLong(1);
        Assert.assertTrue(
            "Expect lastInsertId " + lastInsertId + " is bigger than max sequence before insert " + startSeqVal,
            lastInsertId > startSeqVal);
        st.close();
        rs.close();

        String selectSql = "select pk from " + baseOneTableName;
        st = tddlConnection.createStatement();
        rs = st.executeQuery(selectSql);
        Assert.assertTrue(rs.next());
        long pk = rs.getLong(1);
        Assert.assertTrue("Expect pk " + pk + " is bigger than max sequence before insert " + startSeqVal,
            pk > startSeqVal);
        Assert.assertTrue(pk > startSeqVal);
        st.close();
        rs.close();

        JdbcUtil.executeSuccess(tddlConnection, "delete from " + baseOneTableName);

        JdbcUtil.executeSuccess(tddlConnection, "set session sql_mode=''");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertSelectLastInsertIdTest() throws Exception {

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " (";
        String values = " values ( ";
        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }
        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";
        sql = sql + values;

        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        // Check last_insert_id for INSERT SELECT
        sql = MessageFormat
            .format("insert into {0} (varchar_test, integer_test) select varchar_test, integer_test from {0}",
                baseOneTableName);
        String returnedLastInsertId =
            String.valueOf(JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, null, "pk"));
        String lastInsertId =
            JdbcUtil.executeQueryAndGetFirstStringResult("select last_insert_id()", tddlConnection);

        Assert.assertEquals(lastInsertId, returnedLastInsertId);

        returnedLastInsertId =
            String.valueOf(JdbcUtil.updateDataTddlAutoGen(mysqlConnection, sql, null, "pk"));
        lastInsertId =
            JdbcUtil.executeQueryAndGetFirstStringResult("select last_insert_id()", mysqlConnection);

        Assert.assertEquals(lastInsertId, returnedLastInsertId);

        sql = "select integer_test, varchar_test, char_test, bigint_test, float_test from " + baseOneTableName;

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Test multi value with expression in it
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest() throws Exception {
        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (varchar_test, integer_test, datetime_test) values(''a'', 1, NOW()), (''b'', 2, NOW()), (''c'', 3, NOW())",
                baseOneTableName);
        final Statement statement = JdbcUtil.createStatement(tddlConnection);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try {
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            final ResultSet rs = statement.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            JdbcUtil.close(statement);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "select pk from " + baseOneTableName + " order by pk");
        final List<String> pkList = new ArrayList<>();
        while (rs.next()) {
            pkList.add(rs.getString(1));
        }

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
    }

    /**
     * Test batch insert with preparedStatement
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest1() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (varchar_test, integer_test, datetime_test) values(?, ?, ?)",
                baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            for (int i = 0; i < 3; i++) {
                ps.setString(1, "a");
                ps.setInt(2, i);
                ps.setString(3, "2021-04-08 16:55:25");

                ps.addBatch();
            }

            ps.executeBatch();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "select pk from " + baseOneTableName + " order by pk");
        final List<String> pkList = new ArrayList<>();
        while (rs.next()) {
            pkList.add(rs.getString(1));
        }

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
    }

    /**
     * Test multi value with expression and partial primary key specified
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest2() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(1, ''a'', 1, NOW()), (null, ''b'', 2, NOW()), (2, ''c'', 3, NOW()), (null, ''b'', 4, NOW())",
                baseOneTableName);
        final Statement statement = JdbcUtil.createStatement(tddlConnection);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try {
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            final ResultSet rs = statement.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            JdbcUtil.close(statement);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " where integer_test=2");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
////System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest3() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(?, ?, ?, ?)",
                baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            for (int i = 1; i < 5; i++) {
                if (i % 2 == 0) {
                    ps.setNull(1, Types.INTEGER);
                } else {
                    ps.setInt(1, i);
                }
                ps.setString(2, "a");
                ps.setInt(3, i);
                ps.setString(4, "2021-04-14 16:55:25");

                ps.addBatch();
            }

            ps.executeBatch();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " where integer_test=2");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest4() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(1, ?, ?, ?), (?, ?, ?, ?), (2, ?, ?, ?), (?, ?, ?, ?)",
                baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            ps.setString(1, "a");
            ps.setInt(2, 1);
            ps.setString(3, "2021-04-14 16:55:25");
            ps.setNull(4, Types.INTEGER);
            ps.setString(5, "a");
            ps.setInt(6, 2);
            ps.setString(7, "2021-04-14 16:55:25");
            ps.setString(8, "a");
            ps.setInt(9, 3);
            ps.setString(10, "2021-04-14 16:55:25");
            ps.setInt(11, 0);
            ps.setString(12, "a");
            ps.setInt(13, 4);
            ps.setString(14, "2021-04-14 16:55:25");

            ps.executeUpdate();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " where integer_test=2");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     * Call seq-of-target-table.nextVal explicitly
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest5() throws Exception {

        // Check last_insert_id for INSERT SELECT
        final String sql = MessageFormat.format(
            "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(AUTO_SEQ_" + baseOneTableName
                + ".NEXTVAL, ?, ?, ?), (?, ?, ?, ?), (AUTO_SEQ_" + baseOneTableName
                + ".NEXTVAL, ?, ?, ?), (?, ?, ?, ?)", baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            ps.setString(1, "a");
            ps.setInt(2, 1);
            ps.setString(3, "2021-04-14 16:55:25");
            ps.setNull(4, Types.INTEGER);
            ps.setString(5, "a");
            ps.setInt(6, 2);
            ps.setString(7, "2021-04-14 16:55:25");
            ps.setString(8, "a");
            ps.setInt(9, 3);
            ps.setString(10, "2021-04-14 16:55:25");
            ps.setInt(11, 0);
            ps.setString(12, "a");
            ps.setInt(13, 4);
            ps.setString(14, "2021-04-14 16:55:25");

            ps.executeUpdate();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs = JdbcUtil
            .executeQuerySuccess(tddlConnection, "select pk from " + baseOneTableName + " order by integer_test");
        final List<String> pkList = new ArrayList<>();
        while (rs.next()) {
            pkList.add(rs.getString(1));
        }

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test multi value with expression and partial primary key specified
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest6() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(1, ''a'', 1, NOW()), (2, ''b'', 2, NOW()), (3, ''c'', 3, NOW()), (4, ''b'', 4, NOW())",
                baseOneTableName);
        final Statement statement = JdbcUtil.createStatement(tddlConnection);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try {
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            final ResultSet rs = statement.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        } finally {
            JdbcUtil.close(statement);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " order by pk desc limit 1");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest7() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(?, ?, ?, ?)",
                baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            for (int i = 1; i < 5; i++) {
                ps.setInt(1, i);
                ps.setString(2, "a");
                ps.setInt(3, i);
                ps.setString(4, "2021-04-14 16:55:25");

                ps.addBatch();
            }

            ps.executeBatch();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " order by pk desc limit 1");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest8() throws Exception {

        // Check last_insert_id for INSERT SELECT
        String sql = MessageFormat
            .format(
                "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(?, ?, ?, ?)",
                baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            for (int i = 1; i < 5; i++) {
                ps.setInt(1, i);
                ps.setString(2, "a");
                ps.setInt(3, i);
                ps.setString(4, "2021-04-14 16:55:25");

                ps.addBatch();
            }

            ps.executeBatch();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection,
                "select pk from " + baseOneTableName + " order by pk desc limit 1");
        Assert.assertTrue("No record inserted! ", rs.next());

        Long autoGenerated = rs.getLong(1);

        final List<String> pkList = new ArrayList<>();
        pkList.add(String.valueOf(autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));
        pkList.add(String.valueOf(++autoGenerated));

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    /**
     * Test batch insert with preparedStatement
     * Call seq-of-target-table.nextVal explicitly
     */
    @Test
    public void insertMultiValueAutoGeneratedKeyTest9() throws Exception {

        // Check last_insert_id for INSERT SELECT
        final String sql = MessageFormat.format(
            "insert into {0} (pk, varchar_test, integer_test, datetime_test) values(AUTO_SEQ_" + baseOneTableName
                + ".NEXTVAL, ?, AUTO_SEQ_" + baseOneTableName + ".NEXTVAL, ?), (?, ?, AUTO_SEQ_" + baseOneTableName
                + ".NEXTVAL, ?), (AUTO_SEQ_" + baseOneTableName + ".NEXTVAL, ?, AUTO_SEQ_" + baseOneTableName
                + ".NEXTVAL, ?), (?, ?, AUTO_SEQ_" + baseOneTableName + ".NEXTVAL, ?)", baseOneTableName);

        final List<String> autoGeneratedKey = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "pk")) {
            ps.setString(1, "a");
            ps.setString(2, "2021-04-14 16:55:25");
            ps.setNull(3, Types.INTEGER);
            ps.setString(4, "b");
            ps.setString(5, "2021-04-14 16:55:25");
            ps.setString(6, "c");
            ps.setString(7, "2021-04-14 16:55:25");
            ps.setInt(8, 0);
            ps.setString(9, "d");
            ps.setString(10, "2021-04-14 16:55:25");

            ps.executeUpdate();

            final ResultSet rs = ps.getGeneratedKeys();
            Assert.assertTrue("get generated keys null", rs.next());

            do {
                autoGeneratedKey.add(rs.getString(1));
            } while (rs.next());
        } catch (Exception e) {
            String errorMs = "[statement getGeneratedKeys] failed";
            log.error(errorMs, e);
            Assert.fail(errorMs + " \n " + e);
        }

        final ResultSet rs = JdbcUtil
            .executeQuerySuccess(tddlConnection, "select pk from " + baseOneTableName + " order by varchar_test");
        final List<String> pkList = new ArrayList<>();
        while (rs.next()) {
            pkList.add(rs.getString(1));
        }

        assertWithMessage("AutoGeneratedKey 与 pk 实际取值不一致").that(autoGeneratedKey).containsExactlyElementsIn(pkList);
//System.out.println(String.join(",", pkList));
    }

    private static class SequenceSqlRunner extends Thread {

        private String sql;
        private String sqlMode;
        private int round;

        SequenceSqlRunner(String sql, String sqlMode, int round) {
            this.sql = sql;
            this.sqlMode = sqlMode;
            this.round = round;
        }

        @Override
        public void run() {
            Connection connection = null;
            try {
                connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
                JdbcUtil.useDb(connection, polardbXAutoDBName1());
                String sqlModeString = String.format("set session sql_mode='%s'", sqlMode);
                JdbcUtil.executeSuccess(connection, sqlModeString);

                for (int i = 0; i < round; i++) {
                    Statement st = connection.createStatement();
                    st.execute(sql, Statement.RETURN_GENERATED_KEYS);
                    ResultSet rs = st.getGeneratedKeys();
                    checkResult(rs);
                    st.close();
                    rs.close();
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                if (connection != null) {
                    try {
                        JdbcUtil.executeSuccess(connection, "set session sql_mode=''");
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            connection.close();
                        } catch (SQLException t) {
                            t.printStackTrace();
                        }
                    }
                }
            }
        }

        private void checkResult(ResultSet rs) throws Exception {
            if (sqlMode.isEmpty()) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getLong(1) > 0);
            } else {
                Assert.assertTrue(!rs.next());
            }
        }
    }
}
