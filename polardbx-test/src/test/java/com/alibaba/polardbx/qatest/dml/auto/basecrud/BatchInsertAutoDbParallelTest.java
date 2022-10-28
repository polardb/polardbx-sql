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

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnConn;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class BatchInsertAutoDbParallelTest extends AutoCrudBasedLockTestCase {

    private static int INSERT_NUM = 100000;
    private static String targetTbl = "update_delete_base_multi_tb";

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        String[][] object = {
            //{targetTbl},
        };
        return Arrays.asList(object);
    }

    public BatchInsertAutoDbParallelTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseOneTableName + "2";
    }

    @Before
    public void initData() throws Exception {
        clearDataOnMysqlAndTddl();
    }

    /**
     * Insert all fields and test if it's the same in tddl and mysql. Test 3
     * times: num = INSERT_NUM - 1 / INSERT_NUM / INSERT_NUM + 1
     */
    @Test
    public void insertBatchAllFields() throws Exception {

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

        List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < INSERT_NUM - 2; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            params.add(param);
        }

        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
        // INSERT_NUM - 1, INSERT_NUM, INSERT_NUM + 1
        for (int i = 0; i < 3; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, INSERT_NUM - 2 + i);
            params.add(param);

            clearDataOnMysqlAndTddl();
            //executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

            long st = System.nanoTime();
            executeBatchOnConn(tddlConnection, sql, params);
            long et = System.nanoTime();
            System.out.println("rt:" + (et-st)/1000000);

            //randomTestData("*", INSERT_NUM - 1 + i);
            testCount(INSERT_NUM - 1 + i);
        }
    }

//    /**
//     * Insert varchar with escape characters to see if it's the same in tddl and
//     * mysql.
//     */
//    @Test
//    public void insertBatchWithEscape() throws Exception {
//
//        StringBuilder sqlBuilder = new StringBuilder("insert into ").append(baseOneTableName)
//            .append(" (pk,varchar_test,mediumtext_test) values");
//        for (int i = 0; i < INSERT_NUM; i++) {
//            sqlBuilder.append("(").append(i).append(",");
//            sqlBuilder.append("'\\'abcdef\\ghi\\t'").append(",");
//            sqlBuilder.append("\"abc\\\"defghi\\n\"").append(")");
//            if (i < INSERT_NUM - 1) {
//                sqlBuilder.append(",");
//            }
//        }
//
//        clearDataOnMysqlAndTddl();
//
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sqlBuilder.toString(), new ArrayList<>());
//
//        randomTestData("*", INSERT_NUM);
//        testCount(INSERT_NUM);
//    }
//
//    /**
//     * Insert all fields with IGNORE and test if it's the same in tddl and
//     * mysql.
//     */
//    @Test
//    public void insertBatchIgnore() throws Exception {
//
//        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
//        String sql = "insert ignore into " + baseOneTableName + " (";
//        String values = " values ( ";
//
//        for (int j = 0; j < columns.size(); j++) {
//            String columnName = columns.get(j).getName();
//            sql = sql + columnName + ",";
//            values = values + " ?,";
//        }
//
//        sql = sql.substring(0, sql.length() - 1) + ") ";
//        values = values.substring(0, values.length() - 1) + ")";
//
//        sql = sql + values;
//        List<List<Object>> params = new ArrayList<>();
//        for (int i = 0; i < INSERT_NUM; i++) {
//            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
//            params.add(param);
//        }
//
//        clearDataOnMysqlAndTddl();
//        JdbcUtil.updateDataBatch(mysqlConnection, sql, params);
//
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
//        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
//
//        randomTestData("*", INSERT_NUM);
//        testCount(INSERT_NUM);
//    }
//
//    /**
//     * Insert some fields with functions and test if it's the same in tddl and
//     * mysql.
//     */
//    @Test
//    public void insertBatchWithFunction() throws Exception {
//
//        String sql = "insert into " + baseOneTableName
//            + " (pk,date_test,integer_test,varchar_test) values(?,now(),?+?,concat('test', ?))";
//        List<List<Object>> params = new ArrayList<>();
//        for (int i = 0; i < INSERT_NUM; i++) {
//            List<Object> param = new ArrayList<>();
//            String numeric = RandomStringUtils.randomNumeric(8);
//            param.add(i);
//            param.add(Long.valueOf(numeric));
//            param.add(1);
//            param.add(numeric);
//            params.add(param);
//        }
//
//        clearDataOnMysqlAndTddl();
//
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
//
//        randomTestData("integer_test, varchar_test", INSERT_NUM);
//        testCount(INSERT_NUM);
//    }
//
//    /**
//     * Insert some fields with 'on duplicate key update' and test if it's the
//     * same in tddl and mysql.
//     */
//    @Test
//    public void insertBatchOnDuplicateKey() throws Exception {
//        clearDataOnMysqlAndTddl();
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//
//        String sql = "insert into " + baseOneTableName
//            + " (pk,date_test,integer_test,varchar_test) values(?,now(),?,?) "
//            + "on duplicate key update integer_test=values(integer_test), varchar_test='vt'";
//        List<List<Object>> params = new ArrayList<>();
//        for (int i = 0; i < INSERT_NUM; i++) {
//            List<Object> param = new ArrayList<>();
//            String numeric = RandomStringUtils.randomNumeric(8);
//            param.add(i);
//            param.add(Long.valueOf(numeric));
//            param.add(numeric);
//            params.add(param);
//        }
//        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
//        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
//
//        randomTestData("integer_test, varchar_test", INSERT_NUM);
//        testCount(INSERT_NUM);
//    }
//
//    /**
//     * Insert some fields with hint and test if the hint works. Note that can
//     * only use multi values sql instead of executeBatch. Otherwise JDBC won't
//     * rewrite the statement, and send multi statements to server.
//     */
//    @Test
//    public void insertBatchWithHint() throws Exception {
//
//        String hint = "/*+TDDL:scan()*/";
//        if (baseOneTableName.endsWith("one_db_one_tb")) {
//            return;
//        } else if (!baseOneTableName.endsWith("broadcast")) {
//            hint = "/*+TDDL:scan('" + baseOneTableName + "')*/";
//        }
//
//        int insertNum = INSERT_NUM / 10;
//
//        StringBuilder sb = new StringBuilder("/!+TDDL:dummy()*/ /*xxx*/insert " + hint + " into " + baseOneTableName
//            + " (pk,date_test,integer_test,varchar_test) values");
//        for (int i = 0; i < insertNum; i++) {
//            sb.append("(");
//            sb.append(i);
//            sb.append(",now(),");
//            String numeric = RandomStringUtils.randomNumeric(8);
//            sb.append(numeric);
//            sb.append(",'");
//            sb.append(numeric);
//            sb.append("')");
//            if (i != insertNum - 1) {
//                sb.append(",");
//            }
//        }
//
//        clearDataOnTddl();
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//
//        JdbcUtil.updateData(tddlConnection, sb.toString(), new ArrayList<>());
//
//        String selectSql = hint + "select count(1) from " + baseOneTableName;
//        ResultSet rs = JdbcUtil.executeQuery(selectSql, tddlConnection);
//        rs.next();
//        Assert.assertTrue(rs.getLong(1) == insertNum);
//        rs.close();
//    }
//
//    /**
//     * Insert multiple insert statements and test if the count is right. It will
//     * fallback to normal schedule and the sql won't be split actually.
//     */
//    @Test
//    public void insertMultiStatement() throws Exception {
//        // Because this case is very slow, just test one table.
//        if (!baseOneTableName.endsWith("multi_db_multi_tb")) {
//            return;
//        }
//
//        String prefix = "insert into " + baseOneTableName + " (pk,integer_test) values(";
//        StringBuilder sqlBuilder = new StringBuilder();
//        for (int i = 0; i < INSERT_NUM; i++) {
//            sqlBuilder.append(prefix);
//            sqlBuilder.append(i);
//            sqlBuilder.append(",");
//            sqlBuilder.append(RandomStringUtils.randomNumeric(8));
//            sqlBuilder.append(");");
//        }
//        String sql = sqlBuilder.toString();
//
//        // fall back to normal schedule.
//        clearDataOnTddl();
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//
//        JdbcUtil.executeUpdate(tddlConnection, sql);
//
//        testCount(INSERT_NUM);
//    }
//
//    /**
//     * In SPLIT mode, if values num is smaller than 65532 or the sequence range,
//     * sequence is guaranteed to be consecutive. If values num is greater than
//     * 65532, the sql will be split into multiple sqls.
//     */
//    @Test
//    @Ignore
//    public void insertBatchLastInsertIdTest() throws Exception {
//        if (!baseOneTableName.contains("one_db_one_tb")) {
//            return;
//        }
//
//        final int BATCH_SIZE = 20000;
//
//        // Only SPLIT guarantees last insert id
//        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
//
//        String sql = "insert into " + baseTwoTableName + " (varchar_test) values(?)";
//        List<List<Object>> params = new ArrayList<>();
//        for (int i = 0; i < BATCH_SIZE; i++) {
//            List<Object> p = new ArrayList<>();
//            p.add(columnDataGenerator.varchar_testValue);
//            params.add(p);
//        }
//        List<Long> returnedKeys = new ArrayList<>();
//        JdbcUtil.updateDataBatchReturnKeys(tddlConnection, sql, params, returnedKeys);
//
//        sql = "select min(pk), max(pk) from " + baseTwoTableName;
//        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, new ArrayList<>(), tddlConnection);
//        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
//        rs.next();
//        long minPK = rs.getLong(1);
//        long maxPK = rs.getLong(2);
//        rs.close();
//        tddlPs.close();
//
//        sql = "select last_insert_id()";
//        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
//        rs = JdbcUtil.executeQuery(sql, tddlPs);
//        rs.next();
//        long lastInsertId = rs.getLong(1);
//        rs.close();
//        tddlPs.close();
//
//        // If it's a multiple statement, it'll be the last id
//        if (PropertiesUtil.getConnectionProperties().contains("rewriteBatchedStatements=false")) {
//            org.junit.Assert.assertEquals(maxPK, lastInsertId);
//        } else {
//            org.junit.Assert.assertEquals(minPK, lastInsertId);
//        }
//
//        org.junit.Assert.assertEquals(maxPK - minPK, BATCH_SIZE - 1);
//        sql = "select pk from " + baseTwoTableName;
//        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
//        rs = JdbcUtil.executeQuery(sql, tddlPs);
//        List<Long> actualKeys = new ArrayList<>();
//        while (rs.next()) {
//            actualKeys.add(rs.getLong(1));
//        }
//        rs.close();
//        tddlPs.close();
//
//        assertWithMessage("generated keys wrong").that(actualKeys).containsExactlyElementsIn(returnedKeys);
//    }
//
//    /**
//     * Test if connection properties work. Using 5.2 hint to simulate connection
//     * property. 5.2 hint works before optimization, while 5.3 hint works in
//     * optimization. SPLIT module runs before optimization, so we can only use
//     * 5.2 hint.
//     */
//    @Test
//    public void batchInsertTestConnectionProperty() throws Exception {
//        if (!baseOneTableName.contains("one_db_one_tb")) {
//            return;
//        }
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("insert into ");
//        sb.append(baseOneTableName);
//        sb.append(" (pk,date_test,integer_test,varchar_test) values");
//
//        int valuesNum = 2;
//        for (int i = 0; i < valuesNum; i++) {
//            sb.append("(");
//            sb.append(i);
//            sb.append(",now(),");
//            String numeric = RandomStringUtils.randomNumeric(8);
//            sb.append(numeric);
//            sb.append(",'");
//            sb.append(numeric);
//            sb.append("'");
//            // add an extra comma in the last value list to make it fail.
//            if (i == valuesNum - 1) {
//                sb.append(",");
//            }
//            sb.append(")");
//            if (i != valuesNum - 1) {
//                sb.append(",");
//            }
//        }
//
//        // Create a new connection, which is not set batch insert policy before.
//        Connection newConnection = getPolardbxDirectConnection();
//        useDb(newConnection, polardbxOneDB);
//
//        // Open SPLIT mode and split each value into one statement. Only the
//        // last statement will fail.
//        String hint = "/* TDDL BATCH_INSERT_POLICY=split,MAX_BATCH_INSERT_SQL_LENGTH=0,BATCH_INSERT_CHUNK_SIZE=1*/";
//        String sql = hint + sb.toString();
//        clearDataOnTddl();
//        executeErrorAssert(newConnection, sql, null, "pos 213");
//
//        // Close SPLIT mode. The whole statement fails.
//        hint = "/* TDDL BATCH_INSERT_POLICY=none,MAX_BATCH_INSERT_SQL_LENGTH=0,BATCH_INSERT_CHUNK_SIZE=1*/";
//        sql = hint + sb.toString();
//        executeErrorAssert(newConnection, sql, null, "pos 242");
//
//        // Setting session variable should have higher priority than setting
//        // connection property.
//        sql = "set batch_insert_policy='none'";
//        JdbcUtil.executeSuccess(newConnection, sql);
//        hint = "/* TDDL BATCH_INSERT_POLICY=split,MAX_BATCH_INSERT_SQL_LENGTH=0,BATCH_INSERT_CHUNK_SIZE=1*/";
//        sql = hint + sb.toString();
//        executeErrorAssert(newConnection, sql, null, "pos 243");
//
//        newConnection.close();
//    }
//
//    /**
//     * multi threads test for SPLIT. Note that the pressure can't be too high,
//     * otherwise the client will fail. 3 threads, 3 rounds for each thread,
//     * 200000 records for each round. Check the total count at the end.
//     */
//    @Test
//    public void batchInsertMultiThreads() throws Exception {
//        // Because this case is very slow, just test one table.
//        if (!baseOneTableName.endsWith("multi_db_multi_tb")) {
//            return;
//        }
//
//        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
//        String sql = "insert into " + baseOneTableName + " (";
//        String values = " values ( ";
//
//        for (int j = 0; j < columns.size(); j++) {
//            String columnName = columns.get(j).getName();
//            sql = sql + columnName + ",";
//            values = values + " ?,";
//        }
//
//        sql = sql.substring(0, sql.length() - 1) + ") ";
//        values = values.substring(0, values.length() - 1) + ")";
//        sql = sql + values;
//
//        final int step = 5000;
//        final int round = 3;
//        final int threadNum = 3;
//
//        clearDataOnTddl();
//
//        Thread[] threads = new Thread[threadNum];
//        for (int i = 0; i < threadNum; i++) {
//            int pkStart = i * round * step + 1;
//            Thread runner = new BatchSqlRunner(sql, round, pkStart, step, BatchInsertPolicy.SPLIT);
//            runner.start();
//            threads[i] = runner;
//        }
//
//        for (int i = 0; i < threadNum; i++) {
//            threads[i].join();
//        }
//
//        testCount(threadNum * round * step);
//    }
//
//    private class BatchSqlRunner extends Thread {
//
//        private String sql;
//        private int pkStart;
//        private int pkStep;
//        private int round;
//        private BatchInsertPolicy policy;
//        private ColumnDataGenerator columnDataGenerator;
//
//        BatchSqlRunner(String sql, int round, int pkStart, int pkStep, BatchInsertPolicy policy) {
//            this.sql = sql;
//            this.pkStart = pkStart;
//            this.pkStep = pkStep;
//            this.round = round;
//            this.policy = policy;
//            this.columnDataGenerator = new ColumnDataGenerator();
//        }
//
//        @Override
//        public void run() {
//            List<List<Object>> paramsTemplate = getParamsTemplate();
//
//            Connection connection = null;
//            try {
//                connection = getPolardbxConnection();
//                setBatchInsertPolicy(connection);
//
//                for (int i = 0; i < round; i++) {
//                    setNextStepParams(paramsTemplate);
//                    JdbcUtil.updateDataBatch(connection, sql, paramsTemplate);
//                }
//            } catch (Exception e) {
//                throw GeneralUtil.nestedException(e);
//            } finally {
//                if (connection != null) {
//                    try {
//                        connection.close();
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//
//        private List<List<Object>> getParamsTemplate() {
//            List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
//            List<List<Object>> params = new ArrayList<>();
//            for (int i = 0; i < pkStep; i++) {
//                List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
//                params.add(param);
//            }
//            return params;
//        }
//
//        private void setNextStepParams(List<List<Object>> paramsTemplate) {
//            for (int i = 0; i < pkStep; i++) {
//                paramsTemplate.get(i).set(0, pkStart + i);
//            }
//            pkStart += pkStep;
//        }
//
//        private void setBatchInsertPolicy(Connection conn) {
//            String sql = String.format("set batch_insert_policy='%s'", policy.getName());
//            JdbcUtil.executeSuccess(conn, sql);
//        }
//    }
//
    private void setBatchInsertPolicy(BatchInsertPolicy policy) {
        String sql = String.format("set batch_insert_policy='%s'", policy.getName());
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    private void clearDataOnMysqlAndTddl() {
        String sql = "delete from  " + baseOneTableName;

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    private void clearDataOnTddl() {

        String sql = "delete from  " + baseOneTableName;

        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    // random check 100 values to test if it's the same in tddl and mysql.
    private void randomTestData(String fields, int maxNum) {
        for (int i = 0; i < 100; i++) {
            int r = (int) (Math.random() * maxNum);
            String sql = String.format("select %s from %s where %s=%d", fields, baseOneTableName, PK_COLUMN_NAME, r);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    // test total num of records.
    private void testCount(int totalNum) throws Exception {
        String sql = "select count(1) from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getLong(1) == totalNum);
    }

//    @Test
//    public void testBatchInser() {
//        //String sql = "";
//        String sql = "insert into " + baseOneTableName
//            + " (pk, integer_test, varchar_test, char_test, blob_test, tinyint_test, tinyint_1bit_test,smallint_test, mediumint_test, bit_test, bigint_test, float_test, double_test, decimal_test, date_test, time_test, datetime_test, timestamp_test, year_test, mediumtext_test) values (?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?,?, ?) ";
//        List<List<Object>> params = new ArrayList<>();
//        for (int i = 0; i < 2520; i++) {
//            List<Object> param = new ArrayList<>();
//            param.add(String.valueOf(i));
//            param.add(String.valueOf(i));
//            param.add(null);
//            param.add(null);
//            param.add(null);
//
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(null);
//
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(null);
//
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(null);
//            param.add(String.valueOf(i));
//            params.add(param);
//        }
//        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
//    }
}
