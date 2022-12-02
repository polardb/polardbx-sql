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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.DISABLE_AUTO_PART;
import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.ENABLE_AUTO_PART;

/**
 * @author chenghui.lch
 */
public class PartitionBatchDmlPerlTest extends PartitionTestBase {

    protected static final Log log = LogFactory.getLog(PartitionBatchDmlPerlTest.class);

    protected static String testTableName = "dml_perf_test";
    protected static String dropTblSql = "drop table if exists " + testTableName + ";";
    protected static String truncateTblSql = "truncate table " + testTableName + ";";
    protected static String dataSql = "";
    protected static Long groupParallelism = 32L;
    protected static int dataSizeOneSql = 1000000;
    protected Connection testConn;
    protected String db = "myauto";
    protected boolean isAutoDb = true;
    protected boolean tbExists = false;

    public PartitionBatchDmlPerlTest() {
        super();
    }

    private void initJdbcConn() {
        try {
//            String host = "127.0.0.1";
//            int port = 8527;
//            db = "mydrds";
//            String user = "polardbx_root";
//            String pwd = "123456";
//            String props = "useUnicode=true&characterEncoding=utf8&useSSL=false&connectTimeout=5000&socketTimeout=900000";

//            String host = "pxc-sprmchpczgejv4.public.polarx.singapore.rds.aliyuncs.com";
//            int port = 3306;
//            db = "myauto";
//            String user = "polardbx_root";
//            String pwd = "D4SJ4XAKQoj1";
//            String props = "useUnicode=true&characterEncoding=utf8&useSSL=false&connectTimeout=5000&socketTimeout=9000000";
//
//            String url = com.alibaba.polardbx.gms.util.JdbcUtil.createUrl(host, port, db, props);
//            Connection conn = com.alibaba.polardbx.gms.util.JdbcUtil.createConnection(url, user, pwd);
//            testConn = conn;
        } catch (Throwable ex) {
            log.warn(ex);
        }
    }

    @Before
    public void setUpEnv() {
        try {
            String prepareDataSql = prepareDataSql();
            this.dataSql = prepareDataSql;

            initJdbcConn();
            Connection conn = testConn;
            try {
                ResultSet rs = JdbcUtil.executeQuery("show create database " + db, conn);
                rs.next();
                String showCreateDbStr = rs.getString(2);
                if (showCreateDbStr.toLowerCase().contains("mode = 'drds'")) {
                    isAutoDb = false;
                }
                rs.close();
            } catch (Throwable ex) {
                log.error(ex);
            }

            String createTbl = isAutoDb ? initCreateTableSql() : initCreateTableSqlForDrds();
            createTable(conn, createTbl);
            truncateTable(conn, truncateTblSql);

            //prepareData(conn, prepareDataSql);
            //System.out.println("prepare data finish");
            //dropTable(mysqlConnection, dropTblSql);
            //createTable(mysqlConnection, createTbl);
            //prepareData(mysqlConnection, prepareDataSql);
        } catch (Throwable ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        }
    }

    @After
    public void setDownEnv() {
        Connection conn = testConn;
        try {
            //truncateTable(conn, truncateTblSql);
        } catch (Throwable ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        } finally {
            JdbcUtil.updateDataTddl(conn, ENABLE_AUTO_PART, null);
            if (testConn != null) {
                try {
                    testConn.close();
                } catch (Throwable ex) {
                }
            }
        }
    }

    @Ignore
    public void runTest() {
        int sqlCnt = 1000;
        AtomicLong timeCost = new AtomicLong(0L);
        Connection conn = testConn;
        try {
            JdbcUtil.executeUpdate(conn, String.format("SET GROUP_PARALLELISM=%d", groupParallelism));
            for (int i = 0; i < sqlCnt; i++) {
                String sql = dataSql;
                runTestSql(conn, i, sql, timeCost);

                try {
                    Thread.sleep(2000);
                } catch (Throwable ex) {

                }
            }
        } finally {
            if (conn != null) {
                JdbcUtil.executeUpdate(conn, "SET GROUP_PARALLELISM=2");
            }
        }
    }

    protected void runTestSql(Connection conn, int testNum, String rndSql, AtomicLong totalTimeCost) {
        try {
            JdbcUtil.executeUpdate(conn, "begin");
            Long startTs = System.nanoTime();
            JdbcUtil.executeUpdate(conn, rndSql);
            Long endTs = System.nanoTime();
            JdbcUtil.executeUpdate(conn, "commit");
            if (testNum == 0) {
                return;
            }
            Long cost = endTs - startTs;
            totalTimeCost.addAndGet(cost);
            String logMsg = String.format("%s:oneSqlTimeCost(ms): %d, avgTimeCost(ms):%d\n",  testNum, cost / 1000000, totalTimeCost.get() / (testNum * 1000000));
            log.info(logMsg);
        } catch (Throwable ex) {
            log.error(ex);
            throw ex;
        } finally {
            if (conn != null) {
                JdbcUtil.executeUpdate(conn, "rollback");
            }
        }
    }

    protected void createTable(Connection conn, String createTbl) {
        String disableAutoPartSql = DISABLE_AUTO_PART;
        JdbcUtil.updateDataTddl(conn, disableAutoPartSql, null);
        JdbcUtil.updateDataTddl(conn, createTbl, null);
    }

    protected void dropTable(Connection conn, String dropSql) {
        JdbcUtil.updateDataTddl(conn, dropSql, null);
    }

    protected void truncateTable(Connection conn, String dropSql) {
        JdbcUtil.updateDataTddl(conn, dropSql, null);
    }

    protected static String prepareDataSql() {
        StringBuilder valuesSb = new StringBuilder("");
        int dataSize = dataSizeOneSql;
        for (int i = 0; i < dataSize; i++) {
            if (valuesSb.length() > 0) {
                valuesSb.append(",");
            }

            int modVal = i % 10;
            int divVal = i / 10;

            int a = i + 1;
            int b = i + 1;
            int c = (9000 + modVal) + divVal * 10000;
            int d = 100000 + a;
            int e = 1000000 + a;
            String valItem = String.format("(%s,%s,%s,%s,%s)", a, b, c, d, e);
            valuesSb.append(valItem);
        }
        String insertDataSql = "insert into " + testTableName + " (a,b,c,d,e) values " + valuesSb.toString();
        return insertDataSql;
    }

    public static void prepareData(Connection conn, String insertDataSql) {
        JdbcUtil.updateDataTddl(conn, insertDataSql, null);
    }

    protected static String initCreateTableSqlForDrds() {
        String tmpSql = "create table if not exists " + testTableName + " (\n"
            + "\ta bigint not null PRIMARY KEY, \n"
            + "\tb bigint not null, \n"
            + "\tc bigint not null,\n"
            + "\td bigint not null,\n"
            + "\te bigint not null\n"
            + ")\n"
            + "dbpartition by hash(b) \n"
            + "tbpartition by hash(b) \n"
            + "tbpartitions 64";

        return tmpSql;
    }

    protected static String initCreateTableSql() {

        /**
         *
         * <pre>
         *
         *
         *

         create table if not exists rng_test_tbl (
         a bigint not null, 
         b bigint not null, 
         c bigint not null,
         d bigint not null
         )
         partition by key columns(a)
         partitions 64;
         *
         *
         * </pre>
         *
         *
         *
         */

        String tmpSql = "create table if not exists " + testTableName + " (\n"
            + "\ta bigint not null PRIMARY KEY, \n"
            + "\tb bigint not null, \n"
            + "\tc bigint not null,\n"
            + "\td bigint not null,\n"
            + "\te bigint not null\n"
            + ")\n"
            + "partition by key(b) \n"
            + "partitions 64";

        return tmpSql;
    }
}
