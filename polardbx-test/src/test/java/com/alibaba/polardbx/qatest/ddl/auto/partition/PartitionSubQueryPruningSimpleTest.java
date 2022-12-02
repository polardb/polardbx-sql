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
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.DISABLE_AUTO_PART;
import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.ENABLE_AUTO_PART;

/**
 * @author chenghui.lch
 */
public class PartitionSubQueryPruningSimpleTest extends PartitionTestBase {

    protected static final Log log = LogFactory.getLog(PartitionSubQueryPruningSimpleTest.class);

    protected static String testTableName = "sb_pruning_test";
    protected static String targetTableNameAlias = "`sb_pruning_test`";
    protected static String dropTblSql = "drop table if exists " + testTableName + ";";

    protected static String traceSqlTemplate = "trace %s";
    protected static String showTraceSql = "show trace";

    protected static List<String> allCols = new ArrayList<>();
    protected static List<String> partCols = new ArrayList<>();
    protected static List<Integer> colRangeBase = new ArrayList<>();
    protected static List<String> subQueryExprSampleList = new ArrayList<>();
    protected static List<Boolean> onePartitionLeftAfterPruning = new ArrayList<>();

    protected static String[] tbNames = new String[] {
        testTableName,
        testTableName + "1",
        testTableName + "2",
        testTableName + "3"
    };

    static {
        allCols.add("a");
        allCols.add("b");
        allCols.add("c");
        allCols.add("d");
        allCols.add("e");

        partCols.add("a");
        partCols.add("b");
        partCols.add("c");

        colRangeBase.add(100);
        colRangeBase.add(1000);
        colRangeBase.add(10000);
        colRangeBase.add(10000);

        /**
         * subquery complex Expr for Comparison
         */
        // some fixed subquery values are as followed:
        // (select t1.a from sb_pruning_test1 t1 order by a limit 8,1)=98
        // (select t1.a from sb_pruning_test1 t1 order by a limit 10,1)=190
        subQueryExprSampleList.add(String.format("a = (select t1.a from %s t1 order by a limit 8,1)", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.TRUE);

        subQueryExprSampleList.add(String.format("a = (select t1.a from %s t1 order by a limit 8,1) or a=97", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.TRUE);

        subQueryExprSampleList.add(String.format("a = (select t1.a from sb_pruning_test1 t1 order by a limit 8,1) and b between 10 and 2000", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.TRUE);

        subQueryExprSampleList.add(String.format("a = 98 and b=(select t1.a from %s t1 order by a limit 10,1)", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.TRUE);

        subQueryExprSampleList.add(String.format("a = 98 and (b=(select t1.a from %s t1 order by a limit 10,1) or b=908 )", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.TRUE);

//        subQueryExprSampleList.add(String.format("a in (select t1.a from %s t1 order by a limit 10,1)", tbNames[1]));
//        onePartitionAfterPruning.add(Boolean.TRUE);

        subQueryExprSampleList.add(String.format("b = (select t1.b from %s t1 order by b limit 2,1)", tbNames[1]));
        onePartitionLeftAfterPruning.add(Boolean.FALSE);

        subQueryExprSampleList.add(String.format("a = (select t1.a from %s t1 where t1.b=t.b order by t1.a limit 3,1)", tbNames[2]));
        onePartitionLeftAfterPruning.add(Boolean.FALSE);

        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b=t.b order by t1.c limit 4,1)", tbNames[2]));
        onePartitionLeftAfterPruning.add(Boolean.FALSE);

        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b>t.b order by t1.c limit 5,1)", tbNames[3]));
        onePartitionLeftAfterPruning.add(Boolean.FALSE);

        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b>t.b order by t1.c limit 5,1)", tbNames[3]));
        onePartitionLeftAfterPruning.add(Boolean.FALSE);


    }

    public PartitionSubQueryPruningSimpleTest() {
        super();
    }

    @Before
    public void setUpEnv() {
        try {

            for (int i = 0; i < tbNames.length; i++) {
                String createTbl = initCreateTableSql(tbNames[i]);
                String prepareDataSql = prepareDataSql(tbNames[i]);
                String dropTbl = prepareDropTableSql(tbNames[i]);

                dropTable(tddlConnection, dropTbl);
                createTable(tddlConnection, createTbl);
                prepareData(tddlConnection, prepareDataSql);

                dropTable(mysqlConnection, dropTbl);
                createTable(mysqlConnection, createTbl);
                prepareData(mysqlConnection, prepareDataSql);
            }

        } catch (Throwable ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        }
    }

    @After
    public void setDownEnv() {
        try {
            dropTable(tddlConnection, dropTblSql);
            dropTable(mysqlConnection, dropTblSql);
        } catch (Throwable ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        } finally {
            JdbcUtil.updateDataTddl(tddlConnection, ENABLE_AUTO_PART, null);
        }
    }

    @Test
    public void runTest() {
        int sqlCnt = subQueryExprSampleList.size();
        for (int i = 0; i < sqlCnt; i++) {

            String sql = genTestSql(false, i);
            Boolean isOnePart = onePartitionLeftAfterPruning.get(i);
            String logMsg = String.format("rngSql[%s]: \n%s;\n\n", i, sql);
            log.info(logMsg);
            try {
                runTestSql(sql,isOnePart);
            } catch (Throwable ex) {
                Assert.fail(ex.getMessage());
            }

        }
    }

    protected void runTestSql(String rndSql, boolean isOnePart) throws Throwable  {

        ResultSet rs = null;
        DataValidator dataValidator = new DataValidator();
        int cnt = 0;
        try {
            dataValidator.selectContentSameAssert(rndSql, new ArrayList<>(), tddlConnection, mysqlConnection, true);

            String tracedSql = String.format(traceSqlTemplate, rndSql);
            rs = JdbcUtil.executeQuery(tracedSql, tddlConnection);
            if (rs != null) {
                rs.next();
                rs.close();
            }
            rs = JdbcUtil.executeQuery(showTraceSql, tddlConnection);
            List<Pair<String, String>> phySqlInfos = new ArrayList<>();
            if (rs != null) {
                while (rs.next()) {
                    String dbKey = rs.getString("DBKEY_NAME");
                    String stmt = rs.getString("STATEMENT");
                    phySqlInfos.add(new Pair<>(dbKey, stmt));
                }
                rs.close();
            }
            for (int i = 0; i < phySqlInfos.size(); i++) {
                String stmtStr = phySqlInfos.get(i).getValue();
                if (stmtStr.contains(targetTableNameAlias)) {
                    cnt++;
                }
            }
            Assert.assertEquals(cnt == 1, isOnePart);
        } catch (Throwable ex) {
            log.error(ex);
            throw ex;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Throwable ex) {
            }
        }
    }

    protected String genTestSql(boolean isReverse, int predIdx) {
        String rndSqlPred = genSingleExpr(predIdx);
        String rndSql = "";
        if (!isReverse) {
            rndSql = String.format("select a,b,c,d from %s t where (%s) order by a,b,c", testTableName, rndSqlPred);
        } else {
            rndSql = String.format("select a,b,c,d from %s t where !(%s) order by a,b,c", testTableName, rndSqlPred);
        }

        return rndSql;
    }

    protected String genSingleExpr(int predIdx ) {
        String predExpr = subQueryExprSampleList.get(predIdx);
        return predExpr;
    }

    protected void createTable(Connection conn, String createTbl) {
        String disableAutoPartSql = DISABLE_AUTO_PART;
        JdbcUtil.updateDataTddl(conn, disableAutoPartSql, null);
        JdbcUtil.updateDataTddl(conn, createTbl, null);
    }

    protected void dropTable(Connection conn, String dropSql) {
        JdbcUtil.updateDataTddl(conn, dropSql, null);
    }

    protected static String prepareDropTableSql(String tbName) {
        return "drop table if exists " + tbName + ";";
    }

    protected static String prepareDataSql(String tbName) {
        StringBuilder valuesSb = new StringBuilder("");
        int dataSize = 100;
        for (int i = 0; i < dataSize; i++) {
            if (valuesSb.length() > 0) {
                valuesSb.append(",");
            }

            int modVal = i % 10;
            int divVal = i / 10;

            int a = (90 + modVal) + divVal * 100;
            int b = (900 + modVal) + divVal * 1000;
            int c = (9000 + modVal) + divVal * 10000;
            int d = (90000 + modVal) + divVal * 100000;
            int e = (900000 + modVal) + divVal * 1000000;
            String valItem = String.format("(%s,%s,%s,%s,%s)", a, b, c, d, e);
            valuesSb.append(valItem);
        }
        String insertDataSql = "insert into " + tbName + " (a,b,c,d,e) values " + valuesSb.toString();
        return insertDataSql;
    }

    public static void prepareData(Connection conn, String insertDataSql) {
        JdbcUtil.updateDataTddl(conn, insertDataSql, null);
    }

    protected static String initCreateTableSql(String tbName) {

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
         d bigint not null,
         primary key(a,b,c)
         )
         partition by range columns(a,b,c)
         ( 
         partition p1 values less than (100,1000,10000),
         partition p2 values less than (200,2000,20000),
         partition p3 values less than (300,3000,30000),
         partition p4 values less than (400,4000,40000),
         partition p5 values less than (500,5000,50000),
         partition p6 values less than (600,6000,60000),
         partition p7 values less than (700,7000,70000),
         partition p8 values less than (800,8000,80000),
         partition p9 values less than (900,9000,90000),
         partition p10 values less than (1000,10000,100000)
         );
         *
         *
         * </pre>
         *
         *
         *
         */

        String tmpSql = "create table if not exists " + tbName + " (\n"
            + "\ta bigint not null, \n"
            + "\tb bigint not null, \n"
            + "\tc bigint not null,\n"
            + "\td bigint not null,\n"
            + "\te bigint not null,\n"
            + "\tprimary key(a,b,c)\n"
            + ")\n"
            + "partition by range columns(a,b,c)\n"
            + "( \n"
            + "  partition p1 values less than (100,1000,10000),\n"
            + "  partition p2 values less than (200,2000,20000),\n"
            + "  partition p3 values less than (300,3000,30000),\n"
            + "  partition p4 values less than (400,4000,40000),\n"
            + "  partition p5 values less than (500,5000,50000),\n"
            + "  partition p6 values less than (600,6000,60000),\n"
            + "  partition p7 values less than (700,7000,70000),\n"
            + "  partition p8 values less than (800,8000,80000),\n"
            + "  partition p9 values less than (900,9000,90000),\n"
            + "  partition p10 values less than (1000,10000,100000)\n"
            + ");\n";

        return tmpSql;
    }
}
