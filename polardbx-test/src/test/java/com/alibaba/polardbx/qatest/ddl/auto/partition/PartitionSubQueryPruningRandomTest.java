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
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.DISABLE_AUTO_PART;
import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.ENABLE_AUTO_PART;

/**
 * @author chenghui.lch
 */
public class PartitionSubQueryPruningRandomTest extends PartitionTestBase {

    protected static final Log log = LogFactory.getLog(PartitionSubQueryPruningRandomTest.class);

    protected static String testTableName = "random_pruning_test";
    protected static String dropTblSql = "drop table if exists " + testTableName + ";";

    protected static List<String> allCols = new ArrayList<>();
    protected static List<String> partCols = new ArrayList<>();
    protected static List<Integer> colRangeBase = new ArrayList<>();

    protected static List<SqlKind> logicExprKinds = new ArrayList<>();
    protected static List<SqlKind> cmpExprKinds = new ArrayList<>();
    protected static List<SqlKind> complexExprKinds = new ArrayList<>();

    protected static List<String> complexExprSampleList = new ArrayList<>();
    protected static List<String> subQueryExprSampleList = new ArrayList<>();

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

        logicExprKinds.add(SqlKind.OR);
        logicExprKinds.add(SqlKind.AND);

        cmpExprKinds.add(SqlKind.GREATER_THAN);
        cmpExprKinds.add(SqlKind.GREATER_THAN_OR_EQUAL);
        cmpExprKinds.add(SqlKind.EQUALS);
        cmpExprKinds.add(SqlKind.LESS_THAN_OR_EQUAL);
        cmpExprKinds.add(SqlKind.LESS_THAN);

        complexExprKinds.add(SqlKind.BETWEEN);
        complexExprKinds.add(SqlKind.IN);
        complexExprKinds.add(SqlKind.IS_NULL);

        /**
         * complex Expr for In
         */
        //complexExprSampleList.add("(a,b,e) in ((100,2000,30000),(200,4000,30000))");
        //complexExprSampleList.add("(b,a) in ((1000,300),(2000,400))");
        complexExprSampleList.add("((b=1000 and 100=300) or (b=2000 and a=400))");
        //complexExprSampleList.add("(b,100) in ((1000,300),(a,400))");
        complexExprSampleList.add("((b=1000 and 100=300) or (b=a and 100=400))");

        //complexExprSampleList.add("(a,b,c) in ((400,3000,40000))");
        complexExprSampleList.add("a=400 and b=3000 and c=40000");
        //complexExprSampleList.add("(e,a) not in ((400,900))");
        complexExprSampleList.add("e != 400 and a != 900");
        //complexExprSampleList.add("(d,a) = (400,500)");
        complexExprSampleList.add("d = 400 and a = 500");

        /**
         * complex Expr for between
         */
        complexExprSampleList.add("a between 300 and 600");
        complexExprSampleList.add("b between 7000 and 8000");
        complexExprSampleList.add("c between 50000 and 60000");
        complexExprSampleList.add("300 between a and b");
        complexExprSampleList.add("b between e and 4000");

        /**
         * complex Expr for Comparison
         */
        //complexExprSampleList.add("(a,b,c) = (400,4000,40000)");
        complexExprSampleList.add("a=400 and b=4000 and c=40000");
        //complexExprSampleList.add("(b,c) < (3000, 40000)");
        //complexExprSampleList.add("(a,c) >= (300, 40000)");
        complexExprSampleList.add("(a > 300 or (a=300 and c>40000) or (a=300 and c=40000) )");
        //complexExprSampleList.add("(a,b) != (400, 3000)");
        complexExprSampleList.add("a != 400 and b!=3000");

        /**
         * complex Expr for Comparison
         */
        complexExprSampleList.add("a is null");
        complexExprSampleList.add("d is null");

        /**
         * subquery complex Expr for Comparison
         */
        subQueryExprSampleList.add(String.format("a = (select t1.a from %s t1 order by a limit 10,1)", tbNames[0]));
        subQueryExprSampleList.add(String.format("b = (select t1.b from %s t1 order by b limit 2,1)", tbNames[1]));
        subQueryExprSampleList.add(String.format("c >= (select t1.c from %s t1 order by c limit 3,1)", tbNames[2]));
        subQueryExprSampleList.add(String.format("c < (select t1.c from %s t1 order by c limit 6,1)", tbNames[2]));
        subQueryExprSampleList.add(String.format("b != (select t1.b from %s t1 order by b limit 3,1)", tbNames[2]));

        subQueryExprSampleList.add(String.format("a = (select t1.a from %s t1 where t1.b=t.b order by t1.a limit 3,1)", tbNames[2]));
        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b=t.b order by t1.c limit 4,1)", tbNames[2]));
        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b>t.b order by t1.c limit 5,1)", tbNames[2]));
        subQueryExprSampleList.add(String.format("c = (select t1.c from %s t1 where t1.b>t.b order by t1.c limit 5,1)", tbNames[2]));

        subQueryExprSampleList.add(String.format("a in (select t1.a from %s t1 where t1.a<100)", tbNames[1]));
        subQueryExprSampleList.add(String.format("b in (select t1.b from %s t1 where t1.b>1000)", tbNames[1]));

    }

    protected static class GenRandomCtx {
        protected int maxPredDepth = 4;
        protected int maxPredWidth = 4;
        protected int opPredCount = 12;
        protected AtomicInteger currOpPredCount = new AtomicInteger(0);

    }

    public PartitionSubQueryPruningRandomTest() {
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
        int sqlCnt = 250;
        for (int i = 0; i < sqlCnt; i++) {
            String sql = genRandomSql(false);
            String logMsg = String.format("rngSql[%s]: \n%s;\n\n", i, sql);
            log.info(logMsg);
            runRandomSql(sql);

            String inverseSql = genRandomSql(true);
            logMsg = String.format("inversedRngSql[%s]: \n%s;\n\n", i, inverseSql);
            log.info(logMsg);
            runRandomSql(inverseSql);
        }
    }

    protected void runRandomSql(String rndSql) {

        ResultSet rs = null;
        DataValidator dataValidator = new DataValidator();
        try {
            dataValidator.selectContentSameAssert(rndSql, new ArrayList<>(), tddlConnection, mysqlConnection, true);
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

    protected String genRandomSql(boolean isReverse) {
        GenRandomCtx ctx = new GenRandomCtx();
        String rndSqlPred = genRandPredExpr(0, ctx);
        String rndSql = "";
        if (!isReverse) {
            rndSql = String.format("select a,b,c,d from %s t where (%s) order by a,b,c", testTableName, rndSqlPred);
        } else {
            rndSql = String.format("select a,b,c,d from %s t where !(%s) order by a,b,c", testTableName, rndSqlPred);
        }

        return rndSql;
    }

    protected String genRandPredExpr(int currDepth, GenRandomCtx randomCtx) {
        return genRandAndOrExpr(0, randomCtx);
    }

    protected static Random rnd = new Random(System.currentTimeMillis());

    protected static int randomInt(int modVal) {
        int rs = Math.abs(rnd.nextInt()) % modVal;
        return rs;
    }

    protected String genRandAndOrExpr(int currDepth, GenRandomCtx randomCtx) {

        boolean allowGenAndOrExpr = currDepth < randomCtx.maxPredDepth;

        List<String> exprList = new ArrayList<>();

        int logOpFlag = randomInt(5);
        String logOpKind = null;
        if (logOpFlag < 2) {
            // OR
            logOpKind = logicExprKinds.get(0).sql;
        } else {
            logOpKind = logicExprKinds.get(1).sql;
        }

        int logExprCnt = randomInt(randomCtx.maxPredWidth);
        if (logExprCnt == 0) {
            logExprCnt = 1;
        }
        int opPredCnt = randomInt(randomCtx.maxPredWidth);
        if (opPredCnt == 0) {
            opPredCnt = 1;
        }

        if (allowGenAndOrExpr) {
            for (int i = 0; i < logExprCnt; i++) {
                String exprStr = genRandAndOrExpr(currDepth + 1, randomCtx);
                if (exprStr != null && exprStr.length() > 0) {
                    exprList.add(exprStr);
                }
            }
        }

        for (int i = 0; i < opPredCnt; i++) {
            String exprStr = genRandOpExpr(randomCtx);
            if (exprStr != null && exprStr.length() > 0) {
                exprList.add(exprStr);
            }
        }

        if (exprList.size() == 1) {
            return exprList.get(0);
        }

        StringBuilder orExprSb = new StringBuilder("");
        for (int i = 0; i < exprList.size(); i++) {
            String expr = exprList.get(i);
            if (orExprSb.length() > 0) {
                orExprSb.append(String.format(" %s (%s)", logOpKind, expr));
            } else {
                orExprSb.append(String.format("(%s)", expr));
            }
        }
        return orExprSb.toString();
    }

    protected String genRandOpExpr(GenRandomCtx randomCtx) {
        if (randomCtx.currOpPredCount.get() >= randomCtx.opPredCount) {
            return null;
        }

        int complexExprFlag = randomInt(10);
        String opExprStr = null;
        if (complexExprFlag < 5) {
            int colIdx = randomInt(4);
            int rngBase = colRangeBase.get(colIdx);
            int rngWidth = randomInt(9) + 1;
            int cmpKindIdx = randomInt(cmpExprKinds.size());

            String col = allCols.get(colIdx);
            String cmpKind = cmpExprKinds.get(cmpKindIdx).sql;
            String constVal = String.valueOf(rngWidth * rngBase);
            opExprStr = String.format("%s %s %s", col, cmpKind, constVal);
            randomCtx.currOpPredCount.addAndGet(1);
        } else {
            int complexExprIdx = randomInt(complexExprSampleList.size());
            opExprStr = complexExprSampleList.get(complexExprIdx);
            randomCtx.currOpPredCount.addAndGet(1);

            int subQueryExprFlag = randomInt(10);
            if (subQueryExprFlag < 3) {
                int subQueryExprIdx = randomInt(subQueryExprSampleList.size());
                opExprStr = subQueryExprSampleList.get(subQueryExprIdx);
                randomCtx.currOpPredCount.addAndGet(1);
            }
        }

        return opExprStr;
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
            int d = randomInt(100000);
            int e = randomInt(100000);
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
