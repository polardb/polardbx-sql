package com.alibaba.polardbx.qatest.twoPhaseDdl;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.pushDownDdl.random.RandomDdlGenerator;
import com.alibaba.polardbx.qatest.mdl.MdlDetectionTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NotThreadSafe
@RunWith(Parameterized.class)
public class MultiPartDdlRandomTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(MultiPartDdlRandomTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public MultiPartDdlRandomTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 5;

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("pushdown", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private Future<Boolean> executeRandomDdl(ExecutorService threadPool, String tableName, String createTableStmt,
                                             int totalRound,
                                             Connection connection, Connection mysqlConnection, List<Integer> indexes) {
        return threadPool.submit(() -> {
            String multiDdlPrefix = "<MultiDdlTest> ";
            String multiDdlKeyPrefix = "<MultiDdlTestKey> ";
            Boolean checkOk = false;
            try {
                String showCreateTableStmt = "show create table " + tableName;
                String checkTableStmt = "/*+TDDL:CMD_EXTRA(LOGICAL_CHECK_COLUMN_ORDER=true)*/check table " + tableName;
                Thread.sleep(smallDelay * 1000L);
                logger.info(multiDdlPrefix + "execute SQL:" + createTableStmt);
                JdbcUtil.executeUpdateSuccess(connection, createTableStmt);
                JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableStmt);
                String schemaName =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery("select database()", connection)).get(0).get(0)
                        .toString();
                int sucessCount = 0;
                RandomDdlGenerator randomDdlGenerator = new RandomDdlGenerator(tableName);
                for (int i = 0; i < totalRound; i++) {
                    logger.info(String.format("ROUND %d/%d...", i, totalRound));
                    String roundPrefix = String.format("ROUND %d ", i);
//                    Thread.sleep(smallDelay * 1000L);
                    logger.info("execute SQL:" + showCreateTableStmt);
                    String originalCreateTableStmt =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(showCreateTableStmt, connection)).get(0).get(1)
                            .toString();

                    String originalCreateTableStmtOnMySQL =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(showCreateTableStmt, mysqlConnection)).get(0).get(1)
                            .toString();
                    logger.info(roundPrefix + multiDdlKeyPrefix + "original create table stmt on polardbx: "
                        + formatSQLForPrint(originalCreateTableStmt));
                    logger.info(
                        roundPrefix + multiDdlKeyPrefix + "original create table stmt on mysql: " + formatSQLForPrint(
                            originalCreateTableStmtOnMySQL));
                    String alterTableDdl =
                        randomDdlGenerator.generateRandomAlterDdlForCreateTableStmt(originalCreateTableStmt);
                    logger.info(roundPrefix + multiDdlKeyPrefix + "execute SQL: " + formatSQLForView(alterTableDdl));
                    Boolean executeSuccessOnMySQL = true;
                    String errMsg = "";
                    String alterTableDdlOnMySql = converToMysqlStmt(alterTableDdl);
                    try {
                        JdbcUtil.executeUpdateWithException(mysqlConnection, alterTableDdlOnMySql);
                    } catch (SQLException sqlException) {
                        executeSuccessOnMySQL = false;
                        errMsg = sqlException.getMessage();
                        errMsg = errMsg.replace("'" + tableName + "'", "").replace(tableName, "");
                        logger.info(
                            String.format(
                                roundPrefix + multiDdlKeyPrefix
                                    + "execute SQL on MySQL failed: %s by cause %s for mysql",
                                formatSQLForPrint(alterTableDdlOnMySql), errMsg));
                    }
                    if (executeSuccessOnMySQL) {
                        sucessCount++;
                        logger.info(
                            String.format(roundPrefix + multiDdlKeyPrefix + "execute SQL on MySQL success: %s",
                                formatSQLForPrint(alterTableDdlOnMySql)));
                    }
                    if (!executeSuccessOnMySQL) {
                        logger.info(String.format("ROUND %d expected execute fail on polarx!", i));
                        Boolean executeSuccessOnPolarX = true;
                        String polarxErrMsg = "";
                        try {
                            JdbcUtil.executeUpdateWithException(connection, alterTableDdl);
                        } catch (SQLException sqlException) {
                            polarxErrMsg = sqlException.getMessage();
                            executeSuccessOnPolarX = false;
                        }
                        // TODO(yijin): may be fixed later.
//                        Boolean errOnDn = polarxErrMsg.toLowerCase().contains("Error occurs when execute on GROUP".toLowerCase());
                        Boolean errOnDn = false;
                        if (!executeSuccessOnMySQL && polarxErrMsg.toLowerCase().contains(errMsg.toLowerCase())
                            && !errOnDn) {
                            logger.info(String.format(roundPrefix + multiDdlKeyPrefix +
                                    "execute SQL on PolarX failed: %s by cause %s for polarx",
                                formatSQLForPrint(alterTableDdl),
                                polarxErrMsg));
                        } else if (!executeSuccessOnMySQL && errOnDn) {
                            logger.info(String.format(roundPrefix + multiDdlKeyPrefix +
                                    "execute SQL on PolarX failed: %s by cause %s for polarx",
                                formatSQLForPrint(alterTableDdl),
                                polarxErrMsg));
                            throw new RuntimeException(
                                String.format("ERROR: exepcted err msg %s, now ERROR ON DN: %s", errMsg, polarxErrMsg));
                        } else if (!executeSuccessOnMySQL) {
                            logger.info(String.format(roundPrefix + multiDdlKeyPrefix +
                                    "execute SQL on PolarX failed: %s by cause %s for polarx",
                                formatSQLForPrint(alterTableDdl),
                                polarxErrMsg));
                            throw new RuntimeException(
                                String.format("ERROR: exepcted err msg %s, now err msg %s", errMsg, polarxErrMsg));
                        } else {
                            logger.info(String.format(roundPrefix + multiDdlKeyPrefix +
                                    "execute SQL on PolarX failed: %s by cause %s for polarx",
                                formatSQLForPrint(alterTableDdl),
                                polarxErrMsg));
                            throw new RuntimeException(
                                String.format("ERROR: exepcted err msg %s, now success", errMsg));
                        }
                        waitForRunningDdlDone(connection);
                    } else {
                        logger.info(String.format("ROUND %d expected execute success for polardbx!", i));
                        JdbcUtil.executeUpdateSuccess(connection, alterTableDdl);
                        logger.info(String.format(roundPrefix + multiDdlKeyPrefix +
                                "execute SQL on PolarX success: %s",
                            formatSQLForPrint(alterTableDdl)));
                    }
                    logger.info("execute SQL:" + checkTableStmt);
                    List<List<Object>> checkTableResult =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(checkTableStmt, connection));
                    checkOk = checkTableResult.stream().allMatch(o -> o.get(3).toString().equalsIgnoreCase("ok"));
                    if (!checkOk) {
                        logger.info(" check table failed! results is " + checkTableResult);
                        throw new RuntimeException("bad result of check table, alter failed!");
                    }
                    logger.info(
                        String.format(roundPrefix + multiDdlKeyPrefix + " ROUND %s finished, %d sucess, %d failed.", i,
                            sucessCount, i - sucessCount));
                }
            } catch (Exception e) {
                logger.info(e.getMessage());
                throw e;
            }
            return checkOk;
        });
    }

    void waitForRunningDdlDone(Connection connection) throws InterruptedException {
        int waitEpoch = 0;
        int maxEpoch = 120;
        while (waitEpoch <= maxEpoch) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection, "show ddl"));
            if (results.isEmpty()) {
                return;
            } else {
                Thread.sleep(1000);
            }
            waitEpoch++;
        }
        throw new RuntimeException("wait for ddl terminating timeout!!!");
    }

    void innerTest(String tableName, Connection connection, String sql, int totalRound, int maxWaitTimeout,
                   String createTableStmt,
                   List<Integer> dropPhyTableIndexes, String expectedErrMsg, String otherErrMsg)
        throws SQLException, ExecutionException, InterruptedException {
        final ExecutorService ddlThreadPool =
            new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        Connection connection1 = getPolardbxConnection();
        futures.add(
            executeRandomDdl(ddlThreadPool, tableName, createTableStmt, totalRound, connection1, mysqlConnection,
                dropPhyTableIndexes));

        for (Future<Boolean> future : futures) {
            try {
                if (!future.get(maxWaitTimeout, TimeUnit.SECONDS)) {
                    Assert.fail(otherErrMsg);
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
                Assert.fail(otherErrMsg);
            } catch (Exception e) {
                throw e;
            }
        }

    }

    @Test
    public void testAlterTableRandomTest() throws SQLException, ExecutionException, InterruptedException {
        String mytable = schemaPrefix + randomTableName("alter_table_test", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption
            + " %s(id int, name varchar(100), price double, amount int, extra varchar(50))";
        String sql = String.format(createTableStmt, mytable);
        int maxWaitTimeout = 1000_000;
        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        int totalRound = 1000;
        innerTest(mytable, tddlConnection, sql, totalRound, maxWaitTimeout, sql, null, errMsg, otherErrMsg);
    }

    private String formatSQLForView(String sql) {
        char[] sqlChars = sql.toCharArray();
        int state = 0;
        for (int i = 0; i < sqlChars.length; i++) {
            if (state == 0 && sqlChars[i] == ',') {
                sqlChars[i] = '#';
            } else if (state == 0 && sqlChars[i] == '(') {
                state = 1;
            } else if (state == 1 && sqlChars[i] == ')') {
                state = 0;
            }
        }
        return new String(sqlChars).replace("#", ", \n    ");
    }

    private String formatSQLForPrint(String sql) {
        char[] sqlChars = sql.toCharArray();
        return new String(sqlChars).replace("\n", " ") + "\n";
    }

    private String converToMysqlStmt(String stmt) {
        return stmt.replace("ADD LOCAL INDEX", "ADD INDEX");
    }
}
