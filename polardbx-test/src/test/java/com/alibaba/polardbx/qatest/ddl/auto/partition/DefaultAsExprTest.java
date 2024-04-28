package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.regex.Matcher;

@RunWith(value = Parameterized.class)
public class DefaultAsExprTest extends PartitionAutoLoadSqlTestBase {

    public DefaultAsExprTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {

        return getParameters(DefaultAsExprTest.class, 0, false);
    }

    @Override
    protected void runOneTestCaseInner(AutoLoadSqlTestCaseParams params) {
        boolean mysql80 = isMySQL80();
        // only run in mysql 8.0
        if (!isMySQL80()) {
            return;
        }

        String tcName = params.tcName;
        String dbName = params.testDbName;
        String testClassName = params.testClass.getSimpleName();
        boolean supportAutoPart = params.supportAutoPart;
        Class testClass = params.testClass;
        String exceptedResult = null;
        String testResult = null;
        Connection conn = null;
        try {
            exceptedResult = loadTestResultByTestName(tcName.toLowerCase(), testClass);
            exceptedResult = exceptedResult.trim();
            try {
                conn = ConnectionManager.getInstance().newPolarDBXConnection();
                JdbcUtil.dropDatabase(conn, dbName);
                JdbcUtil.createPartDatabase(conn, dbName);
                applyResourceFileIfExists(dbName, tcName.toLowerCase(), testClass);
                testResult = runTestBySourceSql(tcName.toLowerCase(), supportAutoPart, testClass, dbName, conn,
                    s -> applySubstitute(s));
                JdbcUtil.dropDatabase(conn, dbName);
            } catch (Throwable ex) {
                throw ex;
            }

            testResult = testResult.trim();
            exceptedResult = exceptedResult.replaceAll("\\s+\n", "\n");
            testResult = testResult.replaceAll("\\s+\n", "\n");

            testResult = testResult.replaceAll("PARTITION `", "PARTITION ");
            testResult = testResult.replaceAll("` VAL", " VAL");
            testResult = testResult.replaceAll("`\n \\(SUBPARTITION", "\n \\(SUBPARTITION");
            testResult = testResult.replaceAll("` ENGINE", " ENGINE");

            // Remove the random suffix of GSI.
            // Remove table group
            testResult = (params.supportAutoPart ?
                testResult.replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")) : testResult)
                .replaceAll("tablegroup = `tg[0-9]{1,}` \\*/", "tablegroup = `tg` */");
            exceptedResult =
                params.supportAutoPart ? exceptedResult.replaceAll("#@#", "" + params.defaultPartitions) :
                    exceptedResult;
            exceptedResult = exceptedResult.replaceAll("part_mtr", params.testDbName);

            if (mysql80) {
                String[] rsCmpArr = new String[2];
                rsCmpArr[0] = testResult;
                rsCmpArr[1] = exceptedResult;

                for (int i = 0; i < rsCmpArr.length; i++) {
                    String tmpRs = rsCmpArr[i];
                    tmpRs = tmpRs.replace(" DEFAULT COLLATE = utf8mb4_0900_ai_ci", "");
                    tmpRs = tmpRs.replace(" COLLATE utf8mb4_0900_ai_ci", "");

                    tmpRs = tmpRs.replace(" DEFAULT COLLATE = utf8mb4_general_ci", "");
                    tmpRs = tmpRs.replace(" COLLATE utf8mb4_general_ci", "");

                    tmpRs = tmpRs.replace(" DEFAULT COLLATE = utf8_general_ci", "");
                    tmpRs = tmpRs.replace(" COLLATE utf8_general_ci", "");

                    tmpRs = tmpRs.replace(" DEFAULT COLLATE = gbk_chinese_ci", "");
                    tmpRs = tmpRs.replace(" COLLATE gbk_chinese_ci", "");

                    tmpRs = tmpRs.replace(" CHARSET `utf8mb4` COLLATE `utf8mb4_general_ci`", "");
                    tmpRs = tmpRs.replace(" CHARACTER SET utf8mb4", "");

                    tmpRs = tmpRs.replace("DEFAULT_GENERATED ", "");
                    tmpRs = tmpRs.replace("DEFAULT_GENERATED", "");

                    tmpRs = tmpRs.replace("USING HASH ", "");
                    tmpRs = tmpRs.replace("int(11) ", "int ");

                    rsCmpArr[i] = tmpRs;
                }

                testResult = rsCmpArr[0];
                exceptedResult = rsCmpArr[1];
            }

            if (testResult.contains("varchar(256)")) {
                boolean xrpc = false;
                try (final ResultSet rs = JdbcUtil.executeQuery("show variables like 'new_rpc'", tddlConnection)) {
                    while (rs.next()) {
                        if (rs.getString(2).equalsIgnoreCase("on")) {
                            xrpc = true;
                        }
                    }
                } catch (Throwable ignore) {
                }
                if (xrpc) {
                    testResult = testResult.replaceAll("varchar\\(256\\)", "varchar(192)");
                }
            }

            buildAndPrintTestInfo(testClassName, tcName, testResult, exceptedResult);
            Assert.assertEquals(exceptedResult, testResult);
        } catch (Throwable ex) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(baos);
            ex.printStackTrace(pw);
            log.error(baos.toString());
            System.out.println(ex.getMessage());
            Assert.fail(String.format("TestCase(%s/%s) failed, error is ", testClassName, tcName) + ex.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Throwable ex) {

            }
        }
    }

}
