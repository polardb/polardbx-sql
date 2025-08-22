package com.alibaba.polardbx.qatest.columnar.ddl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;

import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.readFileContent;
import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.runTestBySql;

public abstract class ColumnarDdlAutoLoadSqlTestBase extends PartitionTestBase {
    public static final String DB_NAME = "columnar_test";
    protected static String RESOURCES_FILE_PATH = "columnar/ddl/%s/";
    protected static String TEST_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.test.yml";
    protected static String RESULT_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.result";

    protected static String TEST_CASE_CONFIG_FILE_PATH_TEMPLATE = RESOURCES_FILE_PATH + "testcase.config.yml";

    protected PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams params;

    public ColumnarDdlAutoLoadSqlTestBase(PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams params) {
        super();
        this.params = params;
    }

    @Test
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.createPartDatabase(tddlConnection, DB_NAME);
        }
    }

    @Before
    final public void before() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
    }

    public static Connection getColumnarConnection() throws SQLException {
        Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
        JdbcUtil.useDb(connection, DB_NAME);
        return connection;
    }

    protected static List<PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams> getParameters(Class testClass,
                                                                                                int defaultParttions,
                                                                                                boolean supportAutoPart) {
        List<String> testCases = loadTestCaseByTestClassName(testClass);
        List<PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams> rsList = new ArrayList<>();
        for (int i = 0; i < testCases.size(); i++) {
            PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams p =
                new PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams(testCases.get(i), testClass,
                    defaultParttions, supportAutoPart);
            rsList.add(p);
        }
        return rsList;
    }

    protected static List<String> loadTestCaseByTestClassName(Class cls) {
        BufferedReader bufferedReader = null;
        List<String> testCases = new ArrayList<>();
        try {
            String testClassName = cls.getSimpleName();
            String fileName = String.format(TEST_CASE_CONFIG_FILE_PATH_TEMPLATE, testClassName);
            InputStream in = cls.getClassLoader().getResourceAsStream(fileName);
            InputStreamReader inputStreamReader = new InputStreamReader(in, "utf8");
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                testCases.add(line.trim());
            }
            return testCases;
        } catch (Throwable ex) {
            ex.printStackTrace();
            log.error(ex.getMessage());
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return testCases;
    }

    protected void runOneTestCaseInner(PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams params) {
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
                if ("PartitionTablePartRouteTest".equalsIgnoreCase(testClassName)
                    || "TableReorgTest".equalsIgnoreCase(testClassName)) {
                    JdbcUtil.createPartDatabaseUsingUtf8(conn, dbName);
                } else {
                    JdbcUtil.createPartDatabase(conn, dbName);
                }
                JdbcUtil.disableAutoForceIndex(conn);
                testResult = runTestBySourceSql(tcName.toLowerCase(), supportAutoPart, testClass, dbName, conn, null);
//                JdbcUtil.dropDatabase(conn, dbName);
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

            testResult = (params.ignoreAutoIncrement ?
                testResult.replaceAll("AUTO_INCREMENT = [0-9]{1,}", "AUTO_INCREMENT = ignore_val") : testResult);

            exceptedResult =
                params.supportAutoPart ? exceptedResult.replaceAll("#@#", "" + params.defaultPartitions) :
                    exceptedResult;
            exceptedResult = exceptedResult.replaceAll("part_mtr", params.testDbName);

            // remove HitCache and ;
            testResult = testResult.replaceAll(";\n", "\n");
            exceptedResult = exceptedResult.replaceAll(";\n", "\n");

            String pattern = "HitCache.*\n?|";

            boolean mysql80 = isMySQL80();
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

                    tmpRs = tmpRs.replace("bigint(20)", "bigint");
                    tmpRs = tmpRs.replace("bigint(11)", "bigint");
                    tmpRs = tmpRs.replace("int(11)", "int");

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

            // Remove 'Finish time' of CHECK COLUMNAR INDEX report
            if (testResult.contains("CHECK COLUMNAR INDEX")) {
                testResult = testResult.replaceAll(
                    "\\) Finish time: \\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}",
                    ") Finish time: %");
            }

            buildAndPrintTestInfo(testClassName, tcName, testResult, exceptedResult);
            //The result of ShowDalTest maybe out-of-order by correct (e.g. show columns), therefore we only compare length
            if ("ShowDalTest".equalsIgnoreCase(testClassName)) {
                Assert.assertEquals(exceptedResult.length(), testResult.length());
            } else {
                Assert.assertEquals(exceptedResult, testResult);
            }
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

    protected String loadTestResultByTestName(String testCaseName, Class testClass) {

        String testClassName = testClass.getSimpleName();
        StringBuffer strBuf = new StringBuffer();
        String fileName = String.format(RESULT_FILE_TEMPLATE, testClassName, testCaseName);
        boolean isSucc = readFileContent(strBuf, fileName, testClass);
        if (isSucc) {
            return strBuf.toString();
        }
        return "";
    }

    protected String buildAndPrintTestInfo(String testClassName, String testCaseName, String testedResult,
                                           String exceptedResult) {
        boolean isSame = exceptedResult.equals(testedResult);
        String testInfo = null;
        if (!isSame) {
            testInfo = String
                .format("\n##############\nTestName:%s.%s\nIsSame:%s\nTestedResult:\n%s\n,\n\nExceptedResult:\n%s\n",
                    testClassName, testCaseName, exceptedResult.equals(testedResult),
                    testedResult, exceptedResult);
            log.info(testInfo);
            System.out.print(testInfo);
        }

        return testInfo;
    }

    protected static String runTestBySourceSql(String testCaseName,
                                               boolean isSupportAutoPart,
                                               Class testClass,
                                               String db,
                                               Connection testConn,
                                               Function<String, String> applySubstitute) throws Exception {

        try {

            //String sql = "create table if not exists tbl (a int not null)\npartition by hash(a)\npartitions 4;show create table tbl;explain select * from tbl where a=100;insert into tbl values (10),(99),(100),(101);select * from tbl order by a;explain select * from tbl where a>100;";
            String sql = loadTestSqlByTestName(testCaseName, testClass);

            return runTestBySql(testCaseName, sql, isSupportAutoPart, testClass, db, testConn, applySubstitute, null);
        } catch (Throwable e) {
            e.printStackTrace();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(baos);
            e.printStackTrace(pw);
            log.error(baos.toString());
            Assert.fail(e.getMessage());
        }
        return null;
    }

    private static String loadTestSqlByTestName(String testCaseName, Class testClass) {

        StringBuffer strBuf = new StringBuffer();
        String testClassName = testClass.getSimpleName();
        String fileName = String.format(TEST_FILE_TEMPLATE, testClassName, testCaseName);
        boolean isSucc = readFileContent(strBuf, fileName, testClass);
        if (isSucc) {
            return strBuf.toString();
        }
        return "";
    }
}
