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

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;

/**
 * @author chenghui.lch
 */

public abstract class PartitionAutoLoadSqlTestBase extends PartitionTestBase {

    protected static final String RESOURCES_FILE_PATH = "partition/env/%s/";
    protected static final String ENV_FILE_PATH = "src/test/resources/" + RESOURCES_FILE_PATH;
    protected static final String TEST_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.test.yml";
    protected static final String RESULT_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.result";

    protected static final String RESOURCE_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.resource";
    protected static final String TEST_CASE_CONFIG_FILE_PATH_TEMPLATE = RESOURCES_FILE_PATH + "testcase.config.yml";

    protected static final String DISABLE_FAST_SQL_PARSER_FALG = "DISABLE_FAST_SQL_PARSER";
    protected static final String SQL_ERROR_MSG = "## ERROR_MSG";
    protected static final String SQL_ERROR_MSG_BEGIN_FLAG = "$#";
    protected static final String SQL_ERROR_MSG_END_FLAG = "#$";

    protected static final String DISABLE_AUTO_PART = "set @auto_partition=0;";
    protected static final String ENABLE_AUTO_PART = "set @auto_partition=1;";

    protected AutoLoadSqlTestCaseParams params;

    public static class AutoLoadSqlTestCaseParams {
        public boolean supportAutoPart = false;
        public int defaultPartitions = 0;
        public String tcName;
        public String testDbName;
        public Class testClass;

        public AutoLoadSqlTestCaseParams(String tcName,
                                         Class testClass,
                                         int defaultPartitions,
                                         boolean supportAutoPart) {
            this.tcName = tcName;
            this.defaultPartitions = defaultPartitions;
            this.supportAutoPart = supportAutoPart;
            this.testClass = testClass;
            this.testDbName = tcName.toLowerCase();
            if (testDbName.length() >= 16) {
                this.testDbName = testDbName.substring(0, 16);
            }
            this.testDbName = testDbName + "_" + String.valueOf(Math.abs((testClass.getName() + tcName).hashCode()));
        }

        @Override
        public String toString() {
            return this.tcName + "@" + this.testClass.getSimpleName();
        }
    }

    public PartitionAutoLoadSqlTestBase(AutoLoadSqlTestCaseParams params) {
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

//    @After
//    public void cleanMtr() {
//        cleanDataBase();
//    }

    protected static List<AutoLoadSqlTestCaseParams> getParameters(Class testClass, int defaultParttions,
                                                                   boolean supportAutoPart) {
        List<String> testCases = loadTestCaseByTestClassName(testClass);
        List<AutoLoadSqlTestCaseParams> rsList = new ArrayList<>();
        for (int i = 0; i < testCases.size(); i++) {
            AutoLoadSqlTestCaseParams p =
                new AutoLoadSqlTestCaseParams(testCases.get(i), testClass, defaultParttions, supportAutoPart);
            rsList.add(p);
        }
        return rsList;
    }

    protected String applySubstitute(String sql) {
        return sql;
    }

    protected void runOneTestCaseInner(AutoLoadSqlTestCaseParams params) {
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
            applyResourceFileIfExists(tcName.toLowerCase(), testClass);
            try {
                conn = ConnectionManager.getInstance().newPolarDBXConnection();
                JdbcUtil.dropDatabase(conn, dbName);
                if ("PartitionTablePartRouteTest".equalsIgnoreCase(testClassName)
                || "TableReorgTest".equalsIgnoreCase(testClassName)) {
                    JdbcUtil.createPartDatabaseUsingUtf8(conn, dbName);
                } else {
                    JdbcUtil.createPartDatabase(conn, dbName);
                }
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

    void applyResourceFileIfExists(String testCaseName, Class testClass) throws IOException {
        String fileName = String.format(RESOURCE_FILE_TEMPLATE, testClass.getSimpleName(), testCaseName);
        URL in = testClass.getClassLoader().getResource(fileName);
        if (in == null) {
            return;
        }
        String filename = in.getPath();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        // Execute each SQL statement
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            // Ignore comments and empty lines
            if (!line.startsWith("--") && !line.trim().isEmpty()) {
                sb.append(line).append("\n");
                if (line.endsWith("|")) { // End of statement
                    String sql = sb.toString();
                    JdbcUtil.executeSuccess(tddlConnection, sql.substring(0, sql.length() - 2));
                    sb.setLength(0); // Clear the StringBuilder
                }
            }
        }
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

    String loadTestResultByTestName(String testCaseName, Class testClass) {

        String testClassName = testClass.getSimpleName();
        StringBuffer strBuf = new StringBuffer();
        String fileName = String.format(RESULT_FILE_TEMPLATE, testClassName, testCaseName);
        boolean isSucc = readFileContent(strBuf, fileName, testClass);
        if (isSucc) {
            return strBuf.toString();
        }
        return "";
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

    private boolean writeFileContent(String newResult, String fileName) {
        OutputStream out = null;
        File file = null;
        try {
            URL url = this.getClass().getClassLoader().getResource(fileName);
            file = new File(url.toURI());
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            out = fileOutputStream;
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out);
            outputStreamWriter.write(newResult);
            outputStreamWriter.flush();
            return true;
        } catch (Throwable ex) {
            ex.printStackTrace();
            log.error(ex.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return false;
    }

    protected static boolean readFileContent(StringBuffer strOutputBuf, String fileName, Class cls) {
        BufferedReader bufferedReader = null;
        InputStream in = null;
        try {
            in = cls.getClassLoader().getResourceAsStream(fileName);
            InputStreamReader inputStreamReader = new InputStreamReader(in, "utf8");
            bufferedReader = new BufferedReader(inputStreamReader);
            int c;
            while ((c = bufferedReader.read()) != -1) {
                if (c == '\r') {// fastsql parse '\r\n' error
                    continue;
                }
                strOutputBuf.append((char) c);
            }
            return true;
        } catch (Throwable ex) {
            ex.printStackTrace();
            log.error(ex.getMessage());
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return false;
    }

    protected String getContextFromStream(InputStream inputStream, String streamType) throws Exception {

        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "utf8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        StringBuilder outSb = new StringBuilder("");
        int c = -1;
        while ((c = bufferedReader.read()) != -1) {
            outSb.append((char) c);
        }
        String context = outSb.toString();
        return context;
    }

    protected static String runTestBySourceSql(String testCaseName,
                                               boolean isSupportAutoPart,
                                               Class testClass,
                                               String db,
                                               Connection testConn) throws Exception {
        return runTestBySourceSql(testCaseName, isSupportAutoPart, testClass, db, testConn, null);
    }

    protected static String runTestBySourceSql(String testCaseName,
                                               boolean isSupportAutoPart,
                                               Class testClass,
                                               String db,
                                               Connection testConn,
                                               Function<String, String> applySubstitute) throws Exception {

        try {
            String stmtResult = "";
            String testClassName = testClass.getSimpleName();

            //String sql = "create table if not exists tbl (a int not null)\npartition by hash(a)\npartitions 4;show create table tbl;explain select * from tbl where a=100;insert into tbl values (10),(99),(100),(101);select * from tbl order by a;explain select * from tbl where a>100;";
            String sql = loadTestSqlByTestName(testCaseName, testClass);
            List<String> sqlList = new ArrayList<>();
            if (sql.contains(DISABLE_FAST_SQL_PARSER_FALG)) {
                extractSqlListByDelimiter(sql, sqlList, ";");
            } else {
                List<SQLStatement> stmtList = FastsqlUtils.parseSql(sql);
                for (int i = 0; i < stmtList.size(); i++) {
                    SQLStatement stmt = stmtList.get(i);
                    if (stmt instanceof MySqlHintStatement) {
                        continue;
                    }
                    SQLStatement lastStmt = null;
                    if (i > 0) {
                        lastStmt = stmtList.get(i - 1);
                    }
                    String hintSql = "";
                    if (lastStmt != null && lastStmt instanceof MySqlHintStatement) {
                        hintSql = lastStmt.toString();
                        hintSql = hintSql.replace(';', ' ');
                    }
                    String sqlText = stmt.toString();
                    sqlList.add(hintSql + sqlText);
                }
            }
            try {
                Connection conn = testConn;
                StringBuilder sb = new StringBuilder("");
                JdbcUtil.useDb(conn, db);
                JdbcUtil.executeUpdate(conn, "clear plancache");
                JdbcUtil.executeUpdate(conn, "set ENABLE_MPP=false");
                if (!isSupportAutoPart) {
                    execSqlAndPrintResult(DISABLE_AUTO_PART, false, conn, false);
                }
                for (int i = 0; i < sqlList.size(); i++) {
                    String sqlStmt = sqlList.get(i);

                    String expectErrMsg = null;
                    if (sqlStmt.toUpperCase().contains(SQL_ERROR_MSG)) {
                        int sidx = sqlStmt.indexOf(SQL_ERROR_MSG_BEGIN_FLAG);
                        int eidx = sqlStmt.indexOf(SQL_ERROR_MSG_END_FLAG);
                        expectErrMsg = sqlStmt.substring(sidx + SQL_ERROR_MSG_BEGIN_FLAG.length(), eidx);
                        expectErrMsg = expectErrMsg.trim();
                    }

                    String targetSqlStmt = sqlStmt;
                    if (applySubstitute != null) {
                        targetSqlStmt = applySubstitute.apply(sqlStmt);
                    }
                    String oneSqlResult = execSqlAndPrintResult(targetSqlStmt, false, conn, false);

                    if (expectErrMsg != null && oneSqlResult.contains(expectErrMsg)) {
                        oneSqlResult = expectErrMsg;
                    }

                    sb.append(sqlStmt);
                    sb.append('\n');
                    sb.append(oneSqlResult);
                    if (oneSqlResult.length() > 0) {
                        sb.append('\n');
                    }

                    log.info(String
                        .format("testClass[%s]/testCase[%s]-sql[%s]: sql:[%s], result:[%s]", testClassName,
                            testCaseName, i,
                            sqlStmt, oneSqlResult));
                }

                stmtResult = sb.toString();
            } catch (Throwable ex) {
                throw ex;
            }
            String testResult = stmtResult;
            return testResult;
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

    private static void extractSqlListByDelimiter(String sql, List<String> sqlList, String sqlDelimiter) {
        String[] sqlArr = sql.split(sqlDelimiter);
        for (int i = 0; i < sqlArr.length; i++) {
            String sqlVal = sqlArr[i].trim();
            if (sqlVal.isEmpty()) {
                continue;
            }
            sqlVal += ";";
            sqlList.add(sqlVal);
        }
    }

    protected static String execSqlAndPrintResult(String sql, boolean needGetErrMsg, Connection conn,
                                                  boolean ignoredException)
        throws Throwable {

        StringBuilder sb = new StringBuilder("");
        try {
            Statement stmt = conn.createStatement();
            int updateCnt;
            boolean hasResultSet;
            hasResultSet = stmt.execute(sql);
            while (true) {
                if (!hasResultSet) {
                    updateCnt = stmt.getUpdateCount();
                    if (updateCnt == -1) {
                        break;
                    }
                } else {
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    ResultSet rs = stmt.getResultSet();
                    List<List<String>> rsInfo = JdbcUtil.getAllStringResultWithColumnNames(rs, ignoredException, null);
                    if (sql.toLowerCase().startsWith("explain")) {
                        rsInfo = ignoreTemplateIdInfoForExplainResults(rsInfo);
                    }

                    for (int i = 0; i < rsInfo.size(); i++) {
                        if (i > 0) {
                            sb.append('\n');
                        }
                        List<String> rowInfo = rsInfo.get(i);
                        for (int j = 0; j < rowInfo.size(); j++) {
                            if (j > 0) {
                                sb.append(",");
                            }
                            sb.append(rowInfo.get(j));
                        }
                    }
                }
                hasResultSet = stmt.getMoreResults();
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
            log.error(ex);
            sb.append(ex.getMessage());
//            if (needGetErrMsg) {
//                sb.append(ex.getMessage());
//            } else {
//                ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                PrintWriter pw = new PrintWriter(baos);
//                ex.printStackTrace(pw);
//                log.error(baos.toString());
//                Assert.fail(baos.toString());
//                throw ex;
//            }
        }

        return sb.toString();
    }

    @After
    public void afterLoadCaseTestCase() {
        cleanDataBase();
    }

    protected static List<List<String>> ignoreTemplateIdInfoForExplainResults(List<List<String>> rsInfo) {
        if (rsInfo.size() > 2) {

            int lastIdx = rsInfo.size() - 1;
            if (rsInfo.get(lastIdx).get(0).toLowerCase().startsWith("templateid")) {
                rsInfo.remove(lastIdx);
            }

            lastIdx = rsInfo.size() - 1;
            if (rsInfo.get(lastIdx).get(0).toLowerCase().startsWith("source")) {
                rsInfo.remove(lastIdx);
            }

            lastIdx = rsInfo.size() - 1;
            if (rsInfo.get(lastIdx).get(0).toLowerCase().startsWith("hitcache")) {
                rsInfo.remove(lastIdx);
            }
        }

        for (int i = 0; i < rsInfo.size(); i++) {
            String explainContext = rsInfo.get(i).get(0);
            if (explainContext.toLowerCase().contains("relocate")) {
                explainContext = explainContext.replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$"));
            }
            if (explainContext.toLowerCase().contains("=logicalview#")) {
                explainContext =
                    explainContext.replaceAll("=LogicalView#[0-9a-f]+,", Matcher.quoteReplacement("=LogicalView#"));
            }

            if (explainContext.toLowerCase().contains(">> individual scalar subquery")) {
                explainContext = ">> individual scalar subquery";
            }

            rsInfo.get(i).set(0, explainContext);
        }
        return rsInfo;
    }
}
