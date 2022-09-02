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
import java.util.regex.Matcher;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;

/**
 * @author chenghui.lch
 */

public abstract class PartitionAutoLoadSqlTestBase extends PartitionTestBase {

    protected static final String RESOURCES_FILE_PATH = "partition/env/%s/";
    protected static final String ENV_FILE_PATH = "src/test/resources/" + RESOURCES_FILE_PATH;
    protected static final String TEST_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.test.yml";
    protected static final String RESULT_FILE_TEMPLATE = RESOURCES_FILE_PATH + "%s.result";
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
        public Class testClass;

        public AutoLoadSqlTestCaseParams(String tcName,
                                         Class testClass,
                                         int defaultPartitions,
                                         boolean supportAutoPart) {
            this.tcName = tcName;
            this.defaultPartitions = defaultPartitions;
            this.supportAutoPart = supportAutoPart;
            this.testClass = testClass;
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

    protected void runOneTestCaseInner(AutoLoadSqlTestCaseParams params) {
        String tcName = params.tcName;
        String testClassName = params.testClass.getSimpleName();
        boolean supportAutoPart = params.supportAutoPart;
        Class testClass = params.testClass;
        String exceptedResult = null;
        String testResult = null;
        try {
            exceptedResult = loadTestResultByTestName(tcName.toLowerCase(), testClass);
            exceptedResult = exceptedResult.trim();
            testResult = runTestBySourceSql(tcName.toLowerCase(), supportAutoPart, testClass, tddlDatabase1);
            testResult = testResult.trim();
            exceptedResult = exceptedResult.replaceAll("\\s+\n", "\n");
            testResult = testResult.replaceAll("\\s+\n", "\n");
            // Remove the random suffix of GSI.
            // Remove table group
            testResult = (params.supportAutoPart ?
                testResult.replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")) : testResult)
                .replaceAll("tablegroup = `tg[0-9]{1,}` \\*/", "tablegroup = `tg` */");
            exceptedResult =
                params.supportAutoPart ? exceptedResult.replaceAll("#@#", "" + params.defaultPartitions) :
                    exceptedResult;
            exceptedResult = exceptedResult.replaceAll("part_mtr", tddlDatabase1);
            if (isMySQL80()) {
                testResult = testResult.replace(" DEFAULT COLLATE = utf8mb4_0900_ai_ci", "");
            }

            buildAndPrintTestInfo(testClassName, tcName, testResult, exceptedResult);
            Assert.assertEquals(exceptedResult, testResult);
        } catch (Throwable ex) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(baos);
            ex.printStackTrace(pw);
            log.error(baos.toString());
            Assert.fail(String.format("TestCase(%s/%s) failed, error is ", testClassName, tcName) + ex.getMessage());

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

    private String loadTestResultByTestName(String testCaseName, Class testClass) {

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
                                               String db) throws Exception {
        try {
            String stmtResult = "";
            String testClassName = testClass.getSimpleName();

            //String sql = "create table if not exists tbl (a int not null)\npartition by hash(a)\npartitions 4;show create table tbl;explain select * from tbl where a=100;insert into tbl values (10),(99),(100),(101);select * from tbl order by a;explain select * from tbl where a>100;";
            String sql = loadTestSqlByTestName(testCaseName, testClass);
            List<String> sqlList = new ArrayList<>();
            if (sql.contains(DISABLE_FAST_SQL_PARSER_FALG)) {
                String[] sqlArr = sql.split(";");
                for (int i = 0; i < sqlArr.length; i++) {
                    String sqlVal = sqlArr[i].trim();
                    if (sqlVal.isEmpty()) {
                        continue;
                    }
                    sqlVal += ";";
                    sqlList.add(sqlVal);
                }
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
            try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                StringBuilder sb = new StringBuilder("");
                JdbcUtil.useDb(conn, db);
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
                    String oneSqlResult = execSqlAndPrintResult(sqlStmt, false, conn, false);

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
        return rsInfo;
    }

    @After
    public void afterLoadCaseTestCase() {
        cleanDataBase();
    }
}
