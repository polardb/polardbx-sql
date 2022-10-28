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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
@FileStoreIgnore
public class ProcedureMtrTest extends BaseTestCase {
    protected static final Log log = LogFactory.getLog(ProcedureMtrTest.class);

    @Parameterized.Parameters(name = "{index}:{0}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            fileNameAndMaxLine());
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    public static Object[] fileNameAndMaxLine() {
        return new Pair[] {
            Pair.of("simple_procedure_test.sql", Integer.MAX_VALUE),
            Pair.of("procedure_alias_test.sql", Integer.MAX_VALUE),
            Pair.of("show_procedure_test.sql", Integer.MAX_VALUE),
            Pair.of("show_function_test.sql", Integer.MAX_VALUE),
            Pair.of("procedure_exception_handler_test.sql", Integer.MAX_VALUE)
        };
    }

    protected static final String RESOURCES_FILE_PATH = "procedure/";

    protected Connection mysqlConnection;
    protected Connection tddlConnection;
    private String delimiter = ";";
    private String expectException = null;

    private String testFile;
    private int maxCheckedLine;

    private static String NOT_SUPPORT_PLACE_HOLDER = "?todo_warnings?";
    private static String EXPECT_EXCEPTION_HOLDER = "?expect_exception?";

    public ProcedureMtrTest(Object testFileAndMaxLine) {
        this.testFile = (String) ((Pair) testFileAndMaxLine).getKey();
        this.maxCheckedLine = (Integer) ((Pair) testFileAndMaxLine).getValue();
    }

    @Before
    public void getConnection() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, "SET ENABLE_UDF = true");
    }

    @Test
    public void test() throws IOException {
        BufferedReader testReader = null;
        InputStream testStream = null;
        int lineNumber = 0;

        try {
            testStream = this.getClass().getClassLoader().getResourceAsStream(RESOURCES_FILE_PATH + testFile);
            testReader = new BufferedReader(new InputStreamReader(testStream, "utf8"));

            String str = null;
            StringBuilder sb = new StringBuilder();
            while ((str = testReader.readLine()) != null) {
                str = str.trim();
                lineNumber++;
                if (lineNumber > maxCheckedLine) {
                    System.out.println("Reached Max checked lines: " + maxCheckedLine);
                    break;
                }
                // ignore comment
                if (str.startsWith("-") || str.startsWith("#")) {
                    continue;
                }
                if (str.startsWith(NOT_SUPPORT_PLACE_HOLDER)) {
                    System.out.println(
                        "WARNING: " + " Line: " + lineNumber + ", " + str.substring(str.indexOf("content:")));
                    continue;
                }
                if (str.startsWith(EXPECT_EXCEPTION_HOLDER)) {
                    expectException = str.replace(EXPECT_EXCEPTION_HOLDER, "").trim();
                    continue;
                }
                // change delimiter
                if (str.trim().toLowerCase().startsWith("delimiter")) {
                    delimiter = changeDelimiter(str);
                    continue;
                }
                sb.append(str);
                if (str.endsWith(String.valueOf(delimiter))) {
                    String testSql = sb.toString().replace(delimiter, "").trim();
                    processSql(testSql, expectException);
                    sb.delete(0, sb.length());
                    expectException = null;
                }
                sb.append("\n");
            }
        } catch (Throwable ex) {
            System.out.println(ex.getMessage());
            log.error(ex.getMessage());
            Assert.fail("sp test failed at line:" + lineNumber);
        } finally {
            try {
                if (testStream != null) {
                    testStream.close();
                }
                if (testReader != null) {
                    testReader.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private String changeDelimiter(String sql) {
        if (!sql.endsWith(String.valueOf(delimiter))) {
            throw new RuntimeException("delimiter not excepted!");
        }
        String[] splits = sql.replace(delimiter, "").split(" ");
        return splits[splits.length - 1];
    }

    private void processSql(String sql, String expectException) {
        if (expectException == null) {
            processSql(sql);
            return;
        }
        String[] exceptions = expectException.split(":");
        String mysqlException = exceptions[0].trim();
        JdbcUtil.executeFaied(mysqlConnection, sql, mysqlException);
        if (exceptions.length == 1 || StringUtils.isEmpty(exceptions[1])) {
            JdbcUtil.executeSuccess(tddlConnection, sql);
        } else {
            String tddlException = exceptions[1].trim();
            JdbcUtil.executeFaied(tddlConnection, sql, tddlException);
        }
    }

    private void processSql(String sql) {
        if (sql.toLowerCase().startsWith("select")) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        } else {
            JdbcUtil.executeSuccess(mysqlConnection, sql);
            JdbcUtil.executeSuccess(tddlConnection, sql);
        }
    }
}
