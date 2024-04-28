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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.apache.calcite.sql.SqlFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.traceAllResultMatchAssert;

/**
 * Insert into gsi table with functions.
 *
 * @author changyuan.lh
 */

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class InsertGsiWithFuncTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

    @BeforeClass
    public static void beforeCreateTables() {
        try {
            concurrentCreateNewTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterDropTables() {

        try {
            concurrentDropTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Set<String> UNSUPPORTED_FUNCTION = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // Information Functions
        UNSUPPORTED_FUNCTION.add("CURRENT_ROLE");
        UNSUPPORTED_FUNCTION.add("FOUND_ROWS");
        UNSUPPORTED_FUNCTION.add("ICU_VERSION");
        UNSUPPORTED_FUNCTION.add("ROLES_GRAPHML");
        UNSUPPORTED_FUNCTION.add("ROW_COUNT");
        UNSUPPORTED_FUNCTION.add("SESSION_USER");
        UNSUPPORTED_FUNCTION.add("SYSTEM_USER");
        UNSUPPORTED_FUNCTION.add("TSO_TIMESTAMP");
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        String[][] tableNames = new String[][] {
            {
                "", ExecuteTableName.GSI_DML_TEST + "no_unique_one_index_base",
                ExecuteTableName.GSI_DML_TEST + "no_unique_multi_index_base"},
            {
                HINT_STRESS_FLAG, ExecuteTableName.GSI_DML_TEST + "no_unique_multi_index_base",
                ExecuteTableName.GSI_DML_TEST + "no_unique_one_index_base"}};
        List<String[]> rets = Arrays.asList(tableNames);
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public InsertGsiWithFuncTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Test
    public void testFunc() {
        String sql = "INSERT INTO "
            + baseOneTableName
            + " (pk, integer_test, bigint_test, varchar_test, datetime_test) VALUES ("
            + "1, MONTH(CURDATE()), UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY)), CONCAT('test_', DATE_SUB"
            + "(CURDATE(), INTERVAL 1 DAY)), '2019-10-01 00:00:00')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testFunc1() {
        for (String func : SqlFunction.DYNAMIC_FUNCTION) {
            if (UNSUPPORTED_FUNCTION.contains(func)) {
                continue;
            }

            truncateTable(baseOneTableName);

            String sql = hint + " INSERT INTO "
                + baseOneTableName
                + " (pk, integer_test, bigint_test, varchar_test, datetime_test) VALUES ("
                + "1, MONTH(CURDATE()), UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY)), " + func + "(), NOW())";
//            System.out.println(sql);

            // Do not support sql like " TRACE /* //1/ */ INSERT ", stress flag will not work
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql,
                (TStringUtil.isBlank(hint) ? "TRACE " : "") + sql, null, false);

            if (TStringUtil.isBlank(hint)) {
                traceAllResultMatchAssert("SHOW TRACE", null, tddlConnection,
                    (trace) -> !trace.matches("[\\s\\S]*" + func + "[\\s\\S]*"));
            }
        }
    }
}
