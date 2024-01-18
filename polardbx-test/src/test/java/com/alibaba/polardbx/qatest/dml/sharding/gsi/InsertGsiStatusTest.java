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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */



public class InsertGsiStatusTest extends GsiDMLTest {

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

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> rets = doPrepareData();
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public InsertGsiStatusTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    /**
     * Ignore insert for DELETE_ONLY GSI
     */
    @Test
    public void insertDeleteOnlyTest() throws Exception {
        if (HINT_STRESS_FLAG.equalsIgnoreCase(hint)) {
            return;
        }

        List<Object> param = new ArrayList<>();

        StringBuilder sql = new StringBuilder(
            "insert " + hint + " /*+TDDL: CMD_EXTRA(GSI_DEBUG=\"GsiStatus1\")*/ into ").append(
                baseOneTableName)
            .append(" (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)")
            .append(" values ");

        for (int i = 0; i < 100; i++) {
            sql.append("(?,?,?,?,?,?,?)");
            if (i != 99) {
                sql.append(",");
            }

            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
        }
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), param, true);

        String selectSql = hint + "/*+TDDL:CMD_EXTRA(ENABLE_INDEX_SELECTION=FALSE)*/ select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection, false, true);

        assertIndexEmpty(baseOneTableName);
    }

    /**
     * Insert for WRITE_ONLY GSI
     */
    @Test
    public void insertWriteOnlyTest() throws Exception {
        List<Object> param = new ArrayList<>();

        StringBuilder sql = new StringBuilder(
            (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " :
                "insert " + hint + " /*+TDDL: CMD_EXTRA(GSI_DEBUG=\"GsiStatus2\")*/") + " into ").append(
                baseOneTableName)
            .append(" (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)")
            .append(" values ");

        for (int i = 0; i < 100; i++) {
            sql.append("(?,?,?,?,?,?,?)");
            if (i != 99) {
                sql.append(",");
            }

            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
        }
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), param, true);

        String selectSql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection, false, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }
}
