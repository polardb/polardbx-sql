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

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class SelectGsiJoinTest extends GsiDMLTest {

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

    private static List<String> JOIN_TYPE = ImmutableList.of("INNER JOIN", "LEFT JOIN", "RIGHT JOIN");
    private static Map<Integer, Map<String, String>> INDEX_HINT = ImmutableMap.of(1,
        ImmutableMap.of("a", "/*+TDDL:index(a, {0})*/ ", "b", "/*+TDDL:index(b, {0})*/ "),
        2,
        ImmutableMap.of("a, b", "/*+TDDL:index(a, {0}) index(b, {1})*/ "));
    private static List<String> SELECT_LIST = ImmutableList
        .of("*", "a.integer_test", "b.integer_test", "a.*, b.integer_test", "a.integer_test, b.*");

    private String sql;

    public SelectGsiJoinTest(String hint, String baseOneTableName, String baseTwoTableName, String sql)
        throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
        this.sql = sql;
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1},table2={2},sql={3}")
    public static List<String[]> prepareData() {
        final String[][] initTables = ExecuteTableName.gsiDMLTable();
        List<String[]> tables = prepareNewTableNames(
            Arrays.asList(initTables), tddlTables, shadowTables, mysqlTables);

        final Map<String, List<String>> result = new LinkedHashMap<>();
        IntStream.range(0, tables.size()).forEach(tableIndex -> {
            final String defaultHint = tables.get(tableIndex)[0];
            final String table1 = tables.get(tableIndex)[1];
            final String table2 = tables.get(tableIndex)[2];
            final String index1 = table1.replaceFirst("base", "index1");
            final String index2 = table2.replaceFirst("base", "index1");
            final Map<String, String> indexMap = ImmutableMap.of("a", index1, "b", index2);
            final ImmutableList<String> tList = ImmutableList.of(defaultHint, table1, table2);

            JOIN_TYPE.forEach(joinType -> SELECT_LIST.forEach(selectList -> {
                final String sql = MessageFormat.format(
                    "SELECT {0} FROM {1} a {2} {3} b ON a.integer_test = b.integer_test",
                    selectList,
                    table1,
                    joinType,
                    table2);

                final Map<String, String> singleHints = INDEX_HINT.get(1);
                singleHints.forEach((table, hint) -> result
                    .put(defaultHint + MessageFormat.format(hint, indexMap.get(table)) + sql, tList));

                final Map<String, String> doubleHints = INDEX_HINT.get(2);
                doubleHints
                    .forEach((table, hint) -> result
                        .put(defaultHint + MessageFormat.format(hint, index1, index2) + sql, tList));
            }));
        });

        final String[][] resultArray = new String[result.size()][4];
        final AtomicInteger index = new AtomicInteger(0);
        result.forEach((k, v) -> {
            resultArray[index.get()][0] = v.get(0);
            resultArray[index.get()][1] = v.get(1);
            resultArray[index.get()][2] = v.get(2);
            resultArray[index.getAndIncrement()][3] = k;
        });
        List<String[]> rets = Arrays.asList(resultArray);
        return rets;
    }

    @Before
    public void initData() throws Exception {
        super.initData();
        String sql = hint + "insert into " + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 3; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
            param.add(i);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "insert into " + baseTwoTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    @Test
    public void indexSelectionWithHint1() {
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
