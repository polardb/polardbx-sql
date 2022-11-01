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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.GSI_DML_TEST;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;

public class DynamicFunctionTest extends GsiDMLTest {

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

    // [test_column, function, keyword in physical plan
    String[][] testFunctions = {
        {"decimal_test", "last_insert_id()", "LAST_INSERT_ID"},
        {"timestamp_test", "now()", "NOW"},
        {"date_test", "current_date()", "CURRENT_DATE"}, {"date_test", "current_date", "CURRENT_DATE"},
        {"time_test", "current_time()", "CURRENT_TIME"}, {"time_test", "current_time", "CURRENT_TIME"},
        {"timestamp_test", "current_timestamp()", "CURRENT_TIMESTAMP"},
        {"timestamp_test", "current_timestamp", "CURRENT_TIMESTAMP"}, {"date_test", "curdate()", "CURDATE"},
        {"time_test", "curtime()", "CURTIME"}, {"datetime_test", "sysdate()", "SYSDATE"},
        {"time_test", "utc_time()", "UTC_TIME"}, {"date_test", "utc_date()", "UTC_DATE"},
        {"timestamp_test", "utc_timestamp()", "UTC_TIMESTAMP"}, {"time_test", "localtime", "LOCALTIME"},
        {"time_test", "localtime()", "LOCALTIME"}, {"timestamp_test", "localtimestamp", "LOCALTIMESTAMP"},
        {"timestamp_test", "localtimestamp()", "LOCALTIMESTAMP"}, {"char_test", "uuid()", "UUID"},
        {"char_test", "uuid_short()", "UUID_SHORT"}};

    @Parameterized.Parameters(name = "{index}:hint={0} table={1}")
    public static List<String[]> prepareData() {
        List<String[]> rets = Arrays.asList(new String[][] {
            {"", ExecuteTableName.GSI_DML_TEST + "unique_one_index_base"}
        });
        return prepareTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    static List<String[]> prepareTableNames(List<String[]> inputs, Map<String, String> tddlTables,
                                            Map<String, String> shadowTddlTables,
                                            Map<String, String> mysqlTables) {
        List<String[]> rets = new ArrayList<>();

        try (Connection tddlConn = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Connection mysqlConn = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(tddlConn, polardbXShardingDBName1());
            JdbcUtil.useDb(mysqlConn, mysqlDBName1());
            for (String[] strings : inputs) {
                Preconditions.checkArgument(strings.length == 2);
                final String replaceFlag = randomTableName(GSI_DML_TEST, 6);
                String[] ret = new String[2];
                String hint = strings[0];
                String table1 =
                    prepareNewTableDefine(tddlConn, mysqlConn, strings[1], replaceFlag, tddlTables, shadowTddlTables,
                        mysqlTables, hint != null && hint.contains(HINT_STRESS_FLAG));
                ret[0] = hint;
                ret[1] = table1;
                rets.add(ret);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return rets;
    }

    public DynamicFunctionTest(String hint, String baseOneTableName) throws Exception {
        super(hint, baseOneTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20; i++) {
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

        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
    }

    @Test
    public void functionInFilterTest() throws Exception {
        String updateSqlTemplate = hint + "trace update %s set double_test=0 where %s=%s";
        test(updateSqlTemplate);
    }

    @Test
    public void functionInSetTest() throws Exception {
        String updateSqlTemplate = hint + "trace update %s set %s=%s where pk=0";
        test(updateSqlTemplate);
    }

    @Test
    public void functionInDeleteTest() throws Exception {
        String updateSqlTemplate = hint + "trace delete from %s where %s=%s";
        test(updateSqlTemplate);
    }

    @Test
    public void functionInInsertTest() throws Exception {
        String updateSqlTemplate = hint + "trace insert into %s(pk, integer_test, %s) values(%d, %d, %s)";
        String showTraceSql = "show trace";

        int pk = 100;
        for (String[] unit : testFunctions) {
            String column = unit[0];
            String function = unit[1];
            String keyword = unit[2];

            String updateSql = String.format(updateSqlTemplate, baseOneTableName, column, pk, pk, function);
            pk++;
            JdbcUtil.updateData(tddlConnection, updateSql, Lists.newArrayList());

            ResultSet resultSet = JdbcUtil.executeQuery(showTraceSql, tddlConnection);
            while (resultSet.next()) {
                String statement = resultSet.getString("STATEMENT");
                Assert.assertTrue(!statement.contains(keyword));
            }
            resultSet.close();
        }
    }

    @Test
    public void functionInDuplicateTest() throws Exception {
        String updateSqlTemplate =
            hint + "trace insert into %s(pk, integer_test) values(0, 0) on duplicate key update %s=%s";
        test(updateSqlTemplate);
    }

    @Test
    public void functionInReplaceTest() throws Exception {
        String updateSqlTemplate = hint + "trace replace into %s(pk, integer_test, %s) values(0, 0, %s)";
        test(updateSqlTemplate);
    }

    private void test(String sqlTemplate) throws Exception {
        String showTraceSql = "show trace";

        for (String[] unit : testFunctions) {
            String column = unit[0];
            String function = unit[1];
            String keyword = unit[2];

            String updateSql = String.format(sqlTemplate, baseOneTableName, column, function);
            JdbcUtil.updateData(tddlConnection, updateSql, Lists.newArrayList());

            ResultSet resultSet = JdbcUtil.executeQuery(showTraceSql, tddlConnection);
            while (resultSet.next()) {
                String statement = resultSet.getString("STATEMENT");
                Assert.assertTrue(!statement.contains(keyword), keyword + " still exists in sql: \n" + statement);
            }
            resultSet.close();
        }
    }
}
