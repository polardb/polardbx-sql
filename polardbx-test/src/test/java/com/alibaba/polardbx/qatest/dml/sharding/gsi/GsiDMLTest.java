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
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.GSI_DML_TEST;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public abstract class GsiDMLTest extends CrudBasedLockTestCase {

    // { table name: [primary key, sharding key] }
    protected Map<String, List<String>> tablePrimaryAndShardingKeys;
    protected boolean clearDataWithDelete = true;

    protected static List<String[]> prepareNewTableNames(List<String[]> inputs, Map<String, String> tddlTables,
                                                         Map<String, String> shadowTddlTables,
                                                         Map<String, String> mysqlTables) {
        return prepareNewTableNames(inputs, tddlTables, shadowTddlTables, mysqlTables, polardbXShardingDBName1());
    }

    protected static List<String[]> prepareNewTableNames(List<String[]> inputs, Map<String, String> tddlTables,
                                                         Map<String, String> shadowTddlTables,
                                                         Map<String, String> mysqlTables, String db) {
        List<String[]> rets = new ArrayList<>();
        try (Connection tddlConn = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Connection mysqlConn = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(tddlConn, db);
            JdbcUtil.useDb(mysqlConn, mysqlDBName1());
            for (String[] strings : inputs) {
                Preconditions.checkArgument(strings.length == 3);
                final String replaceFlag = randomTableName(GSI_DML_TEST, 6);
                String[] ret = new String[3];
                String hint = strings[0];
                String table1 =
                    prepareNewTableDefine(tddlConn, mysqlConn, strings[1], replaceFlag, tddlTables, shadowTddlTables,
                        mysqlTables, hint != null && hint.contains(HINT_STRESS_FLAG));
                String table2 =
                    prepareNewTableDefine(tddlConn, mysqlConn, strings[2], replaceFlag, tddlTables, shadowTddlTables,
                        mysqlTables, hint != null && hint.contains(HINT_STRESS_FLAG));
                ret[0] = hint;
                ret[1] = table1;
                ret[2] = table2;
                rets.add(ret);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return rets;
    }

    protected static String prepareNewTableDefine(
        Connection tddlConn, Connection mysqlConn, String tableName,
        String replaceFlag,
        Map<String, String> tddlTables,
        Map<String, String> shadowTddlTables,
        Map<String, String> mysqlTables,
        boolean neeShadow) {
        String newTableName1 = tableName.replace(GSI_DML_TEST, replaceFlag);

        final String sql = String.format("show create table %s", tableName);
        String newTddlCreateTableStr;
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConn, sql)) {
            Assert.assertTrue(resultSet.next());
            final String createTableStr = resultSet.getString(2);
            Preconditions.checkArgument(createTableStr.contains(GSI_DML_TEST));
            newTddlCreateTableStr = createTableStr.replace(GSI_DML_TEST, replaceFlag).replace(
                "CREATE TABLE", "CREATE TABLE if not exists ");
        } catch (Exception e) {
            throw new RuntimeException("show create table failed!", e);
        }

        tddlTables.put(newTableName1, newTddlCreateTableStr);

        //创建影子表
        if (neeShadow) {
            String newTableName2 = tableName.replace(GSI_DML_TEST, "__test_" + replaceFlag);
            String newShadowTddlCreateTableStr = newTddlCreateTableStr.replace(
                replaceFlag, "__test_" + replaceFlag).replace(
                "CREATE TABLE", "CREATE SHADOW TABLE  ");

            shadowTddlTables.put(newTableName2, newShadowTddlCreateTableStr);
        }

        String newMysqlCreateTableStr;
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(mysqlConn, sql)) {
            Assert.assertTrue(resultSet.next());
            final String createTableStr = resultSet.getString(2);
            Preconditions.checkArgument(createTableStr.contains(GSI_DML_TEST));
            newMysqlCreateTableStr = createTableStr.replace(GSI_DML_TEST, replaceFlag).replace(
                "CREATE TABLE", "CREATE TABLE if not exists ");
        } catch (Exception e) {
            throw new RuntimeException("show create table failed!", e);
        }
        mysqlTables.put(newTableName1, newMysqlCreateTableStr);
        return newTableName1;
    }

    protected static void concurrentCreateNewTables(
        Map<String, String> tddlTables, Map<String, String> shadowTddlTables, Map<String, String> mysqlTables)
        throws Exception {
        concurrentCreateNewTables(tddlTables, shadowTddlTables, mysqlTables, polardbXShardingDBName1());
    }

    protected static void concurrentCreateNewTables(
        Map<String, String> tddlTables, Map<String, String> shadowTddlTables, Map<String, String> mysqlTables,
        String db)
        throws Exception {

        final List<Throwable> throwables = new ArrayList<>();
        Iterator<Map.Entry<String, String>> iteratorTddl = tddlTables.entrySet().stream().iterator();
        Iterator<Map.Entry<String, String>> iteratorMysql = mysqlTables.entrySet().stream().iterator();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
                        Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
                        JdbcUtil.useDb(tddlConnection, db);
                        JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
                        boolean finished = false;
                        Map.Entry<String, String> value = null;
                        while (!finished) {
                            synchronized (iteratorTddl) {
                                if (iteratorTddl.hasNext()) {
                                    value = iteratorTddl.next();
                                } else {
                                    value = null;
                                }
                            }
                            if (value != null) {
                                try {
                                    JdbcUtil.executeUpdate(tddlConnection, value.getValue());
                                } catch (Throwable t) {
                                    throwables.add(t);
                                }
                            } else {
                                synchronized (iteratorMysql) {
                                    if (iteratorMysql.hasNext()) {
                                        value = iteratorMysql.next();
                                    } else {
                                        finished = true;
                                        value = null;
                                    }
                                }
                                if (value != null) {
                                    try {
                                        JdbcUtil.executeUpdate(mysqlConnection, value.getValue());
                                    } catch (Throwable t) {
                                        throwables.add(t);
                                    }
                                }
                            }
                        }
                    } catch (Throwable t) {
                        throwables.add(t);
                        throw new RuntimeException(t);
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }
        if (throwables.size() > 0) {
            throw new RuntimeException(throwables.get(0));
        }
        Iterator<Map.Entry<String, String>> iteratorShadow = shadowTddlTables.entrySet().stream().iterator();

        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, db);
            while (iteratorShadow.hasNext()) {
                try {
                    JdbcUtil.executeUpdate(tddlConnection, iteratorShadow.next().getValue());
                } catch (Throwable t) {
                    //ignore
                }
            }
        }
    }

    protected static void concurrentDropTables(
        Map<String, String> tddlTables, Map<String, String> shadowTddlTables, Map<String, String> mysqlTables)
        throws Exception {
        concurrentDropTables(tddlTables, shadowTddlTables, mysqlTables, polardbXShardingDBName1());
    }

    protected static void concurrentDropTables(
        Map<String, String> tddlTables, Map<String, String> shadowTddlTables, Map<String, String> mysqlTables,
        String db)
        throws Exception {
        Iterator<Map.Entry<String, String>> iteratorTddl = tddlTables.entrySet().stream().iterator();
        Iterator<Map.Entry<String, String>> iteratorShadow = shadowTddlTables.entrySet().stream().iterator();
        Iterator<Map.Entry<String, String>> iteratorMysql = mysqlTables.entrySet().stream().iterator();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
                        Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
                        JdbcUtil.useDb(tddlConnection, db);
                        JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
                        boolean finished = false;
                        Map.Entry<String, String> value = null;
                        while (!finished) {
                            synchronized (iteratorTddl) {
                                if (iteratorTddl.hasNext()) {
                                    value = iteratorTddl.next();
                                } else {
                                    value = null;
                                }
                            }
                            if (value != null) {
                                try {
                                    JdbcUtil.dropTable(tddlConnection, value.getKey());
                                } catch (Throwable t) {
                                    //ignore
                                }
                            } else {
                                synchronized (iteratorMysql) {
                                    if (iteratorMysql.hasNext()) {
                                        value = iteratorMysql.next();
                                    } else {
                                        value = null;
                                    }
                                }
                                if (value != null) {
                                    try {
                                        JdbcUtil.dropTable(mysqlConnection, value.getKey());
                                    } catch (Throwable t) {
                                        //ignore
                                    }
                                } else {
                                    synchronized (iteratorShadow) {
                                        if (iteratorShadow.hasNext()) {
                                            value = iteratorShadow.next();
                                        } else {
                                            finished = true;
                                            value = null;
                                        }
                                    }
                                    if (value != null) {
                                        try {
                                            JdbcUtil.dropTable(tddlConnection, value.getKey());
                                        } catch (Throwable t) {
                                            //ignore
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    protected static List<String[]> doPrepareData() {
        return Arrays.asList(ExecuteTableName.gsiDMLTable());
    }

    public GsiDMLTest(String hint, String baseOneTableName) throws Exception {
        this(hint, baseOneTableName, null, null);
    }

    public GsiDMLTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        this(hint, baseOneTableName, baseTwoTableName, null);
    }

    public GsiDMLTest(String hint, String baseOneTableName, String baseTwoTableName, String baseThreeTableName)
        throws Exception {
        this.hint = hint;
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.baseThreeTableName = baseThreeTableName;

    }

    @Before
    public void initData() throws Exception {
        if (baseOneTableName != null) {
            truncateTable(baseOneTableName);
        }
        if (baseTwoTableName != null) {
            truncateTable(baseTwoTableName);
        }
        if (baseThreeTableName != null) {
            truncateTable(baseThreeTableName);
        }
    }

    private void initShardingKeyMap() throws Exception {
        if (tablePrimaryAndShardingKeys == null) {
            tablePrimaryAndShardingKeys = new HashMap<>();
        }

        List<String> tableNames = getBaseAndIndexTableName(baseOneTableName);
        if (TStringUtil.isNotBlank(baseTwoTableName)) {
            tableNames.addAll(getBaseAndIndexTableName(baseTwoTableName));
        }
        if (TStringUtil.isNotBlank(baseThreeTableName)) {
            tableNames.addAll(getBaseAndIndexTableName(baseThreeTableName));
        }
        ResultSet tddlRs = null;
        Statement tddlSt = null;

        String sql;
        for (String tableName : tableNames) {
            if (tablePrimaryAndShardingKeys.containsKey(tableName)) {
                continue;
            }

            Set<String> allKeys = new HashSet<>();

            sql = "show rule from " + tableName;
            tddlSt = tddlConnection.createStatement();
            tddlRs = tddlSt.executeQuery(sql);
            tddlRs.next();
            String partitionKeys = tddlRs.getString("DB_PARTITION_KEY");
            putShardingKeys(partitionKeys, allKeys);
            partitionKeys = tddlRs.getString("TB_PARTITION_KEY");
            putShardingKeys(partitionKeys, allKeys);
            tddlSt.close();
            tddlRs.close();

            sql = "desc " + tableName;
            tddlSt = tddlConnection.createStatement();
            tddlRs = tddlSt.executeQuery(sql);
            while (tddlRs.next()) {
                if (tddlRs.getString("Key").equalsIgnoreCase("PRI")) {
                    allKeys.add(tddlRs.getString("Field"));
                }
            }
            tddlSt.close();
            tddlRs.close();

            tablePrimaryAndShardingKeys.put(tableName, Lists.newArrayList(allKeys));
        }
    }

    private static void putShardingKeys(String partitionKey, Set<String> allKeys) {
        if (partitionKey == null || partitionKey.isEmpty()) {
            return;
        }

        String[] keys = partitionKey.split(",");
        for (String key : keys) {
            key = key.trim();
            if (!key.isEmpty()) {
                allKeys.add(key.toLowerCase());
            }
        }
    }

    /**
     * If record count exceeds 10000, we need a cycle to delete. So just use
     * truncate.
     */
    protected void truncateTable(String tableName) {
        String sql = null;

        if (clearDataWithDelete) {
            sql = hint + "/*+TDDL:scan()*/delete from " + tableName;
            JdbcUtil.executeUpdate(tddlConnection, sql);

            List<String> tableNames = getIndexTableName(tableName);
            for (String table : tableNames) {
                sql = hint + "/*+TDDL:cmd_extra(DML_ON_GSI=true)*/delete from " + table;
                JdbcUtil.executeUpdate(tddlConnection, sql);
            }
        } else {
            sql = hint
                + "/*+TDDL:CMD_EXTRA(TRUNCATE_TABLE_WITH_GSI=TRUE,MERGE_CONCURRENT=TRUE,MERGE_DDL_CONCURRENT=TRUE)*/truncate table "
                + tableName;
            JdbcUtil.executeUpdate(tddlConnection, sql);
        }

        sql = hint + "delete from " + tableName;
        JdbcUtil.executeUpdate(mysqlConnection, sql);
    }

    protected void prepareData(String tableName, int num) {
        truncateTable(tableName);

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + tableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < num; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(columnDataGenerator.year_testValue);
            param.add("test" + i);

            params.add(param);
        }

        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
    }

    protected List<String> getBaseAndIndexTableName(String baseTableName) {
        List<String> tableNames = Lists.newArrayList(baseTableName);
        tableNames.addAll(getIndexTableName(baseTableName));
        return tableNames;
    }

    protected List<String> getIndexTableName(String baseTableName) {
        List<String> tableNames = Lists.newArrayList();
        tableNames.add(baseTableName.replace("_base", "_index1"));
        if (baseTableName.contains("multi_index")) {
            tableNames.add(baseTableName.replace("_base", "_index2"));
        }
        return tableNames;
    }

    protected void assertIndexCountSame(String baseTableName) throws Exception {
        List<String> tableNames = getBaseAndIndexTableName(baseTableName);
        String sqlTemplate = "select count(1) from %s";

        long count = -1;
        for (String tableName : tableNames) {
            String sql = String.format(sqlTemplate, tableName);
            ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
            rs.next();
            long curCount = rs.getLong(1);
            if (count < 0) {
                count = curCount;
            } else {
                Assert.assertTrue(count == curCount);
            }
        }
    }

    /**
     * Assert that all index table is empty.
     */
    protected void assertIndexEmpty(String baseTableName) {
        List<String> tableNames = getIndexTableName(baseTableName);
        assertTablesEmpty(tableNames);
    }

    /**
     * Assert table empty.
     */
    protected void assertTablesEmpty(List<String> tableNames) {
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        for (String tableName : tableNames) {
            String sql = hint + "select * from " + tableName;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, Lists.newArrayList(), tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                List<List<Object>> results = JdbcUtil.getAllResult(tddlRs);
                assertThat(results.isEmpty(), is(true));
            } finally {
                if (tddlPs != null) {
                    JdbcUtil.close(tddlPs);
                }
                if (tddlRs != null) {
                    JdbcUtil.close(tddlRs);
                }
            }
        }
    }

    /**
     * Assert that data in index tables are the same with data in the base
     * table.
     */
    protected void assertIndexSame(String baseTableName) {
        List<String> tableNames = getBaseAndIndexTableName(baseTableName);
        assertTablesSame(tableNames);
    }

    /**
     * Assert that data in all tables are the same, even if columns in index
     * tables may be less than columns in the base table.
     */
    protected void assertTablesSame(List<String> tableNames) {
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        String tableToCompare = tableNames.get(0);
        List<List<Object>> resultsToCompare = null;
        List<String> columnsToCompare = null;

        for (String tableName : tableNames) {
            String sql = hint + "select * from " + tableName;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, Lists.newArrayList(), tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                List<List<Object>> results = JdbcUtil.getAllResult(tddlRs);
                List<String> columns = JdbcUtil.getColumnNameListToLowerCase(tddlRs);
                if (resultsToCompare == null) {
                    resultsToCompare = results;
                    columnsToCompare = columns;
                    continue;
                }

                List<Integer> columnIndexes = columns.stream()
                    .map(columnsToCompare::indexOf)
                    .collect(Collectors.toList());
                List<List<Object>> pickedResults = resultsToCompare.stream()
                    .map(list -> columnIndexes.stream().map(list::get).collect(Collectors.toList()))
                    .collect(Collectors.toList());

                assertWithMessage("Table contents are not the same, tables are " + tableToCompare + " and " + tableName)
                    .that(results)
                    .containsExactlyElementsIn(pickedResults);

                tddlPs = null;
                tddlRs = null;
            } finally {
                if (tddlPs != null) {
                    JdbcUtil.close(tddlPs);
                }
                if (tddlRs != null) {
                    JdbcUtil.close(tddlRs);
                }
            }
        }
    }

    /**
     * Assert that each row of the base table and its indexes are correctly
     * routed.
     */
    protected void assertRouteCorrectness(String baseTableName) throws Exception {
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        String sql = hint + "select * from " + baseTableName;
        tddlPs = JdbcUtil.preparedStatementSet(sql, Lists.newArrayList(), tddlConnection);
        tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
        List<List<Object>> results = JdbcUtil.getAllResult(tddlRs);
        List<String> columns = JdbcUtil.getColumnNameListToLowerCase(tddlRs);

        List<String> tableNames = getBaseAndIndexTableName(baseTableName);
        for (String tableName : tableNames) {
            assertRouteCorrectness(tableName, results, columns);
        }
    }

    /**
     * Assert that all selected data are routed correctly in this table.
     * Approach: select by its sharding keys and primary keys, if it has a
     * result, then it's right.
     *
     * @param tableName may be the base table or the index
     * @param selectedData result of "select *"
     * @param columnNames column names corresponding to selectedData
     */
    private void assertRouteCorrectness(String tableName, List<List<Object>> selectedData, List<String> columnNames)
        throws Exception {
        if (tablePrimaryAndShardingKeys == null) {
            initShardingKeyMap();
        }
        List<String> shardingKeys = tablePrimaryAndShardingKeys.get(tableName);
        JdbcUtil.assertRouteCorrectness(hint, tableName, selectedData, columnNames, shardingKeys, tddlConnection);
    }

}
