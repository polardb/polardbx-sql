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

package com.alibaba.polardbx.qatest.dql.sharding.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.server.util.StringUtil;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author hongxi.chx
 */
public class ExplainTest extends ReadBaseTestCase {

    private static final Log log = LogFactory.getLog(ExplainTest.class);

    public ExplainTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    /**
     * @since 5.4.5
     */
    @Test
    public void explainSelectTest() {
        String sql =
            "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ explain select * from " + baseOneTableName + " a inner join "
                + baseTwoTableName + " b where a.pk = b.pk";
        try (Statement statement = tddlConnection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
            Assert.assertTrue(rs.next());
        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void explainAllSelectTest() {
        Table<String, String, BiConsumer<ResultSet, String>> testCases = ExplainTest.buildCases();

        // may test more sqls
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from " + baseOneTableName + " a join "
            + baseTwoTableName + " b on a.pk = b.pk where a.integer_test not in(10,20,30) and b.float_test < 10");
        sqls.add("select * from " + baseOneTableName + " a where a.pk in(select pk from "
            + baseTwoTableName + " b  where b.float_test < 10)");
        sqls.add("select * from " + baseOneTableName + " a join "
            + baseTwoTableName
            + " b on a.pk = b.integer_test where a.integer_test not in(10,20,30) order by a.pk limit 10");

        for (String sql : sqls) {
            try (Statement statement = tddlConnection.createStatement()) {
                ExplainTest.RunFullTest(testCases, sql, statement, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            //test explain in transaction
            try (Statement statement = tddlConnection.createStatement()) {
                try {
                    statement.execute("begin");
                    ExplainTest.RunFullTest(testCases, sql, statement, true);
                } finally {
                    if (statement != null) {
                        statement.execute("rollback");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    @FileStoreIgnore
    public void explainAllInsertTest() {
        Table<String, String, BiConsumer<ResultSet, String>> testCases = ExplainTest.buildCases();

        // may test more sqls
        List<String> sqls = new ArrayList<>();
        sqls.add("insert into " + baseOneTableName + " a "
            + "(pk,integer_test,varchar_test) values(10+2,10,\"a\")");

        for (String sql : sqls) {
            try (Statement statement = tddlConnection.createStatement()) {
                ExplainTest.RunExplainInsertTest(testCases, sql, statement);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void RunExplainInsertTest(Table<String, String, BiConsumer<ResultSet, String>> testCases,
                                            String sql,
                                            Statement statement) throws SQLException {
        List<String> explains = Arrays.asList("optimizer", "cost", "vec", "base");
        Collections.shuffle(explains);
        for (String explain : explains) {
            for (Map.Entry<String, BiConsumer<ResultSet, String>> entry : testCases.row(explain)
                .entrySet()) {
                String testSql = entry.getKey() + sql;
                ResultSet rs = statement.executeQuery(testSql);
                boolean next = rs.next();
                assertWithMessage("sql: " + testSql + " can't be empty").that(next).isTrue();
                entry.getValue().accept(rs, testSql);
                rs.close();
            }
        }
    }

    public static void RunFullTest(Table<String, String, BiConsumer<ResultSet, String>> testCases,
                                   String sql,
                                   Statement statement,
                                   boolean withoutCache) throws SQLException {
        List<Table.Cell<String, String, BiConsumer<ResultSet, String>>> cases = new ArrayList<>(testCases.cellSet());
        Collections.shuffle(cases);
        for (Table.Cell<String, String, BiConsumer<ResultSet, String>> cell : cases) {
            String testSql = cell.getColumnKey() + sql;
            if (!withoutCache) {
                testSql = "/*+TDDL:cmd_extra()*/" + testSql;
            }
            ResultSet rs = statement.executeQuery(testSql);
            boolean next = rs.next();
            assertWithMessage("sql: " + testSql + " can't be empty").that(next).isTrue();
            Objects.requireNonNull(cell.getValue()).accept(rs, testSql);
            rs.close();
        }
        for (Map.Entry<String, BiConsumer<ResultSet, String>> entry : testCases.row("optimizer").entrySet()) {
            String testSql = entry.getKey() + sql;
            ResultSet rs = statement.executeQuery(testSql);
            boolean next = rs.next();
            assertWithMessage("sql: " + testSql + " can't be empty").that(next).isTrue();
            entry.getValue().accept(rs, testSql);
            rs.close();
        }
    }

    /**
     * @since 5.4.5
     */
    @Test
    public void explainSelectTest1() {
        String sql1 =
            "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ /*+TDDL:node(0)*/explain select * from " + baseOneTableName
                + " a inner join " + baseTwoTableName + " b where a.pk = b.pk";
        String sql2 = "explain/*+TDDL:node(0)*/ select * from " + baseOneTableName + " a inner join " + baseTwoTableName
            + " b where a.pk = b.pk";
        try (Statement statement1 = tddlConnection.createStatement();
            Statement statement2 = tddlConnection.createStatement()) {
            ResultSet rs1 = statement1.executeQuery(sql1);
            ResultSet rs2 = statement2.executeQuery(sql2);
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
            if (rs1.next() && rs2.next()) {
                final String string1 = rs1.getString(1);
                final String string2 = rs2.getString(1);
                Assert.assertTrue(!string1.equals(string2));
            }

        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * @since 5.4.5
     */
    @Test
    public void selectTest() {
        String sql = "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ /*+TDDL master()*/ select * from " + baseOneTableName
            + " a inner join " + baseTwoTableName + " b where a.pk = b.pk";
        try (Statement statement = tddlConnection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            Assert.assertTrue(rs.next());
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }

    }

    private boolean existsMultiDbMultiTb(String... tables) {
        for (int i = 0; i < tables.length; i++) {
            if (tables[i].contains("multi_db_multi_tb")) {
                return true;
            }
        }
        return false;
    }

    /**
     * prepare for all kind of explain
     *
     * @return triple of (abbr., sql usage, check function)
     */
    public static Table<String, String, BiConsumer<ResultSet, String>> buildCases() {
        Table<String, String, BiConsumer<ResultSet, String>> testCases = HashBasedTable.create();

        testCases.put("cost", "explain cost ", (ResultSet rs, String explainSql) -> {
            try {
                String row = rs.getString(1);
                assertWithMessage("sql: " + explainSql + "\nshould contain 'rowcount'")
                    .that(row.contains("rowcount")).isTrue();
                assertWithMessage("sql: " + explainSql + "\nshould contain 'cpu'")
                    .that(row.contains("cpu")).isTrue();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        testCases.put("analyze", "explain analyze ", (ResultSet rs, String explainSql) -> {
            try {
                String row = rs.getString(1);
                assertWithMessage("sql: " + explainSql + "\nshould contain 'actual rowcount'")
                    .that(row.contains("actual rowcount")).isTrue();
                assertWithMessage("sql: " + explainSql + "\nshould contain 'actual time'")
                    .that(row.contains("actual time")).isTrue();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("sharding", "explain sharding ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have column 'SHARDING'")
                    .that(rsmd.getColumnName(2).toLowerCase()).isEqualTo("sharding");
                assertWithMessage("sql: " + explainSql + "\nshould have column 'SHARD_COUNT'")
                    .that(rsmd.getColumnName(3).toLowerCase()).isEqualTo("shard_count");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("base", "explain ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have 1 column")
                    .that(rsmd.getColumnCount()).isEqualTo(1);
                assertWithMessage("sql: " + explainSql + "\nshould have column 'LOGICAL EXECUTIONPLAN'")
                    .that(rsmd.getColumnName(1).toLowerCase()).isEqualTo("logical executionplan");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        testCases.put("vec", "explain vec ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have 2 columns")
                    .that(rsmd.getColumnCount()).isEqualTo(2);
                assertWithMessage("sql: " + explainSql + "\nshould have column 'EXTRA INFO'")
                    .that(rsmd.getColumnName(2).toLowerCase()).isEqualTo("extra info");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("optimizer", "explain optimizer ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have 2 columns")
                    .that(rsmd.getColumnCount()).isEqualTo(2);
                assertWithMessage("sql: " + explainSql + "\nshould have column 'STAGE'")
                    .that(rsmd.getColumnName(1).toLowerCase()).isEqualTo("stage");
                String expectedStart = "Start";
                String expectedEnd = "End";
                boolean existStart = rs.getString(1).contains(expectedStart);
                boolean existEnd = rs.getString(1).contains(expectedEnd);
                while (rs.next()) {
                    existStart |= rs.getString(1).contains(expectedStart);
                    existEnd |= rs.getString(1).contains(expectedEnd);
                }
                assertWithMessage("sql: " + explainSql + "\nshould contain 'Start'")
                    .that(existStart).isTrue();
                assertWithMessage("sql: " + explainSql + "\nshould contain 'End'")
                    .that(existEnd).isTrue();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("statistics", "explain statistics ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have 2 columns")
                    .that(rsmd.getColumnCount()).isEqualTo(2);
                assertWithMessage("sql: " + explainSql + "\nshould contain 'SQL:'")
                    .that(rs.getString(1)).contains("SQL:");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("logicalview", "explain logicalview ", (ResultSet rs, String explainSql) -> {
            try {
                assertWithMessage("sql: " + explainSql + "\nshould have 1 column")
                    .that(rs.getMetaData().getColumnCount()).isEqualTo(1);
                String expectedRelNode = "MysqlTableScan";
                String expectedRelNode2 = "OrcTableScan";
                boolean exist = rs.getString(1).contains(expectedRelNode) ||
                    rs.getString(1).contains(expectedRelNode2);
                while (!exist && rs.next()) {
                    exist = rs.getString(1).contains(expectedRelNode) ||
                        rs.getString(1).contains(expectedRelNode2);
                }
                assertWithMessage("sql: " + explainSql + "\nshould contain 'MysqlTableScan'|OrcTableScan")
                    .that(exist).isTrue();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("execute", "explain execute ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have 12 columns")
                    .that(rsmd.getColumnCount()).isEqualTo(12);
                assertWithMessage("sql: " + explainSql + "\nshould have column 'id'")
                    .that(rsmd.getColumnName(1).toLowerCase()).isEqualTo("id");
                assertWithMessage("sql: " + explainSql + "\nshould have column 'possible_keys'")
                    .that(rsmd.getColumnName(6).toLowerCase()).isEqualTo("possible_keys");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("physical", "explain physical ", (ResultSet rs, String explainSql) -> {
            try {
                String[] targets = new String[] {"ExecutorMode", "ExecutorType"};
                boolean exist = false;
                String row = rs.getString(1);
                Assert.assertTrue(!StringUtil.isEmpty(row));
                for (String target : targets) {
                    exist |= row.contains(target);
                }
                // make sure the have
                assertWithMessage("sql: " + explainSql + "\nshould contain 'ExecutorMode'")
                    .that(exist).isTrue();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("advisor", "explain advisor ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have column 'IMPROVE_VALUE'")
                    .that(rsmd.getColumnName(1).toLowerCase()).isEqualTo("improve_value");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        testCases.put("json", "explain json_plan ", (ResultSet rs, String explainSql) -> {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertWithMessage("sql: " + explainSql + "\nshould have only one column")
                    .that(rsmd.getColumnCount()).isEqualTo(1);
                assertWithMessage("sql: " + explainSql + "\nshould have column 'plan'")
                    .that(rsmd.getColumnName(1).toLowerCase()).isEqualTo("plan");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return testCases;
    }
}
