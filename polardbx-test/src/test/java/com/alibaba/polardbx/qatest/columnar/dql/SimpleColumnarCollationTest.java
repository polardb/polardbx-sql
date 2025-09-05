package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.columnar.dql.FullTypeTest.waitForRowCountEquals;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;

public class SimpleColumnarCollationTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_1 = "test_columnar_collation_1";
    private static final String TABLE_2 = "test_columnar_collation_2";
    private static final String TABLE_3 = "test_columnar_collation_3";

    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 %s(255) COLLATE %s) ";

    private static final String PARTITION_INFO = "partition by key(c1)";

    private String columnType;

    private String collation;

    private boolean compatible;

    private String table1;

    private String table2;

    private String table3;

    public SimpleColumnarCollationTest(Object columnType, Object pair) {
        this.columnType = (String) columnType;
        this.collation = (String) ((Pair) pair).getKey();
        this.compatible = (Boolean) ((Pair) pair).getValue();
        this.table1 = randomTableName(TABLE_1, 4);
        this.table2 = randomTableName(TABLE_2, 4);
        this.table3 = randomTableName(TABLE_3, 4);
    }

    @Parameterized.Parameters(name = "{index}:{0},{1}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            columnTypes(),
            collations()
        );
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

    public static Object[] columnTypes() {
        return new Object[] {
            "char",
            "varchar"
        };
    }

    public static Object[] collations() {
        return new Object[] {
            Pair.of("latin1_swedish_ci", true),
            Pair.of("LATIN1_GENERAL_CS", false),
            Pair.of("LATIN1_BIN", false),
            Pair.of("GBK_CHINESE_CI", true),
            Pair.of("GBK_BIN", false),
            Pair.of("UTF8_GENERAL_CI", true),
            Pair.of("UTF8_BIN", false)
        };
    }

    @Before
    public void prepare() throws SQLException, InterruptedException {
        dropTables();
        prepareData();
    }

    @After
    public void dropTables() throws SQLException {
        try (Connection connection = getPolardbxConnection0(PropertiesUtil.polardbXAutoDBName1())) {
            JdbcUtil.dropTable(connection, table1);
            JdbcUtil.dropTable(connection, table2);
            JdbcUtil.dropTable(connection, table3);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.dropTable(mysqlConnection, table1);
            JdbcUtil.dropTable(mysqlConnection, table2);
            JdbcUtil.dropTable(mysqlConnection, table3);
        }
    }

    private void prepareData() throws SQLException {
        String insertData =
            "insert into %s values (1, 'nihaore'), (2, 'nihaore '), (3, 'nihaOre'), (4, 'a'), (5, 'A'), (6, 'b')";
        String insertDataDelta = "insert into %s values (12, 'test3 '), (13, 'test5'), (14, 'R'), (15, 'r')";
        try (Connection connection = getPolardbxConnection0(PropertiesUtil.polardbXAutoDBName1())) {
            JdbcUtil.executeSuccess(connection,
                String.format(CREATE_TABLE + PARTITION_INFO, table1, columnType, collation));
            JdbcUtil.executeSuccess(connection,
                String.format(CREATE_TABLE + PARTITION_INFO, table2, columnType, collation));
            JdbcUtil.executeSuccess(connection,
                String.format(CREATE_TABLE + PARTITION_INFO, table3, columnType, collation));
            execute(connection, insertData);
            JdbcUtil.executeSuccess(connection, String.format("insert into %s values (11, 'CxC')", table1));
            JdbcUtil.executeSuccess(connection, String.format("insert into %s values (11, 'CDC')", table2));
            ColumnarUtils.createColumnarIndex(connection, "col_" + table1, table1, "c2", "c2", 4);
            ColumnarUtils.createColumnarIndexWithDictionary(connection, "col_" + table2, table2, "c2", "c2", 4, "c2");
            ColumnarUtils.createColumnarIndex(connection, "col_" + table3, table3, "c1", "c1", 4);
            execute(connection, insertDataDelta);
            waitForRowCountEquals(connection, table1, "col_" + table1);
            waitForRowCountEquals(connection, table2, "col_" + table2);
            waitForRowCountEquals(connection, table3, "col_" + table3);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, table1, columnType, collation));
            JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, table2, columnType, collation));
            JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, table3, columnType, collation));
            execute(mysqlConnection, insertData);
            JdbcUtil.executeSuccess(mysqlConnection, String.format("insert into %s values (11, 'CxC')", table1));
            JdbcUtil.executeSuccess(mysqlConnection, String.format("insert into %s values (11, 'CDC')", table2));
            execute(mysqlConnection, insertDataDelta);
        }
    }

    private void execute(Connection connection, String insertTemplate) {
        JdbcUtil.executeSuccess(connection, String.format(insertTemplate, table1));
        JdbcUtil.executeSuccess(connection, String.format(insertTemplate, table2));
        JdbcUtil.executeSuccess(connection, String.format(insertTemplate, table3));
    }

    @Before
    public void useDb() {
        JdbcUtil.useDb(tddlConnection, PropertiesUtil.polardbXAutoDBName1());
        JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
    }

    @Test
    public void testAll() {
        List<Throwable> allExceptions = new ArrayList<>();
        try {
            testFilter();
        } catch (Throwable t) {
            allExceptions.add(t);
        }
        try {
            testProject();
        } catch (Throwable t) {
            allExceptions.add(t);
        }
        try {
            testAgg();
        } catch (Throwable t) {
            allExceptions.add(t);
        }
        try {
            testJoin();
        } catch (Throwable t) {
            allExceptions.add(t);
        }
        try {
            testPairWiseJoin();
        } catch (Throwable t) {
            allExceptions.add(t);
        }
        try {
            testSort();
        } catch (Throwable t) {
            allExceptions.add(t);
        }

        if (!allExceptions.isEmpty()) {
            for (Throwable t : allExceptions) {
                t.printStackTrace();
            }
            throw new RuntimeException(allExceptions.get(0));
        }
    }

    public void testFilter() {
        String sql = "select * from %s where c2 = 'nihaore'";
        DataValidator.selectContentSameAssert(String.format(sql, table1), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table2), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table3), null, mysqlConnection, tddlConnection);
    }

    public void testProject() {
        String sql = "select concat(c2, 'test') from %s where c2 = 'nihaore'";
        DataValidator.selectContentSameAssert(String.format(sql, table1), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table2), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table3), null, mysqlConnection, tddlConnection);
    }

    public void testAgg() {
        String sql = "select count(*) from %s group by c2";
        DataValidator.selectContentSameAssert(String.format(sql, table1), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table2), null, mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table3), null, mysqlConnection, tddlConnection);
    }

    public void testJoin() {
        String sql = "select * from %s %s join %s on %s.c2 = %s.c2";
        DataValidator.selectContentSameAssert(String.format(sql, table1, "inner", table2, table1, table2), null,
            mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table2, "left", table1, table1, table2), null,
            mysqlConnection, tddlConnection);
        DataValidator.selectContentSameAssert(String.format(sql, table1, "right", table2, table1, table2), null,
            mysqlConnection, tddlConnection);
    }

    public void testPairWiseJoin() {
        String sql = "/*+TDDL:cmd_extra(ENABLE_BROADCAST_JOIN=false)*/ select * from %s %s join %s on %s.c2 = %s.c2";
        checkPairWiseJoin(String.format(sql, table1, "inner", table3, table1, table3));
        checkPairWiseJoin(String.format(sql, table3, "left", table1, table1, table3));
        checkPairWiseJoin(String.format(sql, table1, "right", table3, table1, table3));
    }

    private void checkPairWiseJoin(String sql) {
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        String explain = JdbcUtil.getExplainResult(tddlConnection, sql).toLowerCase();
        // explain should contains partition exchange and remote pairwise
        Assert.assertTrue(explain.contains("distribution=hash") && explain.contains("remote")
            && StringUtils.countMatches(explain, "exchange") == 1, "but was " + explain);
    }

    public void testSort() throws SQLException {
        String sql = "select c2 from %s where c2 in ('a', 'b') order by c2 ";
        checkSortResult(tddlConnection, String.format(sql, table1));
        checkSortResult(tddlConnection, String.format(sql, table2));
        checkSortResult(tddlConnection, String.format(sql, table3));
    }

    private void checkSortResult(Connection connection, String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        List<String> results = new ArrayList<>();
        while (rs.next()) {
            results.add(rs.getString(1));
        }
        if (compatible) {
            Assert.assertTrue(results.size() == 3,
                "should get three matched result, but was " + String.join(",", results));
            Assert.assertTrue(results.get(0).equalsIgnoreCase("a"), "should get a, but was " + results.get(0));
            Assert.assertTrue(results.get(1).equalsIgnoreCase("a"), "should get a, but was " + results.get(1));
            Assert.assertTrue(results.get(2).equalsIgnoreCase("b"), "should get a, but was " + results.get(2));
        } else {
            Assert.assertTrue(results.size() == 2,
                "should get three matched result, but was " + String.join(",", results));
            Assert.assertTrue(results.get(0).equalsIgnoreCase("a"), "should get a, but was " + results.get(0));
            Assert.assertTrue(results.get(1).equalsIgnoreCase("b"), "should get a, but was " + results.get(1));
        }
    }
}
