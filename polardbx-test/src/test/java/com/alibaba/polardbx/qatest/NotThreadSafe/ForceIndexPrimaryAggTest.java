package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

@NotThreadSafe
public class ForceIndexPrimaryAggTest extends ReadBaseTestCase {
    final static String SINGLE_TABLE = "force_index_primary_single";
    final static String SHARDING_TABLE = "force_index_primary_sharding";
    final static String CREATE_SINGLE_TABLE =
        "create table if not exists " + SINGLE_TABLE + "(id int primary key, a int, key idx(a))";
    final static String CREATE_SHARDING_TABLE =
        "create table if not exists " + SHARDING_TABLE + "(id int primary key, a int, key idx(a))";
    final static String PART_INFO = "dbpartition by hash(id)";
    final static String DROP_TABLE = "drop table if exists ";
    final static int TABLE_SIZE = 100;
    final static String ENABLE_FORCE_INDEX_HINT =
        "/*+TDDL:ENABLE_FORCE_PRIMARY_FOR_TSO=true ENABLE_FORCE_PRIMARY_FOR_FILTER=false*/";
    final static String ENABLE_FORCE_INDEX_FOR_FILTER_HINT =
        "/*+TDDL:ENABLE_FORCE_PRIMARY_FOR_TSO=true ENABLE_FORCE_PRIMARY_FOR_FILTER=true*/";
    final static String DISABLE_FORCE_INDEX_HINT =
        "/*+TDDL:ENABLE_FORCE_PRIMARY_FOR_TSO=false ENABLE_FORCE_PRIMARY_FOR_FILTER=true*/";
    final static String EMPTY_HINT = "";
    final static List<String> HINT_SQL = ImmutableList.of(
        ENABLE_FORCE_INDEX_HINT,
        ENABLE_FORCE_INDEX_FOR_FILTER_HINT,
        DISABLE_FORCE_INDEX_HINT,
        EMPTY_HINT
    );
    final static List<String> TABLES = ImmutableList.of(
        SINGLE_TABLE,
        SHARDING_TABLE
    );
    final static List<String> WHERE_CLAUSE = ImmutableList.of(
        "where id > 0",
        ""
    );
    final static List<String> TEST_SQL = ImmutableList.of(
        // {0} is hint, {1} is table name, {2} is where clause
        "{0}select count(0) from {1} {2}",
        "{0}select min(a) from {1} {2}",
        "{0}select max(a) from {1} {2}",
        "{0}select sum(id) from {1} {2}",
        "{0}select avg(id) from {1} {2}",
        "{0}select std(id) from {1} {2}",
        "{0}select stddev(id) from {1} {2}",
        "{0}select stddev_samp(id) from {1} {2}",
        "{0}select var_pop(id) from {1} {2}",
        "{0}select var_samp(id) from {1} {2}",
        "{0}select variance(id) from {1} {2}",
        "{0}select count(0) from {1} {2} group by id order by count(0)",
        "{0}select count(0) from {1} {2} group by a order by count(0)",
        "{0}select count from (select count(0) as count from {1} {2}) as t order by count",
        "{0}select count from (select count(0) as count from {1} {2} group by a having count(id) > 0) as t order by count"
    );

    public void before() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, DROP_TABLE + SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, DROP_TABLE + SHARDING_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE + SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE + SHARDING_TABLE);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, CREATE_SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, CREATE_SHARDING_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_SHARDING_TABLE + PART_INFO);
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < TABLE_SIZE; i++) {
            values.append("(").append(i).append(", ").append((100 + i)).append(")");
            if (i < TABLE_SIZE - 1) {
                values.append(", ");
            }
        }
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "insert into " + SINGLE_TABLE + " values " + values);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "insert into " + SHARDING_TABLE + " values " + values);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + SINGLE_TABLE + " values " + values);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + SHARDING_TABLE + " values " + values);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_FILTER=false");
    }

    public void after() {
        JdbcUtil.executeUpdate(mysqlConnection, DROP_TABLE + SINGLE_TABLE, false, true);
        JdbcUtil.executeUpdate(mysqlConnection, DROP_TABLE + SHARDING_TABLE, false, true);
        JdbcUtil.executeUpdate(tddlConnection, DROP_TABLE + SINGLE_TABLE, false, true);
        JdbcUtil.executeUpdate(tddlConnection, DROP_TABLE + SHARDING_TABLE, false, true);
        JdbcUtil.executeUpdate(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true", false, true);
        JdbcUtil.executeUpdate(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_FILTER=false", false, true);
    }

    @Test
    public void test() throws SQLException {
        try {
            before();

            final String[] planCacheOptions = {"true", "false"};

            for (final String opt : planCacheOptions) {
                try {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, "set PLAN_CACHE=" + opt);
                    if (!isMySQL80()) {
                        testForceIndex();
                        testGlobalSwitch();
                        testDistinct();
                    } else {
                        test80();
                    }
                } finally {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, "set PLAN_CACHE=true");
                }
            }

        } finally {
            after();
        }
    }

    public void testForceIndex() {
        TEST_SQL.forEach(sql ->
            HINT_SQL.forEach(hint ->
                TABLES.forEach(table ->
                    WHERE_CLAUSE.forEach(where -> {
                        if (table.equalsIgnoreCase(SHARDING_TABLE) && sql.contains("stddev_samp")
                            || table.equalsIgnoreCase(SHARDING_TABLE) && sql.contains("var_samp")) {
                            // These agg function is not yet implemented on CN.
                        } else {
                            testSql(MessageFormat.format(sql, hint, table, where));
                        }
                    })
                )
            )
        );
    }

    private void testSql(String sql) {
        try {
            final List<Float> expected = getResult(sql, mysqlConnection);

            // For TSO.
            try {
                JdbcUtil.executeUpdate(tddlConnection, "set transaction_policy=TSO");
                JdbcUtil.executeUpdate(tddlConnection, "begin");
                final List<Float> result = getResult(sql, tddlConnection);
                Assert.assertTrue(compareList(expected, result),
                    "[TSO]Execute sql: " + sql + ", got unexpected result: " + result + ", expected: " + expected);
            } finally {
                JdbcUtil.executeUpdate(tddlConnection, "rollback");
            }

            // For XA.
            try {
                JdbcUtil.executeUpdate(tddlConnection, "set transaction_policy=\"XA\"");
                JdbcUtil.executeUpdate(tddlConnection, "begin");
                final List<Float> result = getResult(sql, tddlConnection);
                Assert.assertTrue(compareList(expected, result),
                    "[XA]Execute sql: " + sql + ", got unexpected result: " + result + ", expected: " + expected);
            } finally {
                JdbcUtil.executeUpdate(tddlConnection, "rollback");
            }

            // For autocommit.
            final List<Float> result = getResult(sql, tddlConnection);
            Assert.assertTrue(compareList(expected, result),
                "[None]Execute sql: " + sql + ", got unexpected result: " + result + ", expected: " + expected);
        } catch (Throwable t) {
            Assert.fail("Error when executing sql: " + sql + ", error message: " + t.getMessage());
        }
    }

    private List<Float> getResult(String sql, Connection conn) throws SQLException {
        List<Float> results = new ArrayList<>();
        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            while (rs.next()) {
                results.add(rs.getFloat(1));
            }
        }
        return results;
    }

    private boolean compareList(List<Float> l1, List<Float> l2) {
        if (l1.size() != l2.size()) {
            return false;
        }
        for (int i = 0; i < l1.size(); i++) {
            if (!l1.get(i).equals(l2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public void testGlobalSwitch() throws SQLException {
        final String sql0 = "explain select count(0) from {0}";
        final String sql1 = "explain select count(0) from {0} where id > 0";

        for (String table : TABLES) {
            JdbcUtil.executeUpdate(tddlConnection, "set transaction_policy=TSO");
            testsql(MessageFormat.format(sql0, table), MessageFormat.format(sql1, table));
        }
    }

    private void testDistinct() throws SQLException {
        final String sql0 = "explain select count(distinct id) from {0}";
        for (String table : TABLES) {
            try {
                // Enable optimization for TSO without filter.
                System.out.println("Test distinct");
                JdbcUtil.executeUpdate(tddlConnection, "set transaction_policy=TSO");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true");
                JdbcUtil.executeUpdate(tddlConnection, "begin");

                boolean found = isFoundForceIndex(MessageFormat.format(sql0, table));
                Assert.assertTrue(!found, "Found force index(primary), which should not happen, sql: " + sql0);
            } finally {
                JdbcUtil.executeUpdate(tddlConnection, "rollback");
            }
        }
    }

    private void test80() throws SQLException {
        final String sql0 = "explain select count(0) from {0}";
        for (String table : TABLES) {
            try {
                // Enable optimization for TSO without filter.
                System.out.println("Test 8032 should not force primary");
                JdbcUtil.executeUpdate(tddlConnection, "set transaction_policy=TSO");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true");
                JdbcUtil.executeUpdate(tddlConnection, "begin");

                boolean found = isFoundForceIndex(MessageFormat.format(sql0, table));
                Assert.assertTrue(!found, "Found force index(primary), which should not happen, sql: " + sql0);
            } finally {
                JdbcUtil.executeUpdate(tddlConnection, "rollback");
            }
        }
    }

    /**
     * @param sql0 select count from tb
     * @param sql1 select count from tb where ...
     */
    private void testsql(String sql0, String sql1) throws SQLException {
        try {
            // Enable optimization for TSO without filter.
            System.out.println(
                "Test global switch, ENABLE_FORCE_PRIMARY_FOR_TSO = true, ENABLE_FORCE_PRIMARY_FOR_FILTER = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_FILTER=false");
            JdbcUtil.executeUpdate(tddlConnection, "begin");

            boolean found = isFoundForceIndex(sql0);
            Assert.assertTrue(found, "Not found any force index(primary), sql: " + sql0);

            // Filter should not be optimized.
            found = isFoundForceIndex(sql1);
            Assert.assertTrue(!found, "Found force index(primary), which should not happen, sql: " + sql1);
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "rollback");
        }

        try {
            // Enable optimization for TSO with filter.
            System.out.println("Test global switch, ENABLE_FORCE_PRIMARY_FOR_FILTER = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_FILTER=true");
            JdbcUtil.executeUpdate(tddlConnection, "clear plancache");
            JdbcUtil.executeUpdate(tddlConnection, "begin");

            // Filter should also be optimized.
            boolean found = isFoundForceIndex(sql1);
            Assert.assertTrue(found, "Not found any force index(primary)");
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "rollback");
            // Restore config.
            JdbcUtil.executeUpdate(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_FILTER=false");
            JdbcUtil.executeUpdate(tddlConnection, "clear plancache");
        }

        try {
            // Disable optimization for TSO.
            System.out.println("Test global switch, ENABLE_FORCE_PRIMARY_FOR_TSO = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=false");
            JdbcUtil.executeUpdate(tddlConnection, "begin");

            boolean found = isFoundForceIndex(sql0);
            Assert.assertTrue(!found, "Found force index(primary), which should not happen.");
            found = isFoundForceIndex(sql1);
            Assert.assertTrue(!found, "Found force index(primary), which should not happen.");
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "rollback");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_FORCE_PRIMARY_FOR_TSO=true");
        }
    }

    private boolean isFoundForceIndex(String sql) throws SQLException {
        System.out.println("Execute SQL: " + sql);
        boolean found = false;
        System.out.println("Explain result:");
        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (rs.next()) {
                final String explainResult = rs.getString(1);
                System.out.println(explainResult);
                if (StringUtils.containsIgnoreCase(explainResult, "force index(primary)")) {
                    found = true;
                }
            }
        }
        System.out.println("");
        return found;
    }
}
