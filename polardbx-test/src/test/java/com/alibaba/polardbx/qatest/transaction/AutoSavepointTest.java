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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

import static com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase.quoteSpecialName;

@NotThreadSafe
@Ignore("wait for new XProtocol")
public class AutoSavepointTest extends CrudBasedLockTestCase {
    private final String primaryName;
    private final String gsiName;
    private final String mysqlName;
    private final String trxPolicy;
    private Connection myPolarXConn;
    private Connection myMysqlConn;
    // Call getIncKey() instead of using {globalIncKey} directly.
    private int globalIncKey;
    // Initially, we insert 0, ..., {initialMaxKey} into table.
    private final int initialMaxKey = 15;

    private final String insertTemplate = "INSERT INTO {0} VALUES {1}";
    private final String updateTemplate = "UPDATE {0} SET {1}";
    private Supplier<String> goodValuesSupplier;

    public AutoSavepointTest(String trxPolicy) {
        this.trxPolicy = trxPolicy;
        this.primaryName = "auto_savepoint_test_primary_table" + trxPolicy;
        this.gsiName = "auto_savepoint_test_gsi_table" + trxPolicy;
        this.mysqlName = "auto_savepoint_test_mysql_table" + trxPolicy;
    }

    @Parameterized.Parameters(name = "{index}:trxPolicy={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {"XA"}, new String[] {"TSO"});
    }

    /**
     * Create a new table with a unique index for PolarDB-X and MySQL, and insert some data.
     */
    @Before
    public void before() {
        this.myPolarXConn = getPolardbxConnection();
        this.myMysqlConn = getMysqlConnection();
        Assert.assertTrue(initialMaxKey > 1);

        JdbcUtil.executeUpdateSuccess(myPolarXConn, "set global enable_auto_savepoint = true");

        final String createTableTemplate = "CREATE TABLE {0} ({1} {2}) {3}";
        final String columnDef = "a int primary key, b int, c char(1), d int";
        final String partitionStr = " dbpartition by hash({0}) tbpartition by hash({0}) tbpartitions 2";
        final String uniqueIndexTddlStr =
            ", unique global index " + gsiName + " (b) covering (c) " + MessageFormat.format(partitionStr, "b");
        final String uniqueIndexMysqlStr = ", unique key(b)";

        // Create table with columns a, b, c which is partitioned by a.
        // And create a UGSI on b.
        final String createTddlTableSql = MessageFormat.format(
            createTableTemplate, primaryName, columnDef, uniqueIndexTddlStr, MessageFormat.format(partitionStr, "a"));
        final String createMysqlTableSql = MessageFormat.format(
            createTableTemplate, mysqlName, columnDef, uniqueIndexMysqlStr, "");
        // Create table.
        dropTableWithGsi(primaryName, ImmutableList.of(gsiName));
        JdbcUtil.executeUpdateSuccess(myPolarXConn, createTddlTableSql);
        dropTableIfExists(myMysqlConn, mysqlName);
        JdbcUtil.executeUpdateSuccess(myMysqlConn, createMysqlTableSql);

        // Init data.
        final List<String> initialValues = new ArrayList<>();
        for (int i = 0; i <= initialMaxKey; i++) {
            initialValues.add(getValuesStr(Arrays.asList(
                Integer.toString(i), // a
                Integer.toString(i), // b
                "'0'", // c
                Integer.toString(i) // d
            )));
        }
        final String batchValuesStr = String.join(",", initialValues);
        JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, batchValuesStr));
        JdbcUtil.executeUpdateSuccess(myMysqlConn, getInsertSql(mysqlName, batchValuesStr));

        // Init global increment key.
        this.globalIncKey = initialMaxKey + 1;

        // Default goodValues supplier: only contains four columns: (a, b, c, d).
        goodValuesSupplier = () -> getValuesStr(Arrays.asList(
            getIncKey(), // a
            getIncKey(), // b
            "'0'",       // c
            getIncKey()  // d
        ));
    }

    private void dropTableIfExists(Connection conn, String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private void dropTableWithGsi(String primary, List<String> indexNames) {
        final String finalPrimary = quoteSpecialName(primary);
        try (final Statement stmt = myPolarXConn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + finalPrimary);
            for (String gsi : Optional.ofNullable(indexNames).orElse(ImmutableList.of())) {
                stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(gsi));
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @After
    public void after() throws SQLException {
        dropTableIfExists(myMysqlConn, mysqlName);
        dropTableWithGsi(primaryName, ImmutableList.of(gsiName));
        myPolarXConn.close();
        myMysqlConn.close();
    }

    private String getIncKey() {
        return String.valueOf(globalIncKey++);
    }

    private String getValuesStr(Collection<String> values) {
        return "(" + String.join(",", values) + ")";
    }

    private String getInsertSql(String tableStr, String values) {
        return MessageFormat.format(insertTemplate, tableStr, values);
    }

    private String getTddlUpdateSql(String updateStr) {
        return MessageFormat.format(updateTemplate, primaryName, updateStr);
    }

    private String getMysqlUpdateSql(String updateStr) {
        return MessageFormat.format(updateTemplate, mysqlName, updateStr);
    }

    private void testFramework(Runnable actionsInTrx) throws SQLException {
        try {
            myPolarXConn.setAutoCommit(false);
            myMysqlConn.setAutoCommit(false);
            final String setTrxPolicySql = String.format("set drds_transaction_policy= %s", trxPolicy);
            JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

            // Do something in a trx.
            actionsInTrx.run();
        } finally {
            myPolarXConn.setAutoCommit(true);
            myMysqlConn.setAutoCommit(true);
        }

        compareResultWithMysql();
    }

    @Test
    public void testSimpleCases() throws SQLException {
        testFramework(() -> {
            insertGoodValuesToTddlAndMysql();
            // Insert "(x, x, '00', x)" should fail with data too long for column c.
            expectInsertFailed(getValuesStr(Arrays.asList(getIncKey(), getIncKey(), "'00'", getIncKey())),
                "Data too long for column 'c'");

            insertGoodValuesToTddlAndMysql();
            // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
            expectInsertFailed(getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey())),
                "Duplicate entry '1'");

            insertGoodValuesToTddlAndMysql();
            // Insert "(x, 1, '0', x)" should fail with duplicate entry 1.
            expectInsertFailed(getValuesStr(Arrays.asList(getIncKey(), "1", "'0'", getIncKey())),
                "Duplicate entry '1'");

            insertGoodValuesToTddlAndMysql();
            // Insert "(x, x, '0', 10000000000000)" should fail with out of range value for column d.
            expectInsertFailed(getValuesStr(Arrays.asList(getIncKey(), getIncKey(), "'0'", "10000000000000")),
                "Out of range value for column 'd'");

            insertGoodValuesToTddlAndMysql();
            // Special case: insert uk out of range should fail for tddl with "[ERR_CONVERTOR]",
            // but Mysql has another error of "out of range".
            expectInsertFailed(getValuesStr(Arrays.asList("10000000000000", getIncKey(), "'0'", getIncKey())),
                "[ERR_CONVERTOR]",
                "Out of range value for column 'a'");
            expectInsertFailed(getValuesStr(Arrays.asList(getIncKey(), "-10000000000000", "'0'", getIncKey())),
                "[ERR_CONVERTOR]",
                "Out of range value for column 'b'");

            insertGoodValuesToTddlAndMysql();
            // Update causes duplicate entry.
            final String key = getIncKey();
            insertValuesToTddlAndMysql(getValuesStr(Arrays.asList(key, key, "'0'", key)));
            expectUpdateFailed("a = a + 1 WHERE a = 0 or a = " + key, "Duplicate entry");

            insertGoodValuesToTddlAndMysql();
        });
    }

    private void expectInsertFailed(String badValues, String tddlError) {
        // Assert the same error.
        expectInsertFailed(badValues, tddlError, tddlError);
    }

    private void expectInsertFailed(String badValues, String tddlError, String mysqlError) {
        badValues = wrapWithGoodValues(badValues);

        JdbcUtil.executeUpdateFailed(myPolarXConn, getInsertSql(primaryName, badValues), tddlError);

        JdbcUtil.executeUpdateFailed(myMysqlConn, getInsertSql(mysqlName, badValues), mysqlError);
    }

    private void expectUpdateFailed(String updateStr, String error) {
        JdbcUtil.executeUpdateFailed(myPolarXConn, getTddlUpdateSql(updateStr), error);

        JdbcUtil.executeUpdateFailed(myMysqlConn, getMysqlUpdateSql(updateStr), error);
    }

    private String wrapWithGoodValues(String badValues) {
        return String.join(",", goodValuesSupplier.get(), badValues);
    }

    private void insertGoodValuesToTddlAndMysql() {
        insertValuesToTddlAndMysql(goodValuesSupplier.get());
    }

    private void insertValuesToTddlAndMysql(String values) {
        // Insert into tddl.
        try {
            JdbcUtil.executeUpdate(myPolarXConn, getInsertSql(primaryName, values));
        } catch (Throwable ignore) {
            // Ignore.
        }
        // Insert into mysql.
        try {
            JdbcUtil.executeUpdate(myMysqlConn, getInsertSql(mysqlName, values));
        } catch (Throwable ignore) {
            // Ignore.
        }
    }

    @Test
    public void testRandomCases() throws SQLException {
        testFramework(() -> {
            final Random random = new Random();
            // 15% probability to create a duplicate entry.
            final float duplicateProb = 0.15f;
            final int repeatTime = 20;
            for (int i = 0; i < repeatTime; i++) {
                final int batchSize = 4;
                final List<String> batchValues = new ArrayList<>(batchSize);
                for (int j = 0; j < batchSize; j++) {
                    if (random.nextFloat() < duplicateProb) {
                        final String duplicateEntry = String.valueOf(random.nextInt(initialMaxKey));
                        if (random.nextBoolean()) {
                            // Duplicate primary key.
                            batchValues
                                .add(getValuesStr(Arrays.asList(duplicateEntry, getIncKey(), "'0'", getIncKey())));
                        } else {
                            // Duplicate unique key.
                            batchValues
                                .add(getValuesStr(Arrays.asList(getIncKey(), duplicateEntry, "'0'", getIncKey())));
                        }
                    } else {
                        // Good values.
                        batchValues.add(goodValuesSupplier.get());
                    }
                }
                final String batchValueStr = String.join(",", batchValues);
                insertValuesToTddlAndMysql(batchValueStr);
            }
        });
    }

    @Test
    public void testTime() throws SQLException {
        // Add datetime and time columns.
        final String alterTemplate = "ALTER TABLE {0} ADD COLUMN time1 datetime, ADD COLUMN time2 time";
        JdbcUtil.executeUpdateSuccess(myPolarXConn, MessageFormat.format(alterTemplate, primaryName));
        JdbcUtil.executeUpdateSuccess(myMysqlConn, MessageFormat.format(alterTemplate, mysqlName));

        goodValuesSupplier = () -> getTimeValueStr("now()", "now()");

        testFramework(() -> {
            insertGoodValuesToTddlAndMysql();

            // Insert "time1 = bad" should fail with incorrect datetime value.
            expectInsertFailed(getTimeValueStr("'bad'", "now()"), "Incorrect datetime value");

            // Insert "time2 = bad" should fail with incorrect time value.
            expectInsertFailed(getTimeValueStr("now()", "'bad'"), "Incorrect time value");

            insertGoodValuesToTddlAndMysql();
        });
    }

    private String getTimeValueStr(String time1, String time2) {
        // Columns: a, b, c, d, time1, time2
        return getValuesStr(Arrays.asList(getIncKey(), getIncKey(), "'0'", getIncKey(), time1, time2));
    }

    @Test
    public void testEnumAndSet() throws SQLException {
        // Add enum column.
        final String alterTemplate =
            "ALTER TABLE {0} ADD COLUMN enum1 enum(''0'', ''1''), ADD COLUMN set1 set(''0'', ''1'')";
        JdbcUtil.executeUpdateSuccess(myPolarXConn, MessageFormat.format(alterTemplate, primaryName));
        JdbcUtil.executeUpdateSuccess(myMysqlConn, MessageFormat.format(alterTemplate, mysqlName));

        goodValuesSupplier = () -> getEnumValueStr("'0'", "'0'");

        testFramework(() -> {
            insertGoodValuesToTddlAndMysql();
            // Insert "enum1 = '1000000'" should fail with data truncated for column 'enum1'.
            expectInsertFailed(getEnumValueStr("'1000000'", "'0'"), "Data truncated for column 'enum1'");

            insertGoodValuesToTddlAndMysql();
            // Insert "set1 = '1000000'" should fail with data truncated for column 'set1'.
            expectInsertFailed(getEnumValueStr("'0'", "'1000000'"), "Data truncated for column 'set1'");
        });
    }

    private String getEnumValueStr(String enum1, String set1) {
        // Columns: a, b, c, d, enum1, set1
        return getValuesStr(Arrays.asList(getIncKey(), getIncKey(), "'0'", getIncKey(), enum1, set1));
    }

    private void compareResultWithMysql() throws SQLException {
        final String selectTemplate = "SELECT * FROM {0} ORDER BY a, b, c, d";
        try (final ResultSet tddlRs = JdbcUtil.executeQuerySuccess(myPolarXConn,
            MessageFormat.format(selectTemplate, primaryName));
            final ResultSet mysqlRs = JdbcUtil.executeQuerySuccess(myMysqlConn,
                MessageFormat.format(selectTemplate, mysqlName))) {
            while (tddlRs.next()) {
                Assert.assertTrue("result set size not equal: tddl > mysql", mysqlRs.next());

                Assert.assertEquals("column a not equal",
                    tddlRs.getString("a"), mysqlRs.getString("a"));
                Assert.assertEquals("column b not equal",
                    tddlRs.getString("b"), mysqlRs.getString("b"));
                Assert.assertEquals("column c not equal",
                    tddlRs.getString("c"), mysqlRs.getString("c"));
                Assert.assertEquals("column d not equal",
                    tddlRs.getString("d"), mysqlRs.getString("d"));
            }

            Assert.assertFalse("result set size not equal: tddl < mysql", mysqlRs.next());
        }
    }

    @Test
    public void testAutoSavepointSwitch() throws SQLException {
        try {
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = false");
            try {
                myPolarXConn.setAutoCommit(false);
                final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
                JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

                // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Since we turn off the auto savepoint option, it should fail with
                // "cannot continue or commit transaction after writing failed".
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, goodValuesSupplier.get()),
                    "cannot continue or commit transaction after writing failed.");
            } finally {
                myPolarXConn.rollback();
                myPolarXConn.setAutoCommit(true);
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = true");
        }
    }

    /**
     * Test set global value and set session value.
     */
    @Test
    public void testAutoSavepointSwitch2() throws SQLException {
        try {
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = false");
            try {
                myPolarXConn.setAutoCommit(false);
                final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
                JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

                // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Since we turn off the auto savepoint option, it should fail with
                // "cannot continue or commit transaction after writing failed".
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, goodValuesSupplier.get()),
                    "cannot continue or commit transaction after writing failed.");
                // Now, change this session value to true
                JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = true");
                // And it should not work for this transaction, but the next one.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, goodValuesSupplier.get()),
                    "cannot continue or commit transaction after writing failed.");
            } finally {
                myPolarXConn.rollback();
                myPolarXConn.setAutoCommit(true);
            }
            // enable_auto_savepoint = true should work for the next transaction
            try {
                myPolarXConn.setAutoCommit(false);
                final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
                JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

                // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Insert good values should succeed.
                JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
                // Now, change this session value to false and should not work for this transaction
                JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = false");
                // Fail for duplicate entry.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Transaction can still continue.
                JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            } finally {
                myPolarXConn.rollback();
                myPolarXConn.setAutoCommit(true);
            }

            // Test set global.
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_set_global = true");
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set global enable_auto_savepoint = false");
            try {
                myPolarXConn.setAutoCommit(false);
                final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
                JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

                // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Since we turn off the auto savepoint option, it should fail with
                // "cannot continue or commit transaction after writing failed".
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, goodValuesSupplier.get()),
                    "cannot continue or commit transaction after writing failed.");
            } finally {
                myPolarXConn.rollback();
                myPolarXConn.setAutoCommit(true);
            }
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set global enable_auto_savepoint = true");
            try {
                myPolarXConn.setAutoCommit(false);
                final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
                JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

                // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
                JdbcUtil.executeUpdateFailed(myPolarXConn,
                    getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                    "Duplicate entry '1'");
                // Insert good values should succeed.
                JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            } finally {
                myPolarXConn.rollback();
                myPolarXConn.setAutoCommit(true);
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "set enable_auto_savepoint = true");
        }
    }

    /**
     * Case 0: releasing auto-savepoint should not release user-savepoint
     * conn0              conn1
     * (auto sp 0)
     * sql0
     * (release sp 0)
     * <p>
     * SET sp A           SET sp A
     * <p>
     * (auto sp 1)
     * sql1
     * (release sp 1)
     * <p>
     * RELEASE sp A       RELEASE sp A
     */
    @Test
    public void testUserSavepoints0() throws SQLException {
        try {
            myPolarXConn.setAutoCommit(false);
            final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
            JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "SAVEPOINT USER_SP_A");
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "RELEASE SAVEPOINT USER_SP_A");
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));

            // Auto savepoint should work well.
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "SAVEPOINT USER_SP_B");
            // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
            JdbcUtil.executeUpdateFailed(myPolarXConn,
                getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                "Duplicate entry '1'");
            // Insert good values should succeed.
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "RELEASE SAVEPOINT USER_SP_B");
        } finally {
            myPolarXConn.rollback();
            myPolarXConn.setAutoCommit(true);
        }
    }

    /**
     * Case 1: releasing user-savepoint should not release auto-savepoint
     * conn0              conn1
     * SET sp A           SET sp A
     * <p>
     * (auto sp 0)
     * sql0
     * (release sp 0)
     * <p>
     * RELEASE sp A       RELEASE sp A
     * <p>
     * (auto sp 1)
     * sql1
     * (release sp 1)
     */
    @Test
    public void testUserSavepoints1() throws SQLException {
        try {
            myPolarXConn.setAutoCommit(false);
            final String setTrxPolicySql = String.format("set drds_transaction_policy = %s", trxPolicy);
            JdbcUtil.executeUpdateSuccess(myPolarXConn, setTrxPolicySql);

            JdbcUtil.executeUpdateSuccess(myPolarXConn, "SAVEPOINT USER_SP_A");
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "RELEASE SAVEPOINT USER_SP_A");
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));

            // Auto savepoint should work well.
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "SAVEPOINT USER_SP_B");
            // Insert "(1, x, '0', x)" should fail with duplicate entry 1.
            JdbcUtil.executeUpdateFailed(myPolarXConn,
                getInsertSql(primaryName, getValuesStr(Arrays.asList("1", getIncKey(), "'0'", getIncKey()))),
                "Duplicate entry '1'");
            // Insert good values should succeed.
            JdbcUtil.executeUpdateSuccess(myPolarXConn, getInsertSql(primaryName, goodValuesSupplier.get()));
            JdbcUtil.executeUpdateSuccess(myPolarXConn, "RELEASE SAVEPOINT USER_SP_B");
        } finally {
            myPolarXConn.rollback();
            myPolarXConn.setAutoCommit(true);
        }
    }

}
