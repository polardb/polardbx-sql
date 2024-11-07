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

package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertWithMessage;

public class OnlineModifyColumnTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";
    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String COL_SKIP_BACK_FILL = "COL_SKIP_BACK_FILL=TRUE";
    private static final String COL_USE_SIMPLE_CHECKER = "COL_USE_SIMPLE_CHECKER=TRUE";
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testOnlineModifyColumnOnTableWithGsi() {
        String tableName = "omc_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(" create unique clustered index `%s` on %s (`b`)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column c bigint", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnOnTableWithGsi2() {
        String tableName = "omc_gsi_test_tbl_f" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_f_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`a`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create global index `%s` on %s (`b`) covering (`c`) partition by hash(`b`)",
            indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column c bigint", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnOnTableWithGsi3() {
        String tableName = "omc_gsi_test_tbl_auto" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_auto_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add index %s(a,b)", tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b int not null", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnLocalIndex() {
        String tableName = "omc_test_tbl_li_f" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_test_tbl_li_f_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b varchar(255), c int) partition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // BLOB/TEXT column 'xxx' used in key specification without a key length
        sql = String.format("alter table %s modify column b text", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column b blob(10)", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("drop index %s on %s", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b(10))", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b text", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("alter table %s modify column b blob(10)", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnOnMultipleColumnsSuccess() {
        String tableName = "omc_multi_col_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`b`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint, modify column c bigint", tableName)
            + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testOnlineModifyColumnRollback() {
        String tableName = "omc_rollback_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b tinyint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column c smallint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column d mediumint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column e int,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "99999");
            Assert.assertEquals(rs.getString(3), "-99999");
            Assert.assertEquals(rs.getString(4), "127");
            Assert.assertEquals(rs.getString(5), "-128");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnLocalIndexRollback() {
        String tableName = "omc_rollback_li_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_rollback_li_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, null, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b varchar(0),", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column c smallint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column d mediumint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column e int,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(3), "-99999");
            Assert.assertEquals(rs.getString(4), "127");
            Assert.assertEquals(rs.getString(5), "-128");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnFulltextIndex() {
        String tableName = "omc_rollback_fi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_rollback_fi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int, c int, d int, e int, f varchar(64)) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create fulltext index %s on %s(f)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128, '123')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column d bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column e bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "99999");
            Assert.assertEquals(rs.getString(3), "-99999");
            Assert.assertEquals(rs.getString(4), "127");
            Assert.assertEquals(rs.getString(5), "-128");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Ignore
    @Test
    public void testOnlineModifyColumnSimpleCheckerTest() {
        String tableName = "omc_simple_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(`a`) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(COL_SKIP_BACK_FILL, COL_USE_SIMPLE_CHECKER) + String.format(
            "alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        rollbackDdl(tddlDatabase1, tableName, tddlConnection);

        sql = buildCmdExtra(COL_USE_SIMPLE_CHECKER) + String.format("alter table %s modify column b bigint,", tableName)
            + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Ignore
    @Test
    public void testOnlineModifyColumnCheckerTest() {
        String[] checkerPolicies = new String[] {
            "OMC_CHECKER_CONCURRENT_POLICY=TABLE_CONCURRENT", "OMC_CHECKER_CONCURRENT_POLICY=DB_CONCURRENT",
            "OMC_CHECKER_CONCURRENT_POLICY=INSTANCE_CONCURRENT"};

        for (String checkerPolicy : checkerPolicies) {
            String tableName = "omc_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
            dropTableIfExists(tableName);
            String sql = String.format(
                "create table %s (a int primary key, b int) partition by hash(a) partitions 13",
                tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("insert into table %s values (0, 1)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = buildCmdExtra(COL_SKIP_BACK_FILL, checkerPolicy) + String.format(
                "alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
            rollbackDdl(tddlDatabase1, tableName, tddlConnection);

            sql = buildCmdExtra(checkerPolicy) + String.format("alter table %s modify column b bigint,", tableName)
                + USE_OMC_ALGORITHM;
            execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

            sql = String.format("select * from %s where a=0", tableName);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            try {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0");
                Assert.assertEquals(rs.getString(2), "1");
            } catch (SQLException e) {
                throw new RuntimeException("", e);
            } finally {
                JdbcUtil.close(rs);
            }
        }
    }

    @Test
    public void testShowTable() throws Exception {
        String tableName = "omc_show_table_tbl";
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testShowTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("show create table %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        rs.next();
        final String tableDef = rs.getString(2);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        rs1.next();
                        Assert.assertEquals(rs1.getString(2), tableDef);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        JdbcUtil.executeSuccess(tddlConnection, alterSql);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testShowColumns() throws Exception {
        String tableName = "omc_show_columns_tbl" + RandomUtils.getStringBetween(1, 5);

        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testShowColumnsInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("show full columns from %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        List<List<Object>> results = JdbcUtil.getAllResult(rs);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        List<List<Object>> results1 = JdbcUtil.getAllResult(rs1);
                        assertWithMessage("列信息发生改变")
                            .that(results)
                            .containsExactlyElementsIn(results1);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testDescTable() throws Exception {
        String tableName = "omc_desc_table_tbl" + RandomUtils.getStringBetween(1, 5);

        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testDescTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("desc %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        List<List<Object>> results = JdbcUtil.getAllResult(rs);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        List<List<Object>> results1 = JdbcUtil.getAllResult(rs1);
                        assertWithMessage("列信息发生改变")
                            .that(results)
                            .containsExactlyElementsIn(results1);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testOnlineModifyColumnLocalIndexTest() {
        String tableName = "omc_local_index_rollback_test";
        String indexName = tableName + "_idx";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(a) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnLocalIndexTestAutoPartition() {
        String tableName = "omc_local_index_rollback_test_ap";
        String indexName = tableName + "_idx";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnNullColumn() {
        String tableName = "omc_null_column";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int ) partition by hash(a) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,1),(2,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint null,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("select * from %s where a=2", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "2");
            Assert.assertNull(rs.getString(2));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnColumnName() {
        String tableName = "```omc_column_name```";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(a) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
        try {
            sql = String.format("alter table %s change column b ```c``` bigint not null,", tableName)
                + USE_OMC_ALGORITHM;
            execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

            sql = String.format("select * from %s", tableName);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            try {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0");
                Assert.assertEquals(rs.getString(2), "1");
            } catch (SQLException e) {
                throw new RuntimeException("", e);
            } finally {
                JdbcUtil.close(rs);
            }
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }

    @Test
    public void testTableStatistic() {
        String tableName = "omc_table_statistic_modify";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(a) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("analyze table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and HISTOGRAM is not null",
            tddlDatabase1, tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and HISTOGRAM is not null",
            tddlDatabase1, tableName);
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getInt(1), 1);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs1);
        }
    }

    @Test
    public void testTableStatistic2() {
        String tableName = "omc_table_statistic_change";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(a) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("analyze table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and HISTOGRAM is not null",
            tddlDatabase1, tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("alter table %s change column b c bigint,", tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and HISTOGRAM is not null",
            tddlDatabase1, tableName);
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getInt(1), 1);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs1);
        }
    }

    @Test
    public void testExplainOMC() {
        String tableName = "omc_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b int, c int not null, d int not null default 23) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s modify column b bigint," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column b bigint not null," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column b bigint not null default 123," + USE_OMC_ALGORITHM,
            tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column c bigint," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column c bigint not null," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column c bigint not null default 123," + USE_OMC_ALGORITHM,
            tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column d bigint," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column d bigint not null," + USE_OMC_ALGORITHM, tableName);
        checkExplainCount(sql, 7);

        sql = String.format("explain alter table %s modify column d bigint not null default 123," + USE_OMC_ALGORITHM,
            tableName);
        checkExplainCount(sql, 7);
    }

    public void checkExplainCount(String sql, int expected) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(expected, count);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testOnlineModifyColumnFloatToDecimal() {
        String tableName = "omc_test_float" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b float(12,2)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values(1, 966717.88)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b decimal(12, 2)," + USE_OMC_ALGORITHM, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testOnlineModifyColumnWithUGsi() {
        String tableName = "omc_test_ugsi" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a char(32) primary key, b char(32), unique global index ugsi_b(b) partition by key(b) partitions 64) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName
            + " values('7e6654a4dadc99g248c8744028e362f0', '2a5996ac96da576b83b862s3g78ceddb')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table %s modify column a char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin," + USE_OMC_ALGORITHM,
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "select count(1) from %s force index(usgi_b) where b = '2a5996ac96da576b83b862s3g78ceddb'", tableName);
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getInt(1), 1);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs1);
        }
    }

    @Test
    public void testOnlineModifyColumnWithAutoIncrement() {
        String tableName = "omc_auto_increment" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int ) partition by hash(a) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a bigint auto_increment," + USE_OMC_ALGORITHM, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Missing sequence");

        sql = String.format("CREATE SEQUENCE `AUTO_SEQ_%s`", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a bigint auto_increment," + USE_OMC_ALGORITHM, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s(b) values (null),(null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testOnlineModifyColumnWithNullAble() {
        String tableName = "omc_nullable" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format(
                "create table %s (a int primary key auto_increment, b varchar(10)) partition by hash(a) partitions 3",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        try {
            setSqlMode("", tddlConnection);

            sql = String.format("insert into table %s(b) values (null),(null),(\"123\")", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("alter table %s modify column b varchar(10) not null," + USE_OMC_ALGORITHM, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }
}
