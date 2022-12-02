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

package com.alibaba.polardbx.qatest.failpoint.recoverable.legacy;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.failpoint.base.BaseFailPointTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */

@Ignore("REMOVE DDL NOT SUPPORTED")
public class DropTableWithGsiRecoverableTest extends BaseFailPointTestCase {

    private static final String HINT = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/ ";
    private static final String createTableTemplate = "CREATE TABLE `{0}`( "
        + "    `a` bigint(11) NOT NULL, `b` bigint(11), `c` varchar(20), PRIMARY KEY (`a`), "
        + "    GLOBAL UNIQUE INDEX `{1}`(c) dbpartition by hash(`c`) "
        + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`b`) ";
    private final String createTablePrimary;
    private final String createTableIndex;
    private final String dropTablePrimary;
    private final String dropTableIndex;
    private final String createTable;

    public DropTableWithGsiRecoverableTest(String createTablePrimary, String createTableIndex, String dropTablePrimary,
                                           String dropTableIndex) {
        this.createTablePrimary = createTablePrimary;
        this.createTableIndex = createTableIndex;
        this.dropTablePrimary = dropTablePrimary;
        this.dropTableIndex = dropTableIndex;
        this.createTable = MessageFormat.format(createTableTemplate, this.createTablePrimary, this.createTableIndex);
    }

    /**
     * <pre>
     * 0. primary table name for create table
     * 1. index name for create table
     * 2. name for drop primary table
     * 3. name for drop index table
     * </pre>
     */
    @Parameters(name = "{index}:ct_p={0}, ct_i={1}, dt_p={2}, dt_i={3}")
    public static List<String[]> prepareDate() {
        final String tableName = randomTableName("sct_gsi_p1", 4);
        final String indexName = randomTableName("sct_gsi_i1", 4);

        final List<String[]> params = new ArrayList<>();
        params.add(new String[] {tableName, indexName, tableName, indexName});
        params.add(new String[] {tableName, indexName, tableName, indexName});
        params.add(new String[] {tableName, indexName, tableName, indexName});

        return params.subList(0, 1);
    }

    @Before
    public void before() {

        JdbcUtil.executeUpdateSuccess(failPointConnection, "REMOVE DDL ALL PENDING");
        dropTableIfExists(createTablePrimary);

        JdbcUtil.executeUpdateSuccess(failPointConnection, HINT + createTable);
    }

    @After
    public void after() {
        dropTableIfExists(createTablePrimary);
    }

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        executeDDL();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_FAIL_ONCE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_FAIL_ONCE, "true");
        executeDDL();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "10");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "5");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "5,3000");
        executeDDL();
    }

    public void executeDDL() {
        JdbcUtil.executeQuerySuccess(failPointConnection, "SHOW CREATE TABLE " + dropTablePrimary);
        JdbcUtil.executeQuerySuccess(failPointConnection, "SHOW CREATE TABLE " + dropTableIndex);

        JdbcUtil.executeUpdateFailed(failPointConnection,
            "DROP TABLE " + dropTableIndex,
            "Table '" + dropTableIndex + "' is global secondary index table, which is forbidden to be modified.");

        JdbcUtil.executeUpdateSuccess(failPointConnection, "DROP TABLE " + dropTablePrimary);

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(failPointConnection,
            "SHOW TABLES LIKE " + createTablePrimary)) {
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        }

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(failPointConnection,
            "SHOW TABLES LIKE " + createTableIndex)) {
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        }
    }
}
