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

package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.google.common.truth.Truth.assertWithMessage;

public class LocalIndexTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String OMC_ALTER_TABLE_WITH_GSI = "OMC_ALTER_TABLE_WITH_GSI=TRUE";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private static final String[] MODIFY_PARAMS = new String[] {
        "alter table %s modify column b bigint",
        "alter table %s modify column c bigint first",
        "alter table %s modify column d bigint after e",
    };

    private static final String[] CHANGE_PARAMS = new String[] {
        "alter table %s change column b bb bigint",
        "alter table %s change column c cc bigint first",
        "alter table %s change column d dd bigint after e",
        "alter table %s change column e `3` bigint after dd",
        "alter table %s change column `3` `\"f\"` int first",
        "alter table %s change column `dd` `UNIQUE` int after `\"f\"`",
    };

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";

    @Test
    public void testModifyLocalIndex() {
        String tableName = "omc_modify_index_test_tbl";
        testLocalIndexInternal(tableName, MODIFY_PARAMS);
    }

    @Test
    public void testChangeLocalIndex() {
        String tableName = "omc_change_index_test_tbl";
        testLocalIndexInternal(tableName, CHANGE_PARAMS);
    }

    private void testLocalIndexInternal(String tableName, String[] params) {
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int, c int, d int, e int)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String createIndexSql = String.format("create index l1 on %s(b)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createIndexSql, createIndexSql, null, false);
        createIndexSql = String.format("create index l2 on %s(c,d)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createIndexSql, createIndexSql, null, false);
        createIndexSql = String.format("create unique index l3 on %s(e,d)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createIndexSql, createIndexSql, null, false);

        assertSameIndexInfo(tableName);

        for (int i = 0; i < params.length; i++) {
            String alterSql = String.format(params[i], tableName);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql + USE_OMC_ALGORITHM, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            assertSameIndexInfo(tableName);
        }
    }

    private void assertSameIndexInfo(String tableName) {
        String sql = "show index from " + tableName;
        ResultSet mysqlRs = JdbcUtil.executeQuerySuccess(mysqlConnection, sql);
        ResultSet tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs, false);
        List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs, false);

        // Remove table name from results
        for (List<Object> list : mysqlResults) {
            list.remove(0);
        }

        if (isRDS80) {
            for (List<Object> list : mysqlResults) {
                list.remove(13);
                list.remove(12);
            }
        }

        for (List<Object> list : tddlResults) {
            list.remove(0);
        }
        assertWithMessage("Index not match")
            .that(tddlResults)
            .containsExactlyElementsIn(mysqlResults);
    }

    @Test
    public void testModifyGsiLocalIndex() {
        String tableName = "omc_modify_index_gsi_test_tbl";
        testGsiLocalIndexInternal(tableName, MODIFY_PARAMS);
    }

    @Test
    public void testChangeGsiLocalIndex() {
        String tableName = "omc_change_index_gsi_test_tbl";
        testGsiLocalIndexInternal(tableName, CHANGE_PARAMS);
    }

    private void testGsiLocalIndexInternal(String tableName, String[] params) {
        String refTableName = tableName + "_ref";
        String gsiTableName = tableName + "_idx";
        String refGsiTableName = refTableName + "_idx";
        dropTableIfExists(tableName);
        dropTableIfExists(refTableName);

        String createTableSqlTemplate =
            "create table %s (a int primary key, b int, c int, d int, e int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, refTableName));

        String createGsiSqlTemplate =
            "create clustered index %s on %s(a) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createGsiSqlTemplate, gsiTableName, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(createGsiSqlTemplate, refGsiTableName, refTableName));

        String createIndexSqlTemplate = "create index l1 on %s(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, gsiTableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, refGsiTableName));
        createIndexSqlTemplate = "create index l2 on %s(c,d)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, gsiTableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, refGsiTableName));
        createIndexSqlTemplate = "create index l3 on %s(e,d)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, gsiTableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, refGsiTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);

        for (int i = 0; i < params.length; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI) + String.format(params[i], refTableName));

            execDdlWithRetry(tddlDatabase1, tableName,
                buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI) + String.format(params[i], tableName) + USE_OMC_ALGORITHM,
                tddlConnection);
            assertSameIndexInfoClusteredIndex(tableName, refTableName);
            assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);
        }
    }

    private void assertSameIndexInfoClusteredIndex(String tableName, String refTableName) {
        ResultSet tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, "show index from " + tableName);
        ResultSet tddlRefRs = JdbcUtil.executeQuerySuccess(tddlConnection, "show index from " + refTableName);

        List<List<Object>> mysqlResults = JdbcUtil.getAllResult(tddlRs, false);
        List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRefRs, false);

        // Remove table name from results
        for (List<Object> list : mysqlResults) {
            list.remove(0);
        }
        for (List<Object> list : tddlResults) {
            list.remove(0);
            if ("GLOBAL".equalsIgnoreCase((String) list.get(9))) {
                list.set(1, ((String) list.get(1)).replace("_ref", ""));
            }
        }
        assertWithMessage("Index not match")
            .that(tddlResults)
            .containsExactlyElementsIn(mysqlResults);
    }

    @Test
    public void testUniqueKeyDef1() {
        String tableName = "omc_index_col_unique_test1";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int unique)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String createIndexSqlTemplate = "create index c on %s(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createIndexSqlTemplate, tableName));

        createIndexSqlTemplate = "create index c_2 on %s(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createIndexSqlTemplate, tableName));

        assertSameIndexInfo(tableName);

        String alterSqlTemplate = "alter table %s change column b c bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(alterSqlTemplate, tableName));

        assertSameIndexInfo(tableName);
    }

    @Test
    public void testUniqueKeyDef2() {
        String tableName = "omc_index_col_unique_test2";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int unique)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String createIndexSqlTemplate = "create index c on %s(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createIndexSqlTemplate, tableName));

        assertSameIndexInfo(tableName);

        String alterSqlTemplate =
            buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + "alter table %s modify column b bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(alterSqlTemplate, tableName));

        assertSameIndexInfo(tableName);

        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(alterSqlTemplate, tableName));

        assertSameIndexInfo(tableName);
    }

    @Test
    public void testUniqueKeyDef3() {
        String tableName = "omc_index_col_unique_test3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        assertSameIndexInfo(tableName);

        String alterSqlTemplate = "alter table %s change column b c bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(alterSqlTemplate, tableName));

        assertSameIndexInfo(tableName);
    }

    @Test
    public void testUniqueKeyDef4() {
        String tableName = "omc_index_col_unique_test4";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String createIndexSqlTemplate = "create index c on %s(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createIndexSqlTemplate, tableName));

        assertSameIndexInfo(tableName);

        String alterSqlTemplate =
            buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + "alter table %s modify column b bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(alterSqlTemplate, tableName));

        assertSameIndexInfo(tableName);
    }

    @Test
    public void testUniqueKeyDefGsi1() {
        String tableName = "omc_index_col_unique_test_gsi1";
        String refTableName = tableName + "_ref";
        String gsiTableName = tableName + "_idx";
        String refGsiTableName = refTableName + "_idx";
        dropTableIfExists(tableName);
        dropTableIfExists(refTableName);

        String createTableSqlTemplate =
            "create table %s (a int primary key, b int unique) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, refTableName));

        String createGsiSqlTemplate =
            "create clustered index %s on %s(a) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createGsiSqlTemplate, gsiTableName, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(createGsiSqlTemplate, refGsiTableName, refTableName));

        String createIndexSqlTemplate = "create index c on %s(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, gsiTableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, refGsiTableName));

        createIndexSqlTemplate = "create index c_2 on %s(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, gsiTableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createIndexSqlTemplate, refGsiTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);

        String alterSqlTemplate =
            buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI) + "alter table %s change column b c bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(alterSqlTemplate, refTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);
    }

    @Test
    public void testUniqueKeyDefGsi2() {
        String tableName = "omc_index_col_unique_test_gsi2";
        String refTableName = tableName + "_ref";
        String gsiTableName = tableName + "_idx";
        String refGsiTableName = refTableName + "_idx";
        dropTableIfExists(tableName);
        dropTableIfExists(refTableName);

        String createTableSqlTemplate =
            "create table %s (a int primary key, b int unique) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSqlTemplate, refTableName));

        String createGsiSqlTemplate =
            "create clustered index %s on %s(a) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createGsiSqlTemplate, gsiTableName, tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(createGsiSqlTemplate, refGsiTableName, refTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);

        String alterSqlTemplate = buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI, OMC_FORCE_TYPE_CONVERSION)
            + "alter table %s modify column b bigint";
        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(alterSqlTemplate, refTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);

        execDdlWithRetry(tddlDatabase1, tableName, String.format(alterSqlTemplate, tableName) + USE_OMC_ALGORITHM,
            tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(alterSqlTemplate, refTableName));

        assertSameIndexInfoClusteredIndex(tableName, refTableName);
        assertSameIndexInfoClusteredIndex(gsiTableName, refGsiTableName);
    }

    @Test
    public void testUniqueKeyRollback() throws SQLException {
        String tableName = "omc_index_unique_rollback_2";
        dropTableIfExists(tableName);
        String createSql =
            String.format("create table %s (a int primary key, b varchar(20) unique key, c int) dbpartition by hash(c)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String insertSql = String.format("insert into %s values (1,'fdas',3),(2,'cvx',3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        Connection conn = getPolardbxConnection();
        String sqlMode = JdbcUtil.getSqlMode(conn);

        try {
            setSqlMode("", conn);
            String hint = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION);
            String alterSql = hint + String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
            JdbcUtil.executeUpdateFailed(conn, alterSql, "");

            // check if there is any paused job left
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "SHOW DDL ALL");
            while (rs.next()) {
                String objectName = rs.getString("OBJECT_NAME");
                String state = rs.getString("STATE");
                Assert.assertFalse(
                    objectName.equalsIgnoreCase(tableName) && !state.equalsIgnoreCase("ROLLBACK_COMPLETED"));
            }

            alterSql = hint + String.format("alter table %s change column b d bigint, algorithm=omc", tableName);
            JdbcUtil.executeUpdateFailed(conn, alterSql, "");

            rs = JdbcUtil.executeQuerySuccess(conn, "SHOW DDL ALL");
            while (rs.next()) {
                String objectName = rs.getString("OBJECT_NAME");
                String state = rs.getString("STATE");
                Assert.assertFalse(
                    objectName.equalsIgnoreCase(tableName) && !state.equalsIgnoreCase("ROLLBACK_COMPLETED"));
            }
        } finally {
            setSqlMode(sqlMode, conn);
            conn.close();
        }
    }
}
