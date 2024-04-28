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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

@Ignore
public class ColumnMultiWriteTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String SOURCE_TABLE_NAME = "col_mw_test_src";
    private static final String[][] INSERT_PARAMS = new String[][] {
        new String[] {"(id,a)", "values (0,1)", "(id,a,b)", "values (0,1,1)"},
        new String[] {"(a,id)", "values (2,1)", "(id,a,b)", "values (1,2,2)"},
        new String[] {"(id,a)", "values (2,1+2)", "(id,a,b)", "values (2,3,3)"},
        new String[] {"(id)", "values (3)", "(id,a,b)", "values (3,1,1)"},
        new String[] {"(id,a)", "values (4,5),(5,6)", "(id,a,b)", "values (4,5,5),(5,6,6)"},
        new String[] {"(a,id)", "values (7,6),(8,7)", "(id,a,b)", "values (6,7,7),(7,8,8)"},
        new String[] {"(id,a)", "values (8,9),(9,1+9)", "(id,a,b)", "values (8,9,9),(9,10,10)"},
        new String[] {"(id)", "values (10),(11)", "(id,a,b)", "values (10,1,1),(11,1,1)"},
        new String[] {"(id,a)", "select 12,13", "(id,a,b)", "values (12,13,13)"},
        new String[] {"(id,a)", "select 13,14 union select 14,14+1", "(id,a,b)", "values (13,14,14),(14,15,15)"},
        new String[] {"(id)", "select 14+1", "(id,a,b)", "values (15,1,1)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (100,101,101)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (101,102,102),(102,103,103)"}
    };

    private static final String[][] UPDATE_PARAMS = new String[][] {
        new String[] {"a=1,id=10,a=id+1", "where id=0", "id=10,a=11,b=11", "where id=0"},
        new String[] {"a=1", "where id=1", "a=1,b=1", "where id=1"},
        new String[] {"a=a+1", "where id=1", "a=2,b=2", "where id=1"},
        new String[] {"a=id+2", "where id=1", "a=3,b=3", "where id=1"},
        new String[] {"id=100,a=200", "where id=1", "id=100,a=200,b=200", "where id=1"},
        new String[] {"id=a+10", "where id=2", "id=13", "where id=2"},
        new String[] {"id=1", "where a=20", "id=1,a=20,b=20", "where a=20"}
    };

    private static final String[][] REPLACE_PARAMS = new String[][] {
        new String[] {
            "(id,a)", "values (0,1),(1,2),(2,3),(100,100),(101,103)", "(id,a,b)",
            "values (0,1,1),(1,2,2),(2,3,3),(100,100,100),(101,103,103)"},
        new String[] {"(id)", "values (1)", "(id,a,b)", "values (1,1,1)"},
        new String[] {"(id,a)", "values (3,0+2)", "(id,a,b)", "values (3,2,2)"},
        new String[] {"(id,a)", "values (1,2),(2,3)", "(id,a,b)", "values (1,2,2),(2,3,3)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (100,101,101)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (101,102,102),(102,103,103)"}
    };

    private static final String[][] UPSERT_PARAMS = new String[][] {
        new String[] {
            "(id,a)", "values (1,2),(100,100),(101,103)", "(id,a,b)", "values (1,2,2),(100,100,100),(101,103,103)"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update a=id+2", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=id+2,b=id+2"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update a=values(a)", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update a=values(a),b=values(a)"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update id=id+10", "(id,a,b)",
            "values (1,5,5),(2,3,3) on duplicate key update id=id+10"},
        new String[] {
            "(id,a)", "values (200,300) on duplicate key update id=id+10", "(id,a,b)",
            "values (200,300,300) on duplicate key update id=id+10,a=a,b=a"},
        new String[] {
            "(id,a)", String.format("select * from %s where id=100 order by id ", SOURCE_TABLE_NAME)
            + "on duplicate key update id=id+10", "(id,a,b)", "values (100,101,101) on duplicate key update id=id+10"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100 order by id ", SOURCE_TABLE_NAME)
            + "on duplicate key update id=id+10", "(id,a,b)",
            "values (101,102,102),(102,103,103) on duplicate key update id=id+10"}
    };

    @Test
    public void testColumnMultiWriteInsert() throws SQLException {
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert", " dbpartition by hash(id)", true, false,
            true, false, INSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert_brd", " broadcast", true, false, false,
            false, INSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert_single", " single", true, false, false,
            false, INSERT_PARAMS);

        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert", " dbpartition by hash(id)", false,
            false, true, true, INSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert_brd", " broadcast", false, false, false,
            true, INSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_insert_single", " single", false, false, false,
            true, INSERT_PARAMS);
    }

    @Test
    public void testColumnMultiWriteReplace() throws SQLException {
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace", " dbpartition by hash(id)", false,
            true, true, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_brd", " broadcast", false, true, false,
            false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_single", " single", false, true, false,
            false, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace", " dbpartition by hash(id)", true,
            true, true, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_brd", " broadcast", true, true, false,
            false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_single", " single", true, true, false,
            false, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace", " dbpartition by hash(id)", false,
            true, true, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_brd", " broadcast", false, true, false,
            true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_single", " single", false, true, false,
            true, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace", " dbpartition by hash(id)", true,
            true, true, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_brd", " broadcast", true, true, false,
            true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("replace into", "col_mw_test_replace_single", " single", true, true, false,
            true, REPLACE_PARAMS);
    }

    @Test
    public void testColumnMultiWriteInsertIgnore() throws SQLException {
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace", " dbpartition by hash(id)",
            false, true, true, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_brd", " broadcast", false, true,
            false, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_single", " single", false, true,
            false, false, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace", " dbpartition by hash(id)",
            true, true, true, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_brd", " broadcast", true, true,
            false, false, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_single", " single", true, true,
            false, false, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace", " dbpartition by hash(id)",
            false, true, true, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_brd", " broadcast", false, true,
            false, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_single", " single", false, true,
            false, true, REPLACE_PARAMS);

        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace", " dbpartition by hash(id)",
            true, true, true, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_brd", " broadcast", true, true,
            false, true, REPLACE_PARAMS);
        testColumnMultiWriteInsertInternal("insert ignore into", "col_mw_test_replace_single", " single", true, true,
            false, true, REPLACE_PARAMS);
    }

    @Test
    public void testColumnMultiWriteUpsert() throws SQLException {
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert", " dbpartition by hash(id)", false, true,
            true, false, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_brd", " broadcast", false, true, false,
            false, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_single", " single", false, true, false,
            false, UPSERT_PARAMS);

        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert", " dbpartition by hash(id)", true, true,
            true, false, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_brd", " broadcast", true, true, false,
            false, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_single", " single", true, true, false,
            false, UPSERT_PARAMS);

        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert", " dbpartition by hash(id)", false, true,
            true, true, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_brd", " broadcast", false, true, false,
            true, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_single", " single", false, true, false,
            true, UPSERT_PARAMS);

        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert", " dbpartition by hash(id)", true, true,
            true, true, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_brd", " broadcast", true, true, false,
            true, UPSERT_PARAMS);
        testColumnMultiWriteInsertInternal("insert into", "col_mw_test_upsert_single", " single", true, true, false,
            true, UPSERT_PARAMS);
    }

    private void testColumnMultiWriteInsertInternal(String op, String tableName, String partitionDef, boolean withPk,
                                                    boolean withUk, boolean withGsi, boolean withExtraColumn,
                                                    String[][] params) throws SQLException {
        // Create source table for insert select
        dropTableIfExists(SOURCE_TABLE_NAME);
        String createSourceTableSql =
            String.format("create table if not exists %s (id int primary key, a int)", SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101),(101,102),(102,103)");

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String primaryDef = withPk ? "primary key" : "";
        String uniqueDef = withUk ? "unique key" : "";

        String createTableSql = withExtraColumn ?
            String.format(
                "create table if not exists %s (id int %s, t timestamp(6) default current_timestamp(6) on update current_timestamp(6), a int default 1 %s, b int default 0, c int)",
                tableName, primaryDef, uniqueDef)
            : String.format("create table if not exists %s (id int %s, a int default 1 %s, b int default 0)", tableName,
            primaryDef, uniqueDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String fill = String.format("insert into %s (id,a,b) values (200,201,202)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, fill, fill, null, true);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        for (int i = 0; i < params.length; i++) {
            String insert = String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
            String mysqlInsert =
                String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, false);
            selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        if (withGsi) {
            String gsiName1 = tableName + "_gsi_a";
            String gsiName2 = tableName + "_gsi_b";
            String gsiName3 = tableName + "_gsi_ab";
            String gsiName4 = tableName + "_gsi_ba";
            String createGsiSql1 =
                String.format("create global index %s on %s(a) dbpartition by hash(a)", gsiName1, tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(b) dbpartition by hash(b)", gsiName2, tableName);
            String createGsiSql3 =
                String.format("create global clustered index %s on %s(a) dbpartition by hash(a)", gsiName3, tableName);
            String createGsiSql4 =
                String.format("create global clustered index %s on %s(b) dbpartition by hash(b)", gsiName4, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql3);
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql4);

            String deleteAll = "delete from " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, false);

            for (int i = 0; i < params.length; i++) {
                String insert =
                    String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
                String mysqlInsert =
                    String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null,
                    false);
                selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, gsiName1);
//                checkGsi(tddlConnection, gsiName2);
                checkGsi(tddlConnection, gsiName3);
//                checkGsi(tddlConnection, gsiName4);
            }
        }
    }

    @Test
    public void testColumnMultiWriteUpdate() throws SQLException {
        testColumnMultiWriteUpdateInternal("col_mw_test_update", " dbpartition by hash(id)", "", true, false);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_brd", " broadcast", "", false, false);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_single", " single", "", false, false);

        testColumnMultiWriteUpdateInternal("col_mw_test_update", " dbpartition by hash(id)", "primary key", true,
            false);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_brd", " broadcast", "primary key", false, false);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_single", " single", "primary key", false, false);

        testColumnMultiWriteUpdateInternal("col_mw_test_update", " dbpartition by hash(id)", "", true, true);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_brd", " broadcast", "", false, true);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_single", " single", "", false, true);

        testColumnMultiWriteUpdateInternal("col_mw_test_update", " dbpartition by hash(id)", "primary key", true, true);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_brd", " broadcast", "primary key", false, true);
        testColumnMultiWriteUpdateInternal("col_mw_test_update_single", " single", "primary key", false, true);
    }

    private void testColumnMultiWriteUpdateInternal(String tableName, String partitionDef, String primaryDef,
                                                    boolean withGsi, boolean withExtraColumn) throws SQLException {
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql = withExtraColumn ?
            String.format(
                "create table if not exists %s (id int %s, t timestamp(6) default current_timestamp(6) on update current_timestamp(6), a int, b int, c int)",
                tableName, primaryDef)
            : String.format("create table if not exists %s (id int %s, a int, b int)", tableName, primaryDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String insert =
            String.format("insert into %s (id,a,b) values (1,2,2),(2,3,3),(3,4,4),(19,20,21),(0,0,0)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        for (int i = 0; i < UPDATE_PARAMS.length; i++) {
            String update =
                String.format("update %s set %s %s", tableName, UPDATE_PARAMS[i][0], UPDATE_PARAMS[i][1]);
            String mysqlUpdate =
                String.format("update %s set %s %s", tableName, UPDATE_PARAMS[i][2], UPDATE_PARAMS[i][3]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, hint + update, null,
                false);
            selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        if (withGsi) {
            String gsiName1 = tableName + "_gsi_a";
            String gsiName2 = tableName + "_gsi_b";
            String gsiName3 = tableName + "_gsi_ab";
            String gsiName4 = tableName + "_gsi_ba";
            String createGsiSql1 =
                String.format("create global index %s on %s(a) dbpartition by hash(a)", gsiName1, tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(b) dbpartition by hash(b)", gsiName2, tableName);
            String createGsiSql3 =
                String.format("create global clustered index %s on %s(a) dbpartition by hash(a)", gsiName3, tableName);
            String createGsiSql4 =
                String.format("create global clustered index %s on %s(b) dbpartition by hash(b)", gsiName4, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql3);
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql4);

            String deleteAll = "delete from " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, false);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);

            for (int i = 0; i < UPDATE_PARAMS.length; i++) {
                String update =
                    String.format("update %s set %s %s", tableName, UPDATE_PARAMS[i][0], UPDATE_PARAMS[i][1]);
                String mysqlUpdate =
                    String.format("update %s set %s %s", tableName, UPDATE_PARAMS[i][2], UPDATE_PARAMS[i][3]);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, hint + update, null,
                    false);
                selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, gsiName1);
//                checkGsi(tddlConnection, gsiName2);
                checkGsi(tddlConnection, gsiName3);
//                checkGsi(tddlConnection, gsiName4);
            }
        }
    }

    @Test
    public void testColumnMultiWriteInsertSelect() throws SQLException {
        String tableName = "col_mw_test_insert_select";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format(
                "create table if not exists %s (id int primary key, t timestamp(6) default current_timestamp(6) on update current_timestamp(6), a int, b int, c int)",
                tableName);
        String partitionDef = " dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        String insert = String.format("insert into %s (a,id) select 1,0 union select 0,-1", tableName);
        String mysqlInsert = String.format("insert into %s (id,a,b) values (-1,0,0),(0,1,1)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
        selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s (id,a) select 1,2 union select 2,3", tableName);
        mysqlInsert = String.format("insert into %s (id,a,b) values (1,2,2),(2,3,3)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
        selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s (id) select 3 union select 4", tableName);
        mysqlInsert = String.format("insert into %s (id) values (3),(4)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
        selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);

        String gsiName1 = "col_mw_test_insert_select_gsi_a";
        String gsiName2 = "col_mw_test_insert_select_gsi_b";
        String createGsiSql1 =
            String.format("create global clustered index %s on %s(a) dbpartition by hash(a)", gsiName1, tableName);
        String createGsiSql2 =
            String.format("create global index %s on %s(b) dbpartition by hash(b)", gsiName2, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);

        insert = String.format("insert into %s (id,a) select 5,6", tableName);
        mysqlInsert = String.format("insert into %s (id,a,b) values (5,6,6)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
        selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s (id) select 6", tableName);
        mysqlInsert = String.format("insert into %s (id) values (6)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, true);
        selectContentSameAssert("select id,a,b from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
//        checkGsi(tddlConnection, gsiName2);
    }

    @Test
    public void testColumnMultiWriteOnUpdate() throws SQLException {
        String tableName = "col_mw_test_on_update";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format(
                "create table if not exists %s (id int primary key, "
                    + "a timestamp(6) default current_timestamp(6) on update current_timestamp(6), "
                    + "b timestamp(6) default current_timestamp(6) on update current_timestamp(6))",
                tableName);
        String partitionDef = " dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        String insert = String.format("insert into %s(id) values(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + insert);

        insert = String.format("insert into %s(id, a) values(2, null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + insert);

        selectContentSameAssertWithDiffSql("select a from " + tableName, "select b from " + tableName,
            null, tddlConnection, tddlConnection, false, false, false);

        insert = String.format("update %s set id=2 where id=2", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + insert);

        selectContentSameAssertWithDiffSql("select a from " + tableName, "select b from " + tableName,
            null, tddlConnection, tddlConnection, false, false, false);
    }

    @Test
    public void testColumnMultiWriteUpdateMultipleTable() throws SQLException {
        String tableName1 = "col_mw_test_update_multi_table_1";
        String tableName2 = "col_mw_test_update_multi_table_2";
        dropTableIfExists(tableName1);
        dropTableIfExistsInMySql(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExistsInMySql(tableName2);

        String createTableSql =
            String.format("create table if not exists %s (id int primary key, a int, b int, c int)", tableName1);
        String partitionDef = " dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);
        createTableSql =
            String.format("create table if not exists %s (id int primary key, a int, b int, c int)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String insert = String.format("insert into %s(id,a,b) values(1,2,2),(2,3,3)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        insert = String.format("insert into %s(id,a,b) values(2,3,3),(3,4,4)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        String update =
            MessageFormat.format("update {0},{1} set {0}.a=1,{1}.a=2 where {0}.id={1}.id", tableName1, tableName2);
        String mysqlUpdate =
            MessageFormat.format("update {0},{1} set {0}.a=1,{0}.b=1,{1}.a=2,{1}.b=2 where {0}.id={1}.id", tableName1,
                tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlUpdate);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);

        update = MessageFormat.format("update {0},{1} set {0}.a={1}.a where {0}.id={1}.id", tableName1, tableName2);
        mysqlUpdate = MessageFormat.format("update {0},{1} set {0}.a={1}.a,{0}.b={1}.b where {0}.id={1}.id", tableName1,
            tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlUpdate);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);

        update = MessageFormat.format(
            "update {0},{1} set {0}.a=1,{0}.a={0}.a+1,{1}.a={0}.a,{0}.a={1}.a+10 where {0}.id={1}.id", tableName1,
            tableName2);
        mysqlUpdate =
            MessageFormat.format("update {0},{1} set {0}.a=12,{0}.b=12,{1}.a=2,{1}.b=2 where {0}.id={1}.id", tableName1,
                tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlUpdate);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);

        update = MessageFormat.format("update {0},{1} set {0}.c=20,{1}.a={1}.a+10 where {0}.id={1}.id", tableName1,
            tableName2);
        mysqlUpdate =
            MessageFormat.format("update {0},{1} set {0}.c=20,{1}.a=12,{1}.b=12 where {0}.id={1}.id", tableName1,
                tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlUpdate);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testColumnMultiWriteDeletePushdown() throws SQLException {
        String tableName = "col_mw_test_delete_table";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table if not exists %s (id int primary key, a int, b int, c int)", tableName);
        String partitionDef = " dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);

        String insert = String.format("insert into %s(id,a,b) values(1,2,2),(2,3,3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`a`,`b`\")*/ ";
        String delete = String.format("delete from %s where id=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + hint + delete);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertEquals(1, trace.size());
    }

    @Test
    public void testColumnMultiWriteColumnName() throws SQLException {
        String tableName = "col_mw_test_col_name_table";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table if not exists %s (id int primary key, a int, `3` int, c int)", tableName);
        String partitionDef = " dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);

        String hint = "/*+TDDL:CMD_EXTRA(COLUMN_DEBUG=\"ColumnMultiWrite:`3`,`a`\")*/ ";
        String insert = String.format("insert into %s(id,`3`) values(1,2),(2,3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + insert);

        selectContentSameAssertWithDiffSql("select a from " + tableName, "select `3` from " + tableName, null,
            tddlConnection, tddlConnection, false, false, false);

        String update = String.format("update %s set `3`=4", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + update);

        selectContentSameAssertWithDiffSql("select a from " + tableName, "select `3` from " + tableName, null,
            tddlConnection, tddlConnection, false, false, false);
    }
}
