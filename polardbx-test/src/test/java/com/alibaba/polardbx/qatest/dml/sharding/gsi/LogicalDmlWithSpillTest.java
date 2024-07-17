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

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class LogicalDmlWithSpillTest extends ReadBaseTestCase {

    private String checkSql =
        "select * from %s where id2 between 11 and 1111 order by id2";

    private String limit;

    @Parameterized.Parameters(name = "{index}:hint={0} limit={1} table={2}")
    public static List<String[]> prepareData() {

        String[][] rets = new String[][] {
            {"/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/", "", "dml_spill_test_table1"},
            {
                "/*+TDDL:CMD_EXTRA(INSERT_SELECT_LIMIT=10,UPDATE_DELETE_SELECT_LIMIT=10,SPILL_OUTPUT_MAX_BUFFER_SIZE=1048576,DML_EXECUTION_STRATEGY=LOGICAL)*/",
                "", "dml_spill_test_table2"},
            {
                "/*+TDDL:CMD_EXTRA(INSERT_SELECT_LIMIT=10,UPDATE_DELETE_SELECT_LIMIT=10,SPILL_OUTPUT_MAX_BUFFER_SIZE=1048576,ENABLE_SPILL_OUTPUT=false,DML_EXECUTION_STRATEGY=LOGICAL)*/",
                "", "dml_spill_test_table3"},
            {"/*+TDDL:ENABLE_SPILL_OUTPUT=false*/", "", "dml_spill_test_table4"},
            {"", " limit 100000 ", "dml_spill_test_table5"}
        };
        return Arrays.asList(rets);
    }

    public LogicalDmlWithSpillTest(String hint, String limit, String tableName) {
        this.hint = hint;
        this.limit = limit;
        this.baseOneTableName = tableName;
    }

    private void prepareTable(String tableName) {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id1` INT UNSIGNED NOT NULL ,\n"
            + "  `id2` INT NOT NULL,\n" + "  `strV` VARCHAR(20), PRIMARY KEY (id1),\n INDEX(strV), \n INDEX(id2)"
            + ") dbpartition BY hash(id2) tbpartition by hash(id2) tbpartitions 3;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "  `id1` INT UNSIGNED NOT NULL ,\n"
            + "  `id2` INT NOT NULL,\n" + "  `strV` VARCHAR(20), PRIMARY KEY (id1),\n" + " INDEX(strV), \n"
            + " INDEX(id2)" + ")";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    private void dropTableIfExists(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
    }

    @Before
    public void initData() throws Exception {
        this.checkSql = String.format(checkSql, baseOneTableName);
        dropTableIfExists(baseOneTableName);
        prepareTable(baseOneTableName);
        reinitialize(baseOneTableName, 20, 5000);
    }

    @After
    public void afterData() throws Exception {
        dropTableIfExists(baseOneTableName);
    }

    private void reinitialize(String tableName, int jCnt, int iCnt) {
        String sql = "insert into " + tableName + " (id1,id2,strV) values (?,?,?)";

        int id = 0;
        for (int j = 0; j < jCnt; j++) {
            List<List<Object>> params = new ArrayList<List<Object>>();
            for (int i = 0; i < iCnt; i++) {
                List<Object> param = new ArrayList<Object>();
                param.add(id++);
                param.add(id);
                param.add("test" + id);
                params.add(param);
            }
            executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        }
    }

    /**
     * Logical UPDATE/DELETE
     */
    public void commonTest(String hint, String sqlSuffix, String baseTableName) throws SQLException {
        // update
        //   with transaction
        String sql =
            hint + String
                .format("update %s a set a.strV = 'aaa' where a.id1 > 0 order by a.id2" + sqlSuffix, baseTableName);
        tddlConnection.setAutoCommit(false);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);

        //   without transaction
        sql =
            hint + String
                .format("update %s a set a.strV = 'bbb' where a.id1 > 0 order by a.id2" + sqlSuffix, baseTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);

        // relocate
        //   with transaction
        sql =
            hint + String
                .format("update %s a set a.id2 = a.id1 + 31 where a.id1 > 0 order by id2 " + sqlSuffix, baseTableName);
        tddlConnection.setAutoCommit(false);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);

        //   without transaction
        sql =
            hint + String
                .format("update %s a set a.id2 = a.id1 + 33 where a.id1 > 0 order by id2 " + sqlSuffix, baseTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);

        // delete
        //   with transaction
        sql =
            hint + String.format("delete from %s where id1 > 0 order by id2" + sqlSuffix, baseTableName);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        tddlConnection.rollback();
        mysqlConnection.rollback();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);

        //   without transaction
        sql =
            hint + String.format("delete from %s where id1 > 0 order by id2" + sqlSuffix, baseTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        final String countSql = "select count(*) from " + baseTableName;
        selectContentSameAssert(countSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCursorWithoutSpill() throws SQLException {
        try {
            commonTest(hint, limit, baseOneTableName);
        } catch (Throwable t) {
            if (t.getMessage().contains("Memory")) {
                //ignore
            } else {
                throw new RuntimeException(t);
            }
        } finally {

            try {
                if (!tddlConnection.getAutoCommit()) {
                    tddlConnection.rollback();
                    tddlConnection.setAutoCommit(true);
                }
            } catch (Throwable t) {

            }

            try {
                if (!mysqlConnection.getAutoCommit()) {
                    mysqlConnection.rollback();
                    mysqlConnection.setAutoCommit(true);
                }
            } catch (Throwable t) {

            }
        }
    }
}
