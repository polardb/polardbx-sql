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

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * SavePoint 测试用例
 *
 * @author changyuan.lh 2019/8/8
 * @since 5.3.12
 */

public class SavePointTest extends CrudBasedLockTestCase {

    public SavePointTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    @Before
    public void prepare() throws Exception {
        tableDataPrepare(baseOneTableName,
            10,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    private void testSavePoint(ITransactionPolicy trxPolicy, Object pk) throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        JdbcUtil.setTxPolicy(trxPolicy, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "UPDATE " + baseOneTableName + " SET integer_test = 1 WHERE pk = ?",
            Arrays.asList(pk),
            true);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);

        Savepoint tddlSavePoint = tddlConnection.setSavepoint();
        Savepoint mysqlSavePoint = mysqlConnection.setSavepoint();

        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "UPDATE " + baseOneTableName + " SET integer_test = 2 WHERE pk = ?",
            Arrays.asList(pk),
            true);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);

        tddlConnection.rollback(tddlSavePoint);
        mysqlConnection.rollback(mysqlSavePoint);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);

        tddlConnection.releaseSavepoint(tddlSavePoint);
        mysqlConnection.releaseSavepoint(mysqlSavePoint);

        tddlConnection.rollback();
        mysqlConnection.rollback();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);
    }

    private void testSavePoint2(ITransactionPolicy trxPolicy, Object pk) throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        JdbcUtil.setTxPolicy(trxPolicy, tddlConnection);

        Savepoint tddlSavePoint = tddlConnection.setSavepoint();
        Savepoint mysqlSavePoint = mysqlConnection.setSavepoint();

        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "UPDATE " + baseOneTableName + " SET integer_test = 2 WHERE pk = ?",
            Arrays.asList(pk),
            true);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);

        tddlConnection.rollback(tddlSavePoint);
        mysqlConnection.rollback(mysqlSavePoint);

        tddlConnection.releaseSavepoint(tddlSavePoint);
        mysqlConnection.releaseSavepoint(mysqlSavePoint);

        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        selectContentSameAssert("SELECT integer_test FROM " + baseOneTableName + "  WHERE pk = ?",
            Arrays.asList(pk),
            mysqlConnection,
            tddlConnection);
    }

    @Test
    public void testOnXA() throws Exception {
        if (!JdbcUtil.supportXA(tddlConnection)) {
            return;
        }
        testSavePoint(ITransactionPolicy.XA, 3);
        testSavePoint(ITransactionPolicy.TSO, 3);
        testSavePoint2(ITransactionPolicy.XA, 3);
        testSavePoint2(ITransactionPolicy.TSO, 3);
    }

    @Test
    public void testNullColumn() throws Exception {
        String tableName = "testNullColumn_tb";
        try {
            String createTable = "create table if not exists " + tableName +
                "( id int not null auto_increment primary key, name varchar(30) not null)";
            JdbcUtil.executeSuccess(tddlConnection, createTable);
            JdbcUtil.executeSuccess(tddlConnection, "begin");
            JdbcUtil.executeSuccess(tddlConnection, "savepoint sa_savepoint_1");
            JdbcUtil.executeFailed(tddlConnection, "insert into " + tableName + " (name) values (null)",
                "Column 'name' cannot be null");
            JdbcUtil.executeSuccess(tddlConnection, "rollback to savepoint sa_savepoint_1");
            JdbcUtil.executeSuccess(tddlConnection, "savepoint sa_savepoint_2");
            JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " (name) values ('test_name_0')");
            JdbcUtil.executeSuccess(tddlConnection, "rollback to savepoint sa_savepoint_2");
            JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " (name) values ('test_name_1')");
            JdbcUtil.executeSuccess(tddlConnection, "commit");

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName);
            if (rs.next()) {
                Assert.assertEquals("test_name_1", rs.getString("name"));
            } else {
                Assert.fail("no data");
            }
        } finally {
            String dropTable = "drop table if exists " + tableName;
            JdbcUtil.executeSuccess(tddlConnection, dropTable);
        }
    }
}
