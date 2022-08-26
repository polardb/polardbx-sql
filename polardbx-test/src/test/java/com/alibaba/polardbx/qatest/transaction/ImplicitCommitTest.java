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

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * @author Eric Fu
 */

public class ImplicitCommitTest extends CrudBasedLockTestCase {

    private boolean useBedTrans;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public ImplicitCommitTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName,
            10,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        useBedTrans = !JdbcUtil.supportXA(tddlConnection);
    }

    @Test
    public void testImplicitCommitCausedByDDL() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        if (useBedTrans) {
            //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, tddlConnection);
        }

        String sql1 = "UPDATE " + baseOneTableName + " SET integer_test = 1 WHERE 1=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql1, null);

        // Execute a DDL statement and expect the current transaction to be committed
        String sql2 = "drop table if exists foo_bar";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql2, null);

        // Expect this to be rollbacked
        String sql4 = "UPDATE " + baseOneTableName + " SET integer_test = 5 WHERE 1=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql4, null);

        tddlConnection.rollback();
        mysqlConnection.rollback();

        String sql3 = "SELECT integer_test FROM " + baseOneTableName;
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

}
