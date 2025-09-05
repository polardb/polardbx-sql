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

package com.alibaba.polardbx.qatest.dml.sharding.broadcast;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * Modifying gsi table with transactions.
 *
 * @author minggong
 */
@Ignore


public class TransactionWithBroadCastTest extends CrudBasedLockTestCase {

    @Parameterized.Parameters(name = "{index}:hint={0} table={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(new String[][] {
            {"", "update_delete_base_broadcast"},
            {HINT_STRESS_FLAG, "update_delete_base_broadcast"}
        });
    }

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
        truncateTable(baseOneTableName);
        if (baseTwoTableName != null) {
            truncateTable(baseTwoTableName);
        }
        if (baseThreeTableName != null) {
            truncateTable(baseThreeTableName);
        }
    }

    protected void truncateTable(String tableName) {
        String sql = null;

        sql = hint
            + "/*+TDDL:CMD_EXTRA(TRUNCATE_TABLE_WITH_GSI=TRUE,MERGE_CONCURRENT=TRUE,MERGE_DDL_CONCURRENT=TRUE)*/truncate table "
            + tableName;
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = hint + "delete from " + tableName;
        JdbcUtil.executeUpdate(mysqlConnection, sql);
    }

    public TransactionWithBroadCastTest(String hint, String baseOneTableName) throws Exception {
        this.hint = hint;
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * Forbid commit transaction after writing broadcast failed
     */
    @Test
    public void forbidCommitTest2() throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, hint + "delete from " + baseOneTableName);

        tddlConnection.setAutoCommit(false);

        try {
            String startSql = "set drds_transaction_policy='ALLOW_READ'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, startSql);

            String sql = hint + "insert " + baseOneTableName + "(pk, integer_test, bigint_test) value(?, ?, ?)";

            List<Object> params = Lists.newArrayList(0, 0, 0);
            executeErrorAssert(tddlConnection, sql, params, "ERR_ACCROSS_DB_TRANSACTION");

            // Can't executeSuccess any further sql
            sql = "commit";
            params = Lists.newArrayList();
            executeErrorAssert(tddlConnection,
                sql,
                params,
                "ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL");
        } finally {
            tddlConnection.rollback();
            tddlConnection.setAutoCommit(true);
        }
    }

    /**
     * Forbid commit transaction after writing broadcast failed
     */
    @Test
    public void forbidCommitTest3() throws Exception {
        tddlConnection.setAutoCommit(false);

        try {
            String startSql = "set drds_transaction_policy='ALLOW_READ'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, startSql);

            String sql = hint + "delete from " + baseOneTableName + " where pk > ?";

            List<Object> params = Lists.newArrayList(0);
            executeErrorAssert(tddlConnection, sql, params, "ERR_ACCROSS_DB_TRANSACTION");

            // Can't executeSuccess any further sql
            sql = "commit";
            params = Lists.newArrayList();
            executeErrorAssert(tddlConnection,
                sql,
                params,
                "ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL");
        } finally {
            tddlConnection.rollback();
            tddlConnection.setAutoCommit(true);
        }
    }
}
