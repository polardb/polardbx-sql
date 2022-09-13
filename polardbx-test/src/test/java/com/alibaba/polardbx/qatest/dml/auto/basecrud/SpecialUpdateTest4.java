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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class SpecialUpdateTest4 extends AutoCrudBasedLockTestCase {

    private final String HINT =
        "/*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,COMPLEX_DML_WITH_TRX=FALSE)*/";

    public SpecialUpdateTest4() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
        this.baseTwoTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
    }

    private void clearData(String table) {
        String sql = "delete from  " + table;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, Lists.newArrayList());
    }

    /**
     * Concurrent update
     */
    @Test
    public void updateMultiTableConcurrently() throws Exception {
        clearData(baseOneTableName);
        clearData(baseTwoTableName);

        String insertSql = "insert into " + baseOneTableName + " (pk,integer_test) values (?,?)";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            params.add(Lists.newArrayList(i, i));
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);

        insertSql = "insert into " + baseTwoTableName + " (pk,integer_test) values (?,?)";
        params.clear();
        for (int i = 0; i < 100; i++) {
            params.add(Lists.newArrayList(i, i));
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);

        String sql = String.format(HINT
                + "update %s a, %s b set a.integer_test=b.integer_test where a.integer_test=b.integer_test and a.integer_test=?",
            baseOneTableName,
            baseTwoTableName);

        final List<AssertionError> errors = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {

                public void run() {
                    Connection connection = null;
                    try {
                        connection = getPolardbxConnection();
                        for (int j = 0; j < 100; j++) {
                            List<Object> param = Lists.newArrayList((int) (Math.random() * 100));
                            JdbcUtil.updateData(connection, sql, param);
                        }
                    } catch (AssertionError ae) {
                        errors.add(ae);
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }

        String checkSql = String.format("select pk, integer_test from %s", baseOneTableName);
        selectContentSameAssert(checkSql, null, mysqlConnection, tddlConnection);
    }
}
