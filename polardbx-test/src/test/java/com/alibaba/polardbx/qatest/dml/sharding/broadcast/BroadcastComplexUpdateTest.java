/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.sharding.broadcast;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.BROADCAST_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.TWO;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

public class BroadcastComplexUpdateTest extends CrudBasedLockTestCase {

    public BroadcastComplexUpdateTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:hint={0},table1={1}, table2={2}")
    public static List<String[]> prepareData() {
        final List<String[]> tableNames = Arrays.asList(
            new String[][] {
                //两个广播表update
                {
                    ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX,
                    ExecuteTableName.UPDATE_DELETE_BASE + TWO + BROADCAST_TB_SUFFIX
                }
            }
        );
        return tableNames;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    @Test
    public void updateSameValueOneTableTest() {
        String sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.integer_test=21 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);

        sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.pk=5 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
    }

    @Test
    public void updateSameValueTwoTableTest() {
        String sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.integer_test=21,b.integer_test=21 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
        assertBroadcastTableSame(baseTwoTableName, tddlConnection, mysqlConnection);

        sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.pk=5,b.pk=5,a.integer_test=21,b.integer_test=21 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
        assertBroadcastTableSame(baseTwoTableName, tddlConnection, mysqlConnection);
    }

    @Test
    public void updateTwoTableWithOnUpdateTimeTest() {
        String sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.integer_test=21,a.timestamp_test='2023-05-26 20:12:12',b.integer_test=21 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
        assertBroadcastTableSame(baseTwoTableName, tddlConnection, mysqlConnection);

        sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.pk=5,b.pk=5,a.timestamp_test='2023-05-26 20:12:12',a.integer_test=21,b.integer_test=21 WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
        assertBroadcastTableSame(baseTwoTableName, tddlConnection, mysqlConnection);

        sql = "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.pk=5,b.pk=5,a.timestamp_test=CURRENT_TIMESTAMP(),a.integer_test=21,b.integer_test=21,b.timestamp_test=CURRENT_TIMESTAMP() WHERE a.pk=5 and a.pk = b.pk";
        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, null, true);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);
        assertBroadcastTableSelfSame(baseTwoTableName, tddlConnection);
    }
}
