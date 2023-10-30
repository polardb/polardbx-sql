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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class DataXSpecialTest extends CrudBasedLockTestCase {

    private static final String SELECT_FROM = "SELECT pk, varchar_test, integer_test, char_test, blob_test, " +
        "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, " +
        "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";

    public DataXSpecialTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    @Before
    public void initData() throws Exception {

        final String sql = "DELETE FROM " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @After
    public void clean() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("set transaction policy 3"); // ALLOW_READ
        }
    }

    @Test
    public void testDataXBatchInsert() throws Exception {
        final String sql1 = buildBatchInsertSql(0, 16);
        final String sql2 = buildBatchInsertSql(8, 24);

        // Simulating DataX batch insert procedure:
        // 1. set transaction policy 4;
        // 2. (some unrelated select statements)
        // 3. SET autocommit = 0
        // 4. select @@session.tx_read_only
        // 5. insert ... values ... (insert the user data)
        // 6. COMMIT
        // Then loop from 3 until everything done

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("set transaction policy 4"); // NO_TRANSACTION
            stmt.executeQuery("select * from " + baseOneTableName + " where 1=0").close();
        }
        tddlConnection.setAutoCommit(false);
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select @@session.tx_read_only").close();
            stmt.executeUpdate(sql1);
        }
        tddlConnection.commit();

        tddlConnection.setAutoCommit(false);
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select @@session.tx_read_only").close();
            stmt.executeUpdate(sql2);
        }
        tddlConnection.commit();

        tddlConnection.setAutoCommit(true);

        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
        }

        selectContentSameAssert(SELECT_FROM + baseOneTableName, null, mysqlConnection, tddlConnection);
    }

    private String buildBatchInsertSql(int first, int last) {
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = first; i < last; i++) {
            joiner.add("(" + i + ", " + i + ")");
        }
        return "insert ignore into " + baseOneTableName + "(pk, integer_test) values " + joiner.toString();
    }

}
