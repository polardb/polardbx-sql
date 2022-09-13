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
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Insert broadcast table with functions.
 *
 * @author changyuan.lh
 */

public class BroadcastWriteWithFuncTest extends CrudBasedLockTestCase {

    public BroadcastWriteWithFuncTest(String broadcastTableName) {
        this.baseOneTableName = broadcastTableName;
    }

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        String[][] object = {{ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.BROADCAST_TB_SUFFIX},};
        return Arrays.asList(object);
    }

    @Before
    public void initData() throws Exception {
        final String sql = "DELETE FROM " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @Test
    public void testFunc() {
        String sql = "INSERT INTO " + baseOneTableName + " (pk, date_test) VALUES ("
            + "1, DATE(DATE_ADD(NOW(), INTERVAL 1 DAY)))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "INSERT INTO " + baseOneTableName + " (pk, datetime_test) VALUES ("
            + "2, DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "INSERT INTO " + baseOneTableName + " (pk, integer_test) VALUES ("
            + "3, HOUR(TIMEDIFF(NOW(), DATE_SUB(NOW(), INTERVAL 5 DAY))))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
