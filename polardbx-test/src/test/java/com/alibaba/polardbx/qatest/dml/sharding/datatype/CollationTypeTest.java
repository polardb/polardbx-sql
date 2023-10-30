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

package com.alibaba.polardbx.qatest.dml.sharding.datatype;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;


public class CollationTypeTest extends CrudBasedLockTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.updateBaseOneTableForCollationTest());
    }

    public CollationTypeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, "DELETE FROM " + baseOneTableName);
        JdbcUtil.executeSuccess(mysqlConnection, "DELETE FROM " + baseOneTableName);
        for (int i = 0; i < 20; i++) {
            JdbcUtil.executeSuccess(tddlConnection,
                "INSERT INTO " + baseOneTableName + String.format(" (pk, varchar_test) VALUES (%d, '%d')", i, i));
            JdbcUtil.executeSuccess(mysqlConnection,
                "INSERT INTO " + baseOneTableName + String.format(" (pk, varchar_test) VALUES (%d, '%d')", i, i));
        }
    }

    @Test
    public void testSensitiveWithCICollation() throws Exception {
        String checkSql =
            "select * from (select 'a' c1 union all select 'B' c1 union all select 'c' c1) tmp order by c1;";
        selectOrderAssert(checkSql, null, mysqlConnection, tddlConnection);

        String trigerSql = String.format("select varchar_test from %s  where pk=1", baseOneTableName);
        selectOrderAssert(trigerSql, null, tddlConnection, tddlConnection, true);

        // check again
        selectOrderAssert(checkSql, null, mysqlConnection, tddlConnection);
    }
}
