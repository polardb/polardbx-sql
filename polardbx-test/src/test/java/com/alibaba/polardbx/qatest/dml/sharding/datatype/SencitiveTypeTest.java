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
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;


public class SencitiveTypeTest extends CrudBasedLockTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.sencitiveTypeTable());
    }

    public SencitiveTypeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        String sql = "delete from " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    @Test
    public void testSensitive() throws Exception {
        // 测试通过TEXT生成geom类型
        String insertSql = "insert into " + baseOneTableName + "(id,name,school) values (1,'ljl','zxsc')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        insertSql = String
            .format("insert into %s (id,name,school) values (2, 'Ljl','zxsc')",
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        insertSql = String.format(
            "insert into %s (id,name,school) values (3,'ljL','zxsc')",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        insertSql = String.format(
            "insert into %s (id,name,school) values (3,'HjL','zxsc')",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        // 测试普通function
        String selectSql = String.format("select name from %s order by name",
            baseOneTableName);
        selectOrderByNotPrimaryKeyAssert(selectSql, null,
            mysqlConnection, tddlConnection, "name");

        // 测试普通function
        selectSql = String.format("select school from %s order by school",
            baseOneTableName);
        selectOrderByNotPrimaryKeyAssert(selectSql, null,
            mysqlConnection, tddlConnection, "school");
    }
}
