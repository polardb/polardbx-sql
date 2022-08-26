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

package com.alibaba.polardbx.qatest.dql.auto.compliant;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * DISTINCT 语法的兼容性
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class DistinctQueryTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public DistinctQueryTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void distinctTest() {
        String sql1 = "SELECT integer_test, count(distinct integer_test) from " + baseOneTableName
            + " group by integer_test order by integer_test";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
    }

    @Ignore
    public void distinctTest2() {
        String sql2 = "SELECT integer_test x, count(distinct integer_test) from " + baseOneTableName
            + " group by x order by x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distinctInSubTest() {
        String sql1 = "SELECT * from (SELECT pk a, count(distinct pk) b from " + baseOneTableName
            + " group by a) x order by a, b";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
    }

    @Ignore
    public void distinctInSubTest2() {
        String sql2 = "SELECT * from (SELECT integer_test a, count(distinct integer_test) b from " + baseOneTableName
            + " group by a) x order by a";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distinctInSubTest3() {
        String sql3 = "SELECT * from (SELECT integer_test, count(distinct integer_test) from " + baseOneTableName
            + " group by integer_test) x order by integer_test";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distinctConstantAsPk() {
        String sql = "SELECT DISTINCT 16 as pk from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distinctConstTest() {
        String sql3 = "SELECT distinct 79 from " + baseOneTableName
            + " group by pk";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

}
