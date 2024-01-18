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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class DualQueryTest extends ReadBaseTestCase {
    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public DualQueryTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void testDualExists1() {
        String sql = String.format(
            "select '#dbe9eb', '#dbe9eb',4,1 from dual where exists (select 1 from %s where pk = 10 and integer_test > 0)",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testDualNotExists1() {
        String sql = String.format(
            "select '#dbe9eb', '#dbe9eb',4,1 from dual where not exists (select 1 from %s where pk = 10 and integer_test < 0)",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testDualExists2() {
        String sql = String.format(
            "select '#dbe9eb', '#dbe9eb',4,1 from dual where exists (select 1 from %s where integer_test > 0)",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testDualNotExists2() {
        String sql = String.format(
            "select '#dbe9eb', '#dbe9eb',4,1 from dual where not exists (select 1 from %s where integer_test < 0)",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
