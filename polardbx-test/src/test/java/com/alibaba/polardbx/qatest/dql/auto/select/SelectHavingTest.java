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

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * SELECT with HAVING clause
 *
 * @author eric.fy
 */

public class SelectHavingTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectHavingTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void testSelectHaving() {
        String sql = "select pk, integer_test from " + baseOneTableName + " having integer_test>50";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectHavingWithAlias() {
        String sql = "select t.pk, t.integer_test from " + baseOneTableName + " t having t.integer_test>50";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectHavingOrderBy() {
        String sql =
            "select t.pk, t.integer_test from " + baseOneTableName + " t having t.integer_test>50 order by 1,2";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectGroupByHaving() {
        if (usingNewPartDb()) {
            return;
        }
        String sql = "select min(pk) as min_pk, integer_test from " + baseOneTableName
            + " group by integer_test having min_pk>50";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
