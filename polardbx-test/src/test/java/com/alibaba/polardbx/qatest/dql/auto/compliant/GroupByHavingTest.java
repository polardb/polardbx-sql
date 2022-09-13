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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * group by测试
 *
 * @author chenhui
 * @since 5.1.17
 */

public class GroupByHavingTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public GroupByHavingTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.28
     */
    @Test
    public void havingFuncTest() throws Exception {
        String sql = "select varchar_test, max(pk) from " + baseOneTableName
            + " group by varchar_test having min(pk) < 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void havingFuncTest1() throws Exception {
        String sql = "select pk, max(integer_test) from " + baseOneTableName
            + " group by pk having ceiling(pk) < 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void havingWithAliasTest() throws Exception {
        String sql = "select varchar_test, min(pk) min, max(pk) max from " + baseOneTableName
            + " group by varchar_test having max(pk) - min(pk) > 900";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void havingComplexAliasTest() throws Exception {
        String sql = "select varchar_test, max(pk) - min(pk) diff from " + baseOneTableName
            + " group by varchar_test having (max(pk) - min(pk)) > 900";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void havingComplexTest() throws Exception {
        String sql = "select varchar_test, min(pk) min from " + baseOneTableName
            + " group by varchar_test having (max(pk) - min(pk)) > 900";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
