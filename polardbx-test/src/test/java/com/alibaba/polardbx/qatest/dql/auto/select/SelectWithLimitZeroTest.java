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

/**
 * Limit0优化测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithLimitZeroTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithLimitZeroTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void limit0Test() throws Exception {
        String sql = "select * from " + baseOneTableName + " limit 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void limit0WithGroupByTest() throws Exception {
        String sql = "select pk as pkcol from " + baseOneTableName + " group by pk limit 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void limitOffset0WithGroupByTest() throws Exception {
        String sql = "select pk as pkcol from " + baseOneTableName + " group by pk limit 1000, 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void limitOffset0WithGroupByTest2() throws Exception {
        String sql = "select pk as pkcol from " + baseOneTableName + " group by pk limit 0 offset 1000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }
}
