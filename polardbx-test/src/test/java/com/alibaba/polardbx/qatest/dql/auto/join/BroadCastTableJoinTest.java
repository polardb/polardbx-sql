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

package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 广播表的的相关的Join测试
 *
 * @author chengbi
 * @since 5.1.22
 */

public class BroadCastTableJoinTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0},table1={1},hint={2}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBroadcastAndBaseWithHintTable());
    }

    public BroadCastTableJoinTest(String baseOneTableName, String baseTwoTableName, String hint) throws Exception {
        // 在这个测试用例中， host_info是广播表
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.hint = hint;
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void leftJoinWithBroadCastTableTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " left join "
                + baseTwoTableName
                + "  " + "on " + baseTwoTableName + ".pk=" + baseOneTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void rightJoinWithBroadCastTableTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " right join "
                + baseTwoTableName
                + "  " + "on " + baseTwoTableName + ".pk=" + baseOneTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void innerJoinWithBroadCastTableTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "on " + baseTwoTableName + ".pk=" + baseOneTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
