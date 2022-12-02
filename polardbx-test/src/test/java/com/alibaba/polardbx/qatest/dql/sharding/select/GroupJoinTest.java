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
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class GroupJoinTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(selectBaseOneBaseTwo());
    }

    public static String[][] selectBaseOneBaseTwo() {
        String[][] object = {
            {
                "source_multi_db_multi_tb",
                "table_multi_db_multi_tb"},
        };
        return object;

    }

    public GroupJoinTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void simpleGroupJoinTest() {
        String sql = String.format(
            "/*+TDDL:enable_bka_join=false*/select sum(t2.database) from %s t1 left join %s t2 on t1.id=t2.table group by t1.id;",
            baseOneTableName, baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

}
