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
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * DISTINCT 和 JOIN 语法的兼容性
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class DistinctJoinTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public DistinctJoinTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Test
    public void distinctJoinTest() {
        String sql1 = "SELECT distinct x1.pk from " + baseOneTableName + " x1, " + baseTwoTableName
            + " x2 WHERE x1.integer_test = x2.integer_test order by x1.pk";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
