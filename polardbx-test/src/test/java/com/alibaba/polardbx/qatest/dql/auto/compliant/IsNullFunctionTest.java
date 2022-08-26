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
 * ISNULL(x) 和 (x) IS NULL 语法的支持
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class IsNullFunctionTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public IsNullFunctionTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void functionIsNullTest() {
        String sql1 = "SELECT pk, ISNULL(varchar_test), (varchar_test IS NULL) FROM " + baseOneTableName
            + " ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
    }
}
