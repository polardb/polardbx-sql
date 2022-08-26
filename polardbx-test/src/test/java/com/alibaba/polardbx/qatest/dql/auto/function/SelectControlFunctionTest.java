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

package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 控制函数
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectControlFunctionTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectControlFunctionTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void ifTest() throws Exception {
        String sql = String.format("select if(pk<=integer_test,integer_test,pk) as m from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select sum(if(pk=integer_test,integer_test,pk)) as m from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
