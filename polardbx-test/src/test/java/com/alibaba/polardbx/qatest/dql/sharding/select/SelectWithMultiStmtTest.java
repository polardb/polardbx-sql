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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;

/**
 * 多语句测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

@Ignore
public class SelectWithMultiStmtTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithMultiStmtTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void multiSelectStmtTest() throws Exception {
        String sql = "select * from " + baseOneTableName;
        sql += ";";
        sql += sql;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /***
     * 多语句之间用空隔隔开，应该报错
     *
     * @throws Exception
     */
    @Test
    public void multiSelectStmtSeparatedBySpaceTest() throws Exception {
        String sql = "select 1 select 2 select 3";
        sql += " ";
        sql += sql;
        selectErrorAssert(sql, null, tddlConnection, "sql is not a supported statement");
    }

}
