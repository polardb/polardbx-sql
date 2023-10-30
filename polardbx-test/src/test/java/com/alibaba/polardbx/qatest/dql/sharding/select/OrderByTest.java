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
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;


public class OrderByTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public OrderByTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void simpleOrderBy() {
        String sql = "select * from " + baseOneTableName + " order by " + columnDataGenerator.pkValue;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void groupOrderBy() {
        String sql = "select * from " + baseOneTableName + " order by " + columnDataGenerator.pkValue;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void orderByWithNotEnoughBuffer() {
        String sql = "/*+TDDL:MERGE_SORT_BUFFER_SIZE=1*/select * from " + baseOneTableName + " order by "
            + columnDataGenerator.pkValue;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void orderByWithNotBuffer() {
        String sql = "/*+TDDL:MERGE_SORT_BUFFER_SIZE=0*/select * from " + baseOneTableName + " order by "
            + columnDataGenerator.pkValue;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
