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
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */

public class SegmentedParallelTest extends ReadBaseTestCase {

    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SegmentedParallelTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void greaterTest() throws Exception {
        String sql = "select pk,sum(pk) from " + baseOneTableName + " group by pk order by sum(pk) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    @Ignore
    public void groupbyNumberTest() throws Exception {
        String sql = "select pk,sum(pk) from " + baseOneTableName + " group by 1 order by sum(pk) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectHaving() {
        String sql = "select pk, integer_test from " + baseOneTableName + " having integer_test>50";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void selectOrderAssert(String sql, List<Object> param, Connection mysqlConnection,
                                  Connection tddlConnection) {
        sql = "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,PARALLELISM=5,SEGMENTED=true)*/" + sql;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
