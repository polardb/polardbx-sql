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

package com.alibaba.polardbx.qatest.dql.sharding.datatype;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;


public class TimeTypeTest extends ReadBaseTestCase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public TimeTypeTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * test the cast of time, date, datetime and timestamp.
     */
    @Test
    public void testTimeType() {
        String[] sqls = new String[11];
        sqls[0] = String
            .format("select count(*) from %s where time_test <= (select timestamp_test from %s order by 1 limit 1)",
                baseOneTableName, baseOneTableName);
        sqls[1] = String
            .format("select count(*) from %s where date_test <= (select timestamp_test from %s order by 1 limit 1)",
                baseOneTableName, baseOneTableName);
        sqls[2] = String
            .format("select count(*) from %s where time_test != (select date_test from %s order by 1 limit 1)",
                baseOneTableName, baseTwoTableName);
        sqls[3] = String
            .format("select count(*) from %s where timestamp_test != (select date_test from %s order by 1 limit 1)",
                baseOneTableName, baseTwoTableName);
        sqls[4] = String
            .format("select count(*) from %s where date_test <= (select timestamp_test from %s order by 1 limit 1)",
                baseOneTableName, baseTwoTableName);
        sqls[5] = String.format(
            "select count(*) from %s where date_test <=(select date_test from %s where date_test is not null order by 1 limit 1) order by 1 limit 1",
            baseOneTableName, baseTwoTableName);
        sqls[6] = String.format(
            "select distinct date_test from %s where date_test <= (select date_test from %s where pk=100001 ) or date_test = '2012-12-13 00:00:00' order by 1",
            baseOneTableName, baseTwoTableName);
        sqls[7] = String.format(
            "select distinct date_test from %s where date_test <= (select date_test from %s where pk=100001 ) or date_test >= '2012-12-13 00:00:00' order by 1",
            baseOneTableName, baseTwoTableName);
        sqls[8] = String.format(
            "select pk,NULLIF(date_test, (select max(date_test) from %s)) val1 from %s where integer_test > (select min(integer_test) from %s) or integer_test is null order by 1",
            baseOneTableName, baseOneTableName, baseTwoTableName);
        sqls[9] = String.format(
            "select pk,NULLIF(time_test, (select max(time_test) from %s)) val1 from %s where integer_test > (select min(integer_test) from %s) or integer_test is null order by 1",
            baseOneTableName, baseOneTableName, baseTwoTableName);
        sqls[10] = String.format(
            "select pk,NULLIF(datetime_test, (select max(datetime_test) from %s)) val1 from %s where integer_test > (select min(integer_test) from %s) or integer_test is null order by 1",
            baseOneTableName, baseOneTableName, baseTwoTableName);
        for (String sql : sqls) {
            JdbcUtil.executeQuery(sql, tddlConnection);
        }
    }
}
