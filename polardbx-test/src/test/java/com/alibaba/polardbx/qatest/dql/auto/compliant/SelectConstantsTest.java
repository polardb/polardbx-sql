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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 查询中的常量
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class SelectConstantsTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectConstantsTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.28
     */
    @Test
    public void constantsTest() throws Exception {
        String sql1 = "select * from (select 31, pk, varchar_test from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select * from (select pk, '1024', varchar_test from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select * from (select pk, varchar_test, 3.14 from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void constantsWithAliasTest() throws Exception {
        String sql1 = "select id from (select 31 id, pk, varchar_test from " + baseOneTableName + " limit 10) x";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select name from (select pk, '1024' name, varchar_test from " + baseOneTableName
            + " limit 10) x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select PI from (select pk, varchar_test, 3.14 as PI from " + baseOneTableName + " limit 10) x";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void multiConstantsTest() throws Exception {
        String sql1 = "select * from (select 31, pk, '1024' name, varchar_test from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select * from (select pk, '1024' as name, varchar_test, 3.14 PI from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select * from (select 31 as id, pk, varchar_test, 3.14 from " + baseOneTableName
            + " order by pk limit 10) x";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void duplicateConstantsTest() throws Exception {
        String sql1 = "select 99, pk, 99, varchar_test from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select pk, '1024' name, '1024' name, varchar_test, 3.14 PI from " + baseOneTableName
            + " order by pk limit 10";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select pk, 3.14 PI, varchar_test, 3.14 PI from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    @Ignore
    public void emptyStringTest() throws Exception {
        String sql1 = "select pk, '', '' from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select '', '', pk from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select '', pk, '' from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }
}
