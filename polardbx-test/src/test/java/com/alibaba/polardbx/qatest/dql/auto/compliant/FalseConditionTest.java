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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 永假条件对查询的影响
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class FalseConditionTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public FalseConditionTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.28
     */
    @Test
    public void aggregateFuncTest() throws Exception {
        String sql1 = "SELECT COUNT(*) FROM " + baseOneTableName + " WHERE FALSE";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT COUNT(*) FROM " + baseOneTableName + " WHERE 1 = 0";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "SELECT COUNT(*) FROM " + baseOneTableName + " WHERE pk = 1 AND pk = 2";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        String sql4 = "SELECT SUM(pk) FROM " + baseOneTableName + " WHERE FALSE";
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);

        String sql5 = "SELECT SUM(pk) FROM " + baseOneTableName + " WHERE 1 = 0";
        selectContentSameAssert(sql5, null, mysqlConnection, tddlConnection);

        String sql6 = "SELECT SUM(pk) FROM " + baseOneTableName + " WHERE pk = 1 AND pk = 2";
        selectContentSameAssert(sql6, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void subQueryTest() throws Exception {
        String sql1 = "SELECT COUNT(*) FROM (SELECT * FROM " + baseOneTableName + " WHERE FALSE) x";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT COUNT(*) FROM (SELECT * FROM " + baseOneTableName + " WHERE 1 = 0) x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "SELECT COUNT(*) FROM (SELECT * FROM " + baseOneTableName + " WHERE pk = 1 AND pk = 2) x";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        String sql4 = "SELECT SUM(pk) FROM (SELECT * FROM " + baseOneTableName + " WHERE FALSE) x";
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);

        String sql5 = "SELECT SUM(pk) FROM (SELECT * FROM " + baseOneTableName + " WHERE 1 = 0) x";
        selectContentSameAssert(sql5, null, mysqlConnection, tddlConnection);

        String sql6 = "SELECT SUM(pk) FROM (SELECT * FROM " + baseOneTableName + " WHERE pk = 1 AND pk = 2) x";
        selectContentSameAssert(sql6, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void subQueryNotInTest() throws Exception {
        String sql1 = "SELECT * FROM " + baseOneTableName + " WHERE pk NOT IN (SELECT pk FROM " + baseOneTableName
            + " WHERE FALSE) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT * FROM " + baseOneTableName + " WHERE pk NOT IN (SELECT pk FROM " + baseOneTableName
            + " WHERE 1 = 0) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "SELECT * FROM " + baseOneTableName + " WHERE pk NOT IN (SELECT pk FROM " + baseOneTableName
            + " WHERE pk = 1 AND pk = 2) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void subQueryNotExistsTest() throws Exception {
        String sql1 = "SELECT * FROM " + baseOneTableName + " WHERE NOT EXISTS (SELECT pk FROM " + baseOneTableName
            + " WHERE FALSE) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT * FROM " + baseOneTableName + " WHERE NOT EXISTS (SELECT pk FROM " + baseOneTableName
            + " WHERE 1 = 0) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "SELECT * FROM " + baseOneTableName + " WHERE NOT EXISTS (SELECT pk FROM " + baseOneTableName
            + " WHERE pk = 1 AND pk = 2) ORDER BY pk LIMIT 10";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
