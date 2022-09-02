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
 * LEFT/RIGHT JOIN 条件下推
 *
 * @author changyuan.lh
 * @since 5.1.28
 */

public class LeftRightJoinTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public LeftRightJoinTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.28
     */
    @Test
    public void leftJoinTest() throws Exception {
        String sql1 = "SELECT A.CODE FROM ( SELECT B.pk AS CODE FROM " + baseOneTableName
            + " B ) A LEFT JOIN ( SELECT C.pk AS CODE FROM " + baseOneTableName
            + " C ) D ON A.CODE = D.CODE WHERE A.CODE = 900 LIMIT 1";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT A.CODE, A.NAME FROM ( SELECT B.pk AS CODE, B.varchar_test NAME FROM " + baseOneTableName
            + " B ) A LEFT JOIN ( SELECT C.pk AS CODE, C.varchar_test NAME FROM " + baseOneTableName
            + " C ) D ON A.CODE = D.CODE WHERE A.CODE = 900 LIMIT 1";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.28
     */
    @Test
    public void rightJoinTest() throws Exception {
        String sql1 = "SELECT A.CODE FROM ( SELECT B.pk AS CODE FROM " + baseOneTableName
            + " B ) A RIGHT JOIN ( SELECT C.pk AS CODE FROM " + baseOneTableName
            + " C ) D ON A.CODE = D.CODE WHERE D.CODE = 900 LIMIT 1";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "SELECT A.CODE, A.NAME FROM ( SELECT B.pk AS CODE, B.varchar_test NAME FROM " + baseOneTableName
            + " B ) A RIGHT JOIN ( SELECT C.pk AS CODE, C.varchar_test NAME FROM " + baseOneTableName
            + " C ) D ON A.CODE = D.CODE WHERE D.CODE = 900 LIMIT 1";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
    }
}
