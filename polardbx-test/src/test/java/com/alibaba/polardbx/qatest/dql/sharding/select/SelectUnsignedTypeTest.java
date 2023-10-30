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

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Created by chuanqin on 19/5/14.
 */

public class SelectUnsignedTypeTest extends CrudBasedLockTestCase {

    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.ALL_TYPE));
    }

    public SelectUnsignedTypeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        String sql = "insert ignore into  "
            + baseOneTableName
            + " (pk,int_unsigned_test,bigint_unsigned_test,tinyint_unsigned_test,smallint_unsigned_test,mediumint_unsigned_test,double_unsigned_test) values (1,1,1,1,1,1,1),(2,2,2,2,2,2,2)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

    }

    @Test
    public void selfCorrelatedSubQueryIntUnsigned() {
        String sql = "select   pk, int_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.int_unsigned_test < host.int_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQueryMedIntUnsigned() {
        String sql = "select   pk, mediumint_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.mediumint_unsigned_test < host.mediumint_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQueryBigIntUnsigned() {
        String sql = "select   pk, bigint_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.bigint_unsigned_test < host.bigint_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQueryTinyIntUnsigned() {
        String sql = "select   pk, tinyint_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.tinyint_unsigned_test < host.tinyint_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQuerySmallIntUnsigned() {
        String sql = "select   pk, smallint_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.smallint_unsigned_test < host.smallint_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQueryDoubleUnsigned() {
        String sql = "select   pk, double_unsigned_test    from "
            + baseOneTableName
            + " as host where pk > 0 and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.double_unsigned_test < host.double_unsigned_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
