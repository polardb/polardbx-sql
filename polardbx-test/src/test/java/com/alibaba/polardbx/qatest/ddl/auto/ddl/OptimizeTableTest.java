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

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.gms.metadb.limit.Limits;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.Matchers.is;


public class OptimizeTableTest extends DDLBaseNewDBTestCase {

    private String testTableName = "optimize_test";
    private final String gsiPrimaryTableName = "optimize_gsi_test";
    private final String gsiIndexTableName = "g_i_optimize_test";
    private final String gsiOptimizeHint = "/*+TDDL:cmd_extra(TRUNCATE_TABLE_WITH_GSI=true)*/";

    public OptimizeTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}});
    }

    @Before
    public void before() throws SQLException {
    }

    @Test
    public void testOptimizeTable() {
        String tableName1 = schemaPrefix + testTableName + "_1";
        String tableName2 = schemaPrefix + testTableName + "_2";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql = String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName, tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "optimize table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        dropTableIfExists(tableName1);
    }

    @Test
    public void testOptimizeTable2() {
        String tableName1 = schemaPrefix + testTableName + "_1";
        String tableName2 = schemaPrefix + testTableName + "_2";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql = String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName, tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "/*+TDDL:cmd_extra(OPTIMIZE_TABLE_PARALLELISM=1)*/optimize table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }
}
