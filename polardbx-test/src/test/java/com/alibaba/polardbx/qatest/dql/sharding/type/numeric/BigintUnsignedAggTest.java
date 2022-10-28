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

package com.alibaba.polardbx.qatest.dql.sharding.type.numeric;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class BigintUnsignedAggTest extends CrudBasedLockTestCase {
    protected static final String TEST_TABLE = "t_bigint_unsigned";
    protected static final String SQL_CREATE_TEST_TABLE = "create table if not exists %s (\n"
        + "  pk bigint auto_increment,\n"
        + "  col bigint unsigned,\n"
        + "  primary key (pk)) %s";

    protected static final String SUFFIX = " dbpartition by hash(pk) tbpartition by hash(pk);";
    protected static final String SUFFIX_IN_NEW_PART = "partition by key(pk) partitions 8";

    protected static final String INSERTION_FORMAT =
        "insert into %s (col) values (9223372036854775807), (9223372036854775808), (18446744073709551615), (0), (2147483647), (18446744071562067968), (18446744073709551615), (0), (8388607), (18446744073701163008), (16777215), (0), (32767), (18446744073709518848), (65535), (0), (127), (18446744073709551488), (255), (0), (4294967295);";

    @Before
    public void prepareMySQLTable() {
        String mysqlSql = String.format(SQL_CREATE_TEST_TABLE, TEST_TABLE, "");
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("delete from %s where 1=1", TEST_TABLE));

        JdbcUtil.executeSuccess(mysqlConnection, String.format(INSERTION_FORMAT, TEST_TABLE));
    }

    @Before
    public void preparePolarDBXTable() {
        String tddlSql = String.format(
            SQL_CREATE_TEST_TABLE,
            TEST_TABLE,
            usingNewPartDb() ? SUFFIX_IN_NEW_PART : SUFFIX);
        JdbcUtil.executeSuccess(tddlConnection, tddlSql);
        JdbcUtil.executeSuccess(tddlConnection, String.format("delete from %s where 1=1", TEST_TABLE));

        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERTION_FORMAT, TEST_TABLE));
    }

    @After
    public void afterTable() {
        JdbcUtil.dropTable(tddlConnection, TEST_TABLE);
        JdbcUtil.dropTable(mysqlConnection, TEST_TABLE);
    }

    @Test
    public void test() {
        String[] sqlList = {
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/ select sum(col) from t_bigint_unsigned ;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/ select avg(col) from t_bigint_unsigned ;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/ select count(col) from t_bigint_unsigned ;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/ select min(col) from t_bigint_unsigned ;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/ select max(col) from t_bigint_unsigned ;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

    }
}
