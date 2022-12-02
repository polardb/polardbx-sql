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

package com.alibaba.polardbx.qatest.dql.sharding.type.time;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

public abstract class TimeTableTestBase extends TimeTestBase {
    protected String TABLE_NAME;

    protected String table;

    protected static final String CREATE_TABLE_POLAR_X = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "`pk` bigint(11) NOT NULL auto_increment,\n"
        + "`time_0` time DEFAULT NULL,\n"
        + "`time_1` time(1) DEFAULT NULL,\n"
        + "`time_2` time(2) DEFAULT NULL,\n"
        + "`time_3` time(3) DEFAULT NULL,\n"
        + "`time_4` time(4) DEFAULT NULL,\n"
        + "`time_5` time(5) DEFAULT NULL,\n"
        + "`time_6` time(6) DEFAULT NULL,\n"
        + "`datetime_0` datetime DEFAULT null,\n"
        + "`datetime_1` datetime(1) DEFAULT null,\n"
        + "`datetime_2` datetime(2) DEFAULT null,\n"
        + "`datetime_3` datetime(3) DEFAULT null,\n"
        + "`datetime_4` datetime(4) DEFAULT null,\n"
        + "`datetime_5` datetime(5) DEFAULT null,\n"
        + "`datetime_6` datetime(6) DEFAULT null,\n"
        + "`timestamp_0` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "`timestamp_1` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1),\n"
        + "`timestamp_2` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),\n"
        + "`timestamp_3` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"
        + "`timestamp_4` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),\n"
        + "`timestamp_5` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),\n"
        + "`timestamp_6` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),\n"
        + "`date_0` date DEFAULT NULL,\n"
        + "PRIMARY KEY (`pk`)\n"
        + ") dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4";

    protected static final String CREATE_TABLE_POLAR_X_NEW_PART = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "`pk` bigint(11) NOT NULL auto_increment,\n"
        + "`time_0` time DEFAULT NULL,\n"
        + "`time_1` time(1) DEFAULT NULL,\n"
        + "`time_2` time(2) DEFAULT NULL,\n"
        + "`time_3` time(3) DEFAULT NULL,\n"
        + "`time_4` time(4) DEFAULT NULL,\n"
        + "`time_5` time(5) DEFAULT NULL,\n"
        + "`time_6` time(6) DEFAULT NULL,\n"
        + "`datetime_0` datetime DEFAULT null,\n"
        + "`datetime_1` datetime(1) DEFAULT null,\n"
        + "`datetime_2` datetime(2) DEFAULT null,\n"
        + "`datetime_3` datetime(3) DEFAULT null,\n"
        + "`datetime_4` datetime(4) DEFAULT null,\n"
        + "`datetime_5` datetime(5) DEFAULT null,\n"
        + "`datetime_6` datetime(6) DEFAULT null,\n"
        + "`timestamp_0` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "`timestamp_1` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1),\n"
        + "`timestamp_2` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),\n"
        + "`timestamp_3` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"
        + "`timestamp_4` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),\n"
        + "`timestamp_5` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),\n"
        + "`timestamp_6` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),\n"
        + "`date_0` date DEFAULT NULL,\n"
        + "PRIMARY KEY (`pk`)\n"
        + ") partition by key(`pk`) partitions 3";

    protected static final String CREATE_TABLE_MYSQL = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "`pk` bigint(11) NOT NULL auto_increment,\n"
        + "`time_0` time DEFAULT NULL,\n"
        + "`time_1` time(1) DEFAULT NULL,\n"
        + "`time_2` time(2) DEFAULT NULL,\n"
        + "`time_3` time(3) DEFAULT NULL,\n"
        + "`time_4` time(4) DEFAULT NULL,\n"
        + "`time_5` time(5) DEFAULT NULL,\n"
        + "`time_6` time(6) DEFAULT NULL,\n"
        + "`datetime_0` datetime DEFAULT null,\n"
        + "`datetime_1` datetime(1) DEFAULT null,\n"
        + "`datetime_2` datetime(2) DEFAULT null,\n"
        + "`datetime_3` datetime(3) DEFAULT null,\n"
        + "`datetime_4` datetime(4) DEFAULT null,\n"
        + "`datetime_5` datetime(5) DEFAULT null,\n"
        + "`datetime_6` datetime(6) DEFAULT null,\n"
        + "`timestamp_0` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "`timestamp_1` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1),\n"
        + "`timestamp_2` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),\n"
        + "`timestamp_3` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"
        + "`timestamp_4` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),\n"
        + "`timestamp_5` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),\n"
        + "`timestamp_6` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),\n"
        + "`date_0` date DEFAULT NULL,\n"
        + "PRIMARY KEY (`pk`)\n"
        + ")";

    protected static final String INSERT_SQL_FORMAT = "insert into %s (%s) values (?)";

    /**
     * Test Aggregation of time type
     */
    protected static final String ORDER_BY_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_SORT=false*/select concat(%s) from %s order by %s";

    /**
     * Test Join
     */
    protected static final String JOIN_SQL_FORMAT =
        "select concat(a.%s), concat(b.%s) from %s a inner join %s b where a.%s = b.%s";

    /**
     * Test compare
     */
    protected static final String COMPARE_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select a.%s, b.%s, a.%s %s b.%s from %s a, %s b;";

    protected static final String[] COMPARE_OP = {">", ">=", "<", "<=", "=", "!="};

    public TimeTableTestBase(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    @Before
    public void preparMySQLTable() {
        String mysqlSql = String.format(CREATE_TABLE_MYSQL, TABLE_NAME);
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("truncate table %s", TABLE_NAME));
    }

    @Before
    public void preparePolarDBXTable() {
        JdbcUtil.executeSuccess(tddlConnection,
            usingNewPartDb() ? String.format(CREATE_TABLE_POLAR_X_NEW_PART, TABLE_NAME) :
                String.format(CREATE_TABLE_POLAR_X, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format("truncate table %s", TABLE_NAME));
    }

    @After
    public void afterTable() {
        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
        JdbcUtil.dropTable(mysqlConnection, TABLE_NAME);
    }

    protected void insertTimeStrings(List<String> timeStringList, String col) {
        final String insertSql = String.format(INSERT_SQL_FORMAT, TABLE_NAME, col);
        List params = timeStringList.stream()
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);
    }

    protected void testOrderBy(List<String> timeStringList, String col) {
        insertTimeStrings(timeStringList, col);
        String selectSql = String.format(ORDER_BY_SQL_FORMAT, col, TABLE_NAME, col);
        selectOrderAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void testJoin(List<String> timeStringList, String col) {
        insertTimeStrings(timeStringList, col);
        String selectSql = String.format(JOIN_SQL_FORMAT, col, col, TABLE_NAME, TABLE_NAME, col, col);
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void testCompareOp(List<String> timeStringList1, String col1, List<String> timeStringList2, String col2) {
        insertTimeStrings(timeStringList1, col1);
        insertTimeStrings(timeStringList2, col2);
        for (String op : COMPARE_OP) {
            String selectSql = String.format(COMPARE_SQL_FORMAT, col1, col2, col1, op, col2, TABLE_NAME, TABLE_NAME);
            selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
        }
    }
}
