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

package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author hongxi.chx
 */
public class EnumTest extends ReadBaseTestCase {

    @Before
    public void prepare() throws Exception {
        String createTableMySQL = "CREATE TABLE if not exists `enum_test_partition` (\n"
            + "\t`testID` int(4) NOT NULL,\n"
            + "\t`enumValue` enum('a', 'b', 'c') DEFAULT NULL,\n"
            + "\tKEY `auto_shard_key_testid` USING BTREE (`testID`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = latin1";

        String createTableTddl = "CREATE TABLE if not exists `enum_test_partition` (\n"
            + "\t`testID` int(4) NOT NULL,\n"
            + "\t`enumValue` enum('a', 'b', 'c') DEFAULT NULL,\n"
            + "\tKEY `auto_shard_key_testid` USING BTREE (`testID`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = latin1";

        if (usingNewPartDb()) {
            createTableTddl += " partition by hash(`testID`)";
        } else {
            createTableTddl += " dbpartition by hash(`testID`)";
        }
        String createTable2MySQL = "CREATE TABLE if not exists `enum_test_simple` (\n"
            + "\t`testID` int(4) NOT NULL AUTO_INCREMENT,\n"
            + "\t`enumValue` enum('a', 'b', 'c') DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`testID`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 3 DEFAULT CHARSET = latin1";
        String createTable2Tddl = "CREATE TABLE if not exists `enum_test_simple` (\n"
            + "\t`testID` int(4) NOT NULL AUTO_INCREMENT,\n"
            + "\t`enumValue` enum('a', 'b', 'c') DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`testID`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 3 DEFAULT CHARSET = latin1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableTddl);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable2Tddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableMySQL);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable2MySQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from enum_test_simple");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "delete from enum_test_simple");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from enum_test_partition");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "delete from enum_test_partition");
        //test insert string and index to simple table
        String sql1 = "insert into enum_test_simple values(1,'a'),(2,2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql1);
        //test insert string and index to simple table
        String sql2 = "insert into enum_test_partition values(1,'a'),(2,2),(2,'a'),(3,'b'),(4,'c')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql2);
    }

    @After
    public void after() throws Exception {
        JdbcUtil.dropTable(tddlConnection, "enum_test_partition");
        JdbcUtil.dropTable(tddlConnection, "enum_test_simple");
        JdbcUtil.dropTable(mysqlConnection, "enum_test_partition");
        JdbcUtil.dropTable(mysqlConnection, "enum_test_simple");
    }

    @Test
    public void testSelectTest() {
        //select from simple table
        selectContentSameAssert("select * from enum_test_simple", null, mysqlConnection, tddlConnection);

    }

    @Test
    public void testSelectPartitionTest() {
        //select from partition table
        selectContentSameAssert("select * from enum_test_partition", null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectJoinTest() {
        selectContentSameAssert(
            "select * from ( select ttest.testid, ttest1.enumValue,count(*) "
                + "from enum_test_partition ttest1 "
                + "  join enum_test_simple ttest "
                + "  on ttest.testID = ttest1.testID "
                + " group by ttest1.enumValue, ttest.testID ) a "
                + " order by a.enumValue desc;", null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectFilterTest() {
        //select from simple table
        selectContentSameAssert("select * from enum_test_simple where enumValue = 'a'", null, mysqlConnection,
            tddlConnection);
        //select from partition table
        selectContentSameAssert("select * from enum_test_partition where enumValue in ('a','b')", null,
            mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectAggTest() {
        //select from simple table
        selectContentSameAssert("select enumValue from enum_test_simple group by enumValue", null, mysqlConnection,
            tddlConnection);
        //select from partition table
        selectContentSameAssert("select enumValue from enum_test_partition group by enumValue", null,
            mysqlConnection, tddlConnection);
    }

}
