/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.auto.basecrud.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.List;

/**
 * @author lijiu.lzw
 */
@Ignore("dn `nextval`, `currval` is keyword")
public class SequenceFunctionTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "nextval_currval_test";
    private static final String CREAT_TABLE = "CREATE TABLE `" + TABLE_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NULL,"
        + " `nextval` varchar(40) NOT NULL DEFAULT \"123\","
        + " `currval` varchar(40) NOT NULL DEFAULT \"abc\","
        + " PRIMARY KEY (`pk`)"
        + ") {0} ";

    private static final String PARTITIONS_METHOD = "partition by hash(pk) partitions 3";

    private static final String SEQUENCE_NAME = "nextval_currval_test_seq";

    private static final String SEQUENCE_CREATE = "create new sequence " + SEQUENCE_NAME + " START WITH 1000";

    @Before
    public void initData() throws Exception {

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(CREAT_TABLE, ""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, PARTITIONS_METHOD));

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP SEQUENCE " + SEQUENCE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, SEQUENCE_CREATE);
    }

    @After
    public void dropTable() throws Exception {
//        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP SEQUENCE " + SEQUENCE_NAME);
    }

    @Test
    public void sqlTest() throws Exception {
        String selectSql = "select `pk`, `c1`, `nextval`, `currval` from " + TABLE_NAME;

        //insert
        String sql = "insert into " + TABLE_NAME + "(pk, c1, nextval, currval) values(1,10, 'abc', 'aa')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "insert into " + TABLE_NAME + "(pk, c1, `nextval`, `currval`) values(2, 20, 'vgb', 'agr')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql =
            String.format("insert into %s (pk, c1, %s.`nextval`, %s.`currval`) values(3, 60, 'bgb', 'grd')", TABLE_NAME,
                TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("3",
            JdbcUtil.executeQueryAndGetFirstStringResult(selectSql + " where pk = 3 ", tddlConnection));

        sql =
            String.format("insert into %s (pk, c1, %s.`nextval`, %s.`currval`) values(4, 60, 'bgb', 'grd')", TABLE_NAME,
                TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("4",
            JdbcUtil.executeQueryAndGetFirstStringResult(selectSql + " where pk = 4 ", tddlConnection));

        sql = String.format("insert into %s (pk, c1, %s.nextval, %s.currval) values(5, 60, 'bgb', 'grd')", TABLE_NAME,
            TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("5",
            JdbcUtil.executeQueryAndGetFirstStringResult(selectSql + " where pk = 5 ", tddlConnection));

        //insert on duplicate key
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + TABLE_NAME, null, false);
        sql = "insert into " + TABLE_NAME + "(pk, c1, `nextval`, `currval`) values(2, 20, 'vgb', 'agr')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "insert into " + TABLE_NAME + "(pk, c1, `nextval`, `currval`) values(2, 30, 'vv', 'gg') " +
            "ON DUPLICATE KEY UPDATE nextval = values(nextval), currval = values(nextval) ";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "insert into " + TABLE_NAME + "(pk, c1, `nextval`, `currval`) values(2, 70, 'cs', 'hg') " +
            "ON DUPLICATE KEY UPDATE `nextval` = values(`nextval`), `currval` = values(`nextval`) ";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = String.format("insert into %s (pk, c1, `nextval`, `currval`) values(2, 80, 'vf', 'ng') " +
                "ON DUPLICATE KEY UPDATE %s.`nextval` = values(%s.`nextval`), %s.`currval` = values(%s.`currval`) ",
            TABLE_NAME, TABLE_NAME, TABLE_NAME, TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("vf",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 3));
        Assert.assertEquals("ng",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 4));

        sql = String.format("insert into %s (pk, c1, `nextval`, `currval`) values(2, 80, 'bg', 'gk') " +
                "ON DUPLICATE KEY UPDATE %s.nextval = values(%s.`nextval`), %s.currval = values(%s.`currval`) ", TABLE_NAME,
            TABLE_NAME, TABLE_NAME, TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("bg",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 3));
        Assert.assertEquals("gk",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 4));

        //update
        sql = String.format("update %s set %s.`nextval` = 'tyh', %s.`currval` = 'vfs'  where pk = 2 ", TABLE_NAME,
            TABLE_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tyh",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 3));
        Assert.assertEquals("vfs",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 4));

        sql =
            String.format("update %s set %s.nextval = 'vfd', %s.currval = 'vvb'  where pk = 2 ", TABLE_NAME, TABLE_NAME,
                TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("vfd",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 3));
        Assert.assertEquals("vvb",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 2 ", tddlConnection, 4));

        //relocate
        sql =
            String.format("update %s set pk = 5, %s.nextval = 'vfd', %s.currval = 'vvb'  where pk = 2 ", TABLE_NAME,
                TABLE_NAME,
                TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("vfd",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 5 ", tddlConnection, 3));
        Assert.assertEquals("vvb",
            JdbcUtil.executeQueryAndGetStringResult(selectSql + " where pk = 5 ", tddlConnection, 4));

        //select
        Assert.assertEquals("vfd",
            JdbcUtil.executeQueryAndGetStringResult(String.format(" select * from %s where pk = 5 ", TABLE_NAME),
                tddlConnection, 3));
        Assert.assertEquals("vvb",
            JdbcUtil.executeQueryAndGetStringResult(String.format(" select * from %s where pk = 5 ", TABLE_NAME),
                tddlConnection, 4));
        Assert.assertEquals("vfd", JdbcUtil.executeQueryAndGetStringResult(
            String.format(" select %s.`nextval` from %s where pk = 5 ", TABLE_NAME, TABLE_NAME), tddlConnection, 1));
        Assert.assertEquals("vvb", JdbcUtil.executeQueryAndGetStringResult(
            String.format(" select %s.`currval` from %s where pk = 5 ", TABLE_NAME, TABLE_NAME), tddlConnection, 1));
    }

    @Test
    public void sqlSequenceTest() throws Exception {
        //nextval
        Assert.assertEquals("1000",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.nextval", SEQUENCE_NAME),
                tddlConnection, 1));

        Assert.assertEquals("1000",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.currval", SEQUENCE_NAME),
                tddlConnection, 1));

        //insert
        String sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ insert into %s (pk, c1, nextval, currval) values(1,10, %s.nextval, %s.currval)",
            TABLE_NAME, SEQUENCE_NAME, SEQUENCE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("1001",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 3));
        Assert.assertEquals("1001",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 4));

        //insert on duplicate key
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ insert into %s (pk, c1, `nextval`, `currval`) values(1, 20, 'cs', 'hg') "
                +
                " ON DUPLICATE KEY UPDATE `nextval` = %s.nextval, `currval` = %s.currval ", TABLE_NAME, SEQUENCE_NAME,
            SEQUENCE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("1002",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 3));
        Assert.assertEquals("1002",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 4));

        //update
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ update %s set `nextval` = %s.nextval, `currval` = %s.currval  where pk = 1 ",
            TABLE_NAME,
            SEQUENCE_NAME, SEQUENCE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("1003",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 3));
        Assert.assertEquals("1003",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 1 ",
                    TABLE_NAME),
                tddlConnection, 4));

        //relocate
        sql = String.format(
            "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ update %s set pk = 2, `nextval` = %s.nextval, `currval` = %s.currval  where pk = 1 ",
            TABLE_NAME, SEQUENCE_NAME, SEQUENCE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("1004",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 2 ",
                    TABLE_NAME),
                tddlConnection, 3));
        Assert.assertEquals("1004",
            JdbcUtil.executeQueryAndGetStringResult(
                String.format(
                    "/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select `pk`, `c1`, `nextval`, `currval` from %s where pk = 2 ",
                    TABLE_NAME),
                tddlConnection, 4));

        //select
        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.nextval from %s where pk = 2 ",
            SEQUENCE_NAME, TABLE_NAME);
        Assert.assertEquals("1005", JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 1));

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.currval from %s where pk = 2 ",
            SEQUENCE_NAME, TABLE_NAME);
        Assert.assertEquals("1005", JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 1));

        sql =
            String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.`currval`, %s.currval from %s where pk = 2 ",
                TABLE_NAME, SEQUENCE_NAME,
                TABLE_NAME);
        Assert.assertEquals("1004", JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 1));
        Assert.assertEquals("1005", JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2));

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.nextval where count = 4 ", SEQUENCE_NAME);
        List<String> result = JdbcUtil.executeQueryAndGetColumnResult(sql, tddlConnection, 1);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals("1006", result.get(0));
        Assert.assertEquals("1007", result.get(1));
        Assert.assertEquals("1008", result.get(2));
        Assert.assertEquals("1009", result.get(3));

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.nextval from dual where count = 4 ",
            SEQUENCE_NAME);
        result = JdbcUtil.executeQueryAndGetColumnResult(sql, tddlConnection, 1);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals("1010", result.get(0));
        Assert.assertEquals("1011", result.get(1));
        Assert.assertEquals("1012", result.get(2));
        Assert.assertEquals("1013", result.get(3));

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select %s.currval from dual", SEQUENCE_NAME);
        Assert.assertEquals("1013", JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 1));
    }

    @Test
    public void sequenceFailedTest() throws Exception {
        String sql = "select xxx.nextval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"ERR_VALIDATE", "sequence", "not found"});

        sql = "select `xxx`.`xxx`.nextval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"ERR_VALIDATE"});

        sql = "select `xxx`.currval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"ERR_VALIDATE", "sequence", "not found"});

        sql = "select `xxx`.`xxx`.currval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"ERR_VALIDATE"});

        sql = "select `xxx`.prevval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"not support"});

        sql = "select " + SEQUENCE_NAME + ".prevval";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, new String[] {"not support"});
    }

}
