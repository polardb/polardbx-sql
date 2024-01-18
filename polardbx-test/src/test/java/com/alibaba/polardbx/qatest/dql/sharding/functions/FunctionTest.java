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

package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertContentLengthSame;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentLengthSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameStringIgnoreCaseAssert;

/**
 * Created by chuanqin on 17/12/6.
 */

public class FunctionTest extends ReadBaseTestCase {

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public FunctionTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Test
    public void mathOperatorTest() {
        String sql = "select 10/(2*2)/2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void subAndDateAddWithIntervalTest() {
        String sql = "select date_add(curdate(), interval 1 day) - interval 1 second";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/"
            + "select date_add(curdate(), interval integer_test day) from " + baseOneTableName + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isNullTest() throws Exception {
        String sql = "select isnull(varchar_test) from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void countTest() throws Exception {
        String sql = "select count(varchar_test) from " + baseOneTableName + " group by pk order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leastTest() throws Exception {
        String sql = "select least('1', '2', 'a') from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void greatestTest() throws Exception {
        String sql = "select greatest('1', '2.1', 'a') from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void greatest2Test() throws Exception {
        String sql = "select greatest('1', '2.1') from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void groupConcatTest() throws Exception {
        String sql = "select group_concat(varchar_test) from " + baseOneTableName
            + " group by varchar_test order by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * test cn variable GROUP_CONCAT_MAX_LEN by set global
     */
    @Test
    @Ignore("global setting maybe affect other case.")
    public void groupConcatTestLengthBySetGlobal() throws Exception {
        // do not test single/broadcast table
        if (baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX) ||
            baseOneTableName.endsWith(ExecuteTableName.BROADCAST_TB_SUFFIX)) {
            return;
        }
        tddlConnection.createStatement().execute("set global group_concat_max_len=10");
        Thread.sleep(5000);
        String sql = "select group_concat(varchar_test) from " + baseOneTableName;
        ResultSet rs = tddlConnection.createStatement().executeQuery(sql);
        rs.next();
        String groupConcat = rs.getString(1);
        Assert.assertTrue(groupConcat.length() <= 10);
        rs.close();
        tddlConnection.createStatement().execute("set global group_concat_max_len=1024");
    }

    /**
     * test cn variable GROUP_CONCAT_MAX_LEN by hint
     */
    @Test
    public void groupConcatTestLengthByHint() throws Exception {
        // do not test single/broadcast table
        if (baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX) ||
            baseOneTableName.endsWith(ExecuteTableName.BROADCAST_TB_SUFFIX)) {
            return;
        }
        String sql = "/*TDDL:group_concat_max_len=10*/ select group_concat(varchar_test) from " + baseOneTableName;
        ResultSet rs = tddlConnection.createStatement().executeQuery(sql);
        rs.next();
        String groupConcat = rs.getString(1);
        Assert.assertTrue(groupConcat.length() <= 10);
        rs.close();
    }

    @Test
    public void groupConcatTestLengthBySession() throws Exception {
        // do not test single/broadcast table
        if (baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX) ||
            baseOneTableName.endsWith(ExecuteTableName.BROADCAST_TB_SUFFIX)) {
            return;
        }
        tddlConnection.createStatement().execute("set GROUP_CONCAT_MAX_LEN=100");
        String sql = "select group_concat(varchar_test) from " + baseOneTableName;
        ResultSet rs = tddlConnection.createStatement().executeQuery(sql);
        rs.next();
        String groupConcat = rs.getString(1);
        Assert.assertTrue(groupConcat.length() == 100);
        rs.close();
    }

    @Test
    public void bitCountTest() throws Exception {
        String sql = "select bit_count(bigint_test) from " + baseOneTableName
            + " group by bigint_test order by bigint_test limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void base64Test() throws Exception {
        String sql = "select to_base64('aa')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fromBase64Test() {
        String sql = "select from_base64('EUUF05Uo77SX1bEGje6USA==')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fromToBase64Test() {
        String sql = "select from_base64(to_base64('polarbd-x'))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void toBase64WithBinaryInputTest() {
        String sql = "select to_base64(aes_encrypt(\"123456\", unhex(MD5(\"123\"))));";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void bitAndTest() throws Exception {
        String sql = "select bit_and(pk) from " + baseOneTableName + " group by pk order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitNotTest() throws Exception {
        String sql = "select ~pk from " + baseOneTableName + " group by pk order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void intervalTest() throws Exception {
        String sql = "select interval(1, 2, 3) from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ifNullTest() throws Exception {
        String sql = "select ifnull(1, 2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // numeric
        sql = "select "
            + "ifnull(integer_test, bigint_test), "
            + "ifnull(bigint_test, decimal_test), "
            + "ifnull(decimal_test, float_test), "
            + "ifnull(decimal_test, double_test) "
            + "from "
            + baseOneTableName
            + " limit 1000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // datetime
        sql = "select "
            + "ifnull(date_test, timestamp_test), "
            + "ifnull(time_test, timestamp_test), "
            + "ifnull(datetime_test, timestamp_test), "
            + "ifnull(date_test, date_test), "
            + "ifnull(time_test, time_test), "
            + "ifnull(datetime_test, datetime_test), "
            + "ifnull(timestamp_test, timestamp_test) "
            + "from "
            + baseOneTableName
            + " limit 1000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // varchar
        sql = "select "
            + "ifnull(date_test, integer_test), "
            + "ifnull(time_test, decimal_test), "
            + "ifnull(varchar_test, timestamp_test), "
            + "ifnull(varchar_test, decimal_test) "
            + "from "
            + baseOneTableName
            + " limit 1000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // null
        sql = "select "
            + "ifnull(null, integer_test), "
            + "ifnull(null, decimal_test), "
            + "ifnull(null, timestamp_test), "
            + "ifnull(null, varchar_test) "
            + "from "
            + baseOneTableName
            + " limit 1000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void nullIfTest() throws Exception {
        String sql = "select nullif(1, 2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void caseWhenTest() throws Exception { // Calcite`s behavior is
        // inconsistent with MySql
        String sql = "select case when 47>46 then '9~' when 6 > 6 then '1' else 'A' end";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitOrTest() throws Exception {
        String sql = "select bit_or(pk) from " + baseOneTableName
            + " group by integer_test order by integer_test limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void betweenAndTest() throws Exception {
        String sql = "select pk between 1 and integer_test from " + baseOneTableName
            + " order by integer_test limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitXorTest() throws Exception {
        String sql = "select bit_xor(pk) from " + baseOneTableName
            + " group by integer_test order by integer_test limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void unixTimestampTest() throws Exception {
        String sql = "select UNIX_TIMESTAMP( '1970-01-02 00:00:00' )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void unixTimestampEmptyTest() throws Exception {
        String sql = "select UNIX_TIMESTAMP()";
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void weekOfYearTest() throws Exception {
        String sql = "select WEEKOFYEAR( '2008-02-20' ) ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void minuteTest() throws Exception {
        String sql = "select MINUTE( '2004-01-01 12:00:00.000010' )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select MINUTE( date('2020-12-12') )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void makeTimeTest() throws Exception {
        String sql = "select maketime(22,30,55)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void strToDateTest() throws Exception {
        String sql = "SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT\n"
            + "    STR_TO_DATE('a09:3:1', 'a%h:%i:%s') a,\n"
            + "    STR_TO_DATE('a09:3:1p', 'a%h:%i:%s') b,\n"
            + "    STR_TO_DATE('May 1, 2013', '%M %d, %Y') c,\n"
            + "    STR_TO_DATE('Mayy 1, 2013', '%M %d, %Y') d,\n"
            + "    STR_TO_DATE('9', '%m'),\n"
            + "    STR_TO_DATE('9', '%s') e,\n"
            + "    STR_TO_DATE('9/2001', '%i/%Y') f;\n";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    // @Test
    public void timestampTest() throws Exception {
        String sql = "select timestamp('2007-12-31 21:58:55','1:1:1')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timestampTest2() throws Exception {
        String sql = "SELECT TIMESTAMP('2003-12-31 12:00:00')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    // @Test
    public void microsecondTest() throws Exception {
        String sql = "select microsecond('2007-12-31 22:59:57')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void toSecondTest() throws Exception {
        String sql = "select to_seconds('2007-01-30 22:59:57')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timediffTest() throws Exception {
        String sql = "select timediff('2007-12-31 22:59:57', '2007-12-31 22:59:57')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void logTest() throws Exception {
        String sql = "select log(2,111)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void truncateTest() throws Exception {
        String sql = "select truncate(2.111,2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void piTest() throws Exception {
        String sql = "select pi()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void cotTest() throws Exception {
        String sql = "select cot(12)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void roundTest() throws Exception {
        doTest("SELECT ROUND(150.000,2), ROUND(150.2)");

        doTest("SELECT ROUND('150.000',1), ROUND('150.2',0)");

        doTest("select round(1.12e1, 4294967296), truncate(1.12e1, 4294967296)");

        doTest(
            "select truncate(52.64,1),truncate(52.64,2),truncate(52.64,-1),truncate(52.64,-2), truncate(-52.64,1),truncate(-52.64,-1);");

        doTest("select round(5.64,1),round(5.64,2),round(5.64,-1),round(5.64,-2);");

        doTest("select round(111,-10);");

        doTest("select round(4, cast(-2 as unsigned)), round(4, 18446744073709551614), round(4, -2);");

        doTest("select truncate(4, cast(-2 as unsigned)), truncate(4, 18446744073709551614), truncate(4, -2);");

        doTest("select round(1e0, -309), truncate(1e0, -309);");

        doTest("select round(1.5, -2147483649), round(1.5, 2147483648);");

        doTest("select truncate(1.5, -2147483649), truncate(1.5, 2147483648);");

        doTest("select round(1.5, -4294967296), round(1.5, 4294967296);");

        doTest("select truncate(1.5, -4294967296), truncate(1.5, 4294967296);");

        doTest("select round(1.5, -9223372036854775808), round(1.5, 9223372036854775808);");

        doTest("select truncate(1.5, -9223372036854775808), truncate(1.5, 9223372036854775808);");

        doTest("select round(4, -4294967200), truncate(4, -4294967200);");

        doTest("select round(999999999, -9);");

        doTest("select round(999999999.0, -9);");

        doTest(
            "select round(15.1,-1), round(15.4,-1),round(15.5,-1),round(15.6,-1),round(15.9,-1),round(-15.1,-1),round(-15.91,-1);");

        doTest(
            "select truncate(5678.123451,-1),truncate(5678.123451,-2),truncate(5678.123451,-3),truncate(5678.123451,-4);");

        doTest("SELECT ROUND(4054410556, -90121447944986105767675502273012972);");
    }

    private void doTest(String sql) {
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void roundFireworksTest() throws Exception {
        String sql = "select round(pk/3,1) from " + baseOneTableName + " order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void divideTest() throws Exception {
        String sql = "select 17 / 29";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void exportSetTest2() throws Exception {
        String sql = "SELECT EXPORT_SET(6,'1','0',',',10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insrTest() throws Exception {
        String sql = "select INSTR ('foobarbar','foobar' )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void concatWsTest() throws Exception {
        String sql = "select CONCAT_WS(',','First name','Second name','Last Name')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void hexTest() throws Exception {
        String sql = "SELECT HEX ('abc' )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void hexFromBase64Test() {
        String sql = "select hex(from_base64('EUUF05Uo77SX1bEGje6USA=='))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void hexAesEncryptTest() {
        String sql = "select hex(aes_encrypt('polardbx', 'key'))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void octTest() throws Exception {
        String sql = "SELECT oct (12.2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insertTest() throws Exception {
        String sql = "select INSERT ('Quadratic',56,2 ,'What' )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void findInSetTest() throws Exception {
        String sql = "select find_in_set ('ed' ,'a,b,c,ed,b')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fieldTest() throws Exception {
        String sql = "SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ordTest() throws Exception {
        String sql = "SELECT ord('2')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void notLikeTest() throws Exception {
        String sql = "select 'ae' NOT LIKE 'David_'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void Md5Test() throws Exception {
        String sql = "SELECT MD5('testing')";
        selectContentSameStringIgnoreCaseAssert(sql, null, mysqlConnection, tddlConnection, false);
        // test binary input
        sql = "SELECT MD5(unhex('123456ab'))";
        selectContentSameStringIgnoreCaseAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void regexTest() throws Exception {
        String sql = "SELECT 'Michael!' REGEXP '.*'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void notRegexTest() throws Exception {
        String sql = "SELECT 'Michael!' NOT REGEXP '.*'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void xorTest() throws Exception {
        String sql = "select 1 XOR 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void xor2Test() throws Exception {
        String sql = "select 1 XOR 'a2'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitwiseAndTest() throws Exception {
        String sql = "select 1 & 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void nullSafeEqTest() throws Exception {
        String sql = "select 1 <=> 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitwiseXorTest() throws Exception {
        String sql = "select 1 ^ 0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timestampdiffTest() throws Exception {
        String sql = "SELECT TIMESTAMPDIFF(DAY,'2003-02-01','2004-06-01')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timestampaddTest() throws Exception {
        String sql = "SELECT time_format( TIMESTAMPADD(MINUTE,1,'2003-01-02'), '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/"
            + "SELECT time_format( TIMESTAMPADD(MINUTE,integer_test,'2003-01-02'), '%H:%i') from " + baseOneTableName
            + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void weekTest() throws Exception {
        String sql = "SELECT weekofyear('2003-04-01')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void tosecondsTest() throws Exception {
        String sql = "SELECT TO_SECONDS('99-11-01')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void absTest() throws Exception {
        String sql = "SELECT abs(-11.1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void acosTest() throws Exception {
        String sql = "SELECT acos(1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void addDateTest() throws Exception {
        String sql = "SELECT ADDDATE('2008-01-02 00:01:01', 31)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void addTimeTest() throws Exception {
        String sql = "SELECT ADDTIME('2007-12-31 23:59:59', '1:1:1.0')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void andTest() throws Exception {
        String sql = "SELECT TRUE AND FALSE";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void asciiTest() throws Exception {
        String sql = "SELECT ASCII('2')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void asinTest() throws Exception {
        String sql = "SELECT asin(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void atanTest() throws Exception {
        String sql = "SELECT atan(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void atan2Test() throws Exception {
        String sql = "SELECT atan2(-2,2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void binTest() throws Exception {
        String sql = "SELECT bin(12)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void binaryTest() throws Exception {
        String sql = "SELECT BINARY 'a' = 'A'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void binaryTest2() throws Exception {
        String sql = "SELECT BINARY 'A' = 'A'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitlengthTest() throws Exception {
        String sql = "SELECT BIT_LENGTH('text')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitwiseorTest() throws Exception {
        String sql = "SELECT 1 | 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bitInversionTest() throws Exception {
        String sql = "SELECT 5 & ~1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void castTest() throws Exception {
        String sql = "SELECT CAST('3.12' AS SIGNED)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT cast(9999.953990000 as decimal(4,0))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void castWithCharSetTest() throws Exception {
        String sql = "SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin AS anon_1 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void castUnsignedTest() throws Exception {
        String sql = "SELECT CAST('-30.12' AS UNSIGNED)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void castDecimalTest() throws Exception {
        String sql = "SELECT CAST('-30.12' AS DECIMAL)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ceilTest() throws Exception {
        String sql = "SELECT CEIL(1.2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ceilingTest() throws Exception {
        String sql = "SELECT CEILING(1.2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore("JDBC返回结果与 MYSQL 不一致")
    public void charTest() throws Exception {
        String sql = "SELECT CHAR(77,121,83,81,'76')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void charlengthTest() throws Exception {
        String sql = "SELECT CHAR_LENGTH('ASASDASD')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void coalesceTest() throws Exception {
        String sql = "SELECT COALESCE(1,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void concatTest() throws Exception {
        String sql = "SELECT CONCAT('My', 'S', 'QL')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void convTest() throws Exception {
        String sql = "select "
            // special characters
            + "conv('1f&^&*3', 16, 2), "
            + "conv('18.3', -16, -2), "
            + "conv('18ztg.3', 36, 25), "
            + "conv('18ztg.3', 36, -25), "
            + "conv('18ztg.3', 36, 37), "
            // unsigned & signed tests
            + "conv('-1',16, 10), "
            + "conv('-1',16, -10), "
            + "conv('-1',-16, 10), "
            + "conv('-1',-16, -10), "
            // cases in mysql doc
            + "conv('a',16,2), "
            + "conv('6E', 18, 8), "
            + "conv('-17',10, -18), "
            + "conv('40',10, 10);";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void convertTest() throws Exception {
        String sql = "SELECT CONVERT('abc' USING 'utf8')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT CONVERT('ab', char(1))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT CONVERT(1.23, decimal(2,1))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void convertSignedTest() throws Exception {
        String sql = "SELECT CONVERT(1, SIGNED)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT CONVERT(1, UNSIGNED)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void cosTest() throws Exception {
        String sql = "SELECT cos(45)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void crc32Test() throws Exception {
        String sql = "SELECT CRC32('MySQL')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void curdateTest() throws Exception {
        String sql = "SELECT CURDATE()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void curtimeTest() throws Exception {
        String sql = "SELECT time_format( CURTIME(), '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void nowTest() throws Exception {
        String sql = "SELECT time_format(now(), '%H:%i')";
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dateTest() throws Exception {
        String sql = "SELECT DATE('2003-12-31 01:02:03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    // @Ignore("与 MySQL 不兼容,CoronaDB 返回时间,MySQL 返回一串数字")
    public void dateAddTest() throws Exception { // need interval support
        String sql = "SELECT DATE_ADD('2000-12-31 23:59:59', INTERVAL 1 HOUR)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dateFormatAddTest() throws Exception {
        String sql = "SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    // @Ignore("与 MySQL 不兼容,CoronaDB 返回时间,MySQL 返回一串数字")
    public void dateSubTest() throws Exception {
        String sql = "SELECT DATE_SUB('2000-12-31 23:59:59', INTERVAL 1 SECOND)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ "
            + "SELECT DATE_SUB('2000-12-31 23:59:59', INTERVAL integer_test SECOND) from " + baseOneTableName
            + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dateDiffTest() throws Exception {
        String sql = "SELECT DATEDIFF('2000-12-31 23:59:59', '2000-12-30 23:59:59')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dayTest() throws Exception {
        String sql = "SELECT DAYOFMONTH('2007-02-03');";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dayNameTest() throws Exception {
        String sql = "SELECT DAYNAME('2007-02-03');";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dayOfWeekTest() throws Exception {
        String sql = "SELECT DAYOFWEEK('2007-02-03');";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dayOfYearTest() throws Exception {
        String sql = "SELECT DAYOFYEAR('2007-02-03');";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void degreeTest() throws Exception {
        String sql = "SELECT degrees(11)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void divTest() throws Exception {
        String sql = "SELECT 5 div 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void eltTest() throws Exception {
        String sql = "SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void expTest() throws Exception {
        String sql = "SELECT EXP(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void exportSetTest() throws Exception {
        String sql = "SELECT EXPORT_SET(5,'Y','N',',',4)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void extractSetTest() throws Exception {
        String sql = "SELECT EXTRACT(YEAR FROM '2009-07-02 01:02:03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fieldSetTest() throws Exception {
        String sql = "SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void floorTest() throws Exception {
        String sql = "SELECT FLOOR(1.22)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void formatTest() throws Exception {
        String sql = "SELECT FORMAT(12332.123456, 4)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void foundRowsTest() throws Exception {
        String select = "SELECT pk FROM " + baseOneTableName + " order by pk LIMIT 10";
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        String sql = "SELECT FOUND_ROWS()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fromDaysTest() throws Exception {
        String sql = "SELECT FROM_DAYS(649600)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fromUnixTimeTest() throws Exception {
        String[] params = {
            // normal integer
            "1447430881",
            "1",
            "12",
            "43819",
            "85918535",
            "1682566145",

            // invalid integer
            "-1",
            "-1447430881",
            "-1682566145",
            "168256614589583",
            "43189929583795485454",

            // valid fractional
            "1447430881.123",
            "1447430881.999",
            "1682566145.09999",
            "1680285908.578457934598",
            "1682566145.5784579345",

            // invalid fractional
            "111447430881.123",
            "901447430881.999",
            "-1682566145.09999",
            "1680285777908.578457934598",
            "11682566145.57845898979345"
        };

        // test numeric
        String format1 = "SELECT FROM_UNIXTIME(%s)";
        // test chars
        String format2 = "SELECT FROM_UNIXTIME(\"%s\")";
        for (String param : params) {
            // test numeric
            selectContentSameAssert(String.format(format1, param), null, mysqlConnection, tddlConnection);
            // test chars
            selectContentSameAssert(String.format(format2, param), null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void getFormatTest() throws Exception {
        String sql = "SELECT GET_FORMAT(TIME,'ISO')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void hourTest() throws Exception {
        String sql = "SELECT hour('2009-07-02 01:02:03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ifTest() throws Exception {
        String sql = "SELECT IF(1>2,2,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void inTest() throws Exception {
        String sql = "SELECT 2 IN (0,3,5,7)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isNotTest() throws Exception {
        String sql = "SELECT 1 IS NOT TRUE, 0 IS NOT FALSE, null IS NOT UNKNOWN";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT null IS NOT TRUE";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select 'x' from dual where null is not true";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isTest() throws Exception {
        String sql = "SELECT 1 IS TRUE, 0 IS FALSE, NULL IS UNKNOWN";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isNotNullTest() throws Exception {
        String sql = "SELECT 1 IS NOT NULL, 0 IS NOT NULL, NULL IS NOT NULL;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void JsonArrayTest() throws Exception {
        String sql = "SELECT JSON_ARRAY(1, \"abc\", \"NULL\", TRUE, CURTIME())";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * 少一些json相关
     */
    // @Test
    public void JsonValidTest() throws Exception {
        String sql = "SELECT JSON_VALID('{\"a\": 1}')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lastDayTest() throws Exception {
        String sql = "SELECT LAST_DAY('2003-02-05')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void lastInsertIdTest() throws Exception {
        String sql = "SELECT LAST_INSERT_ID()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lCaseTest() throws Exception {
        String sql = "SELECT LCase('ss')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftTest() throws Exception {
        String sql = " SELECT LEFT('foobarbar', 5)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftShiftTest() throws Exception {
        String sql = "SELECT 1 << 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lengthTest() throws Exception {
        String sql = "SELECT LENGTH('text')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lengthUnhexTest() throws Exception {
        String sql = "SELECT length(unhex(md5(\"abrakadabra\")));";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lnTest() throws Exception { // Calcite
        String sql = "SELECT LN(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void locateTest() throws Exception {
        String sql = "SELECT LOCATE('bar', 'foobarbar')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('xbar', 'foobar')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('foobarfoobar', 'foobar')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('bar', 'foobarbar', 5)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('bar', 'foobarbar', -1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('bar', 'foobarbar', 10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT LOCATE('foobar', 'foobarbar', 5)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String format = "select locate('L', 'ALIHELLOWORLD', %s) from dual";
        for (int i = 0; i < 15; i++) {
            sql = String.format(format, i);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void log() throws Exception {
        String sql = "SELECT LOG(2,65536)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void log2() throws Exception {
        String sql = "SELECT LOG2(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void log10() throws Exception {
        String sql = "SELECT LOG10(2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lowerTest() throws Exception {
        String sql = "SELECT lower('sdsadA')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void lpadTest() throws Exception {
        String sql = "SELECT LPAD('hi',4,'??')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void ltrimTest() throws Exception {
        String sql = "SELECT LTRIM('  barbar')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void makesetTest() throws Exception {
        // first argument unsupported
        String sql = "SELECT MAKE_SET(1 & 3,'hello','nice','world')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void makedateTest() throws Exception {
        String sql = "SELECT MAKEDATE(2011,31), MAKEDATE(2011,32);";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void midTest() throws Exception {
        String sql = "SELECT mid('dsdasdas', 1, 3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void modTest() throws Exception {
        String sql = "SELECT MOD(34.5,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void monthTest() throws Exception {
        String sql = "SELECT MONTH('2008-02-03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void monthnameTest() throws Exception {
        String sql = "SELECT MONTHNAME('2008-02-03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void notTest() throws Exception {
        String sql = "SELECT NOT TRUE";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void notInTest() throws Exception {
        String sql = "SELECT 2 NOT IN (0,3,5,7)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void orTest() throws Exception {
        String sql = "SELECT true || false";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void periodAddTest() throws Exception {
        String sql = "SELECT PERIOD_ADD(200801,2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void periodDiffTest() throws Exception {
        String sql = "SELECT PERIOD_DIFF(200801,200803)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void addTest() throws Exception {
        String sql = "SELECT 1.5 + 1.3";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void powTest() throws Exception {
        String sql = "SELECT pow(2,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void powerTest() throws Exception {
        String sql = "SELECT power(2,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void quarterTest() throws Exception {
        String sql = "SELECT QUARTER('2008-04-01')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void quoteTest() throws Exception {
        String sql = "SELECT QUOTE('Do it!')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void radiansTest() throws Exception {
        String sql = "SELECT radians(90)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void randTest() throws Exception {
        String sql = "SELECT radians(90)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void repeatTest() throws Exception {
        String sql = "SELECT REPEAT('MySQL', 3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void repeatUpperBoundTest() throws SQLException {
        String sql = "SELECT length(REPEAT('1', 1024*1024*16))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT length(REPEAT('1', 1024*1024*16+1))";
        ResultSet resultSet = null;
        try {
            resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
            if (resultSet.next()) {
                String value = resultSet.getString(1);
                Assert.assertTrue(value == null);
                return;
            }
            Assert.fail();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

    }

    @Test
    public void replaceTest() throws Exception {
        String sql = "SELECT REPLACE('www.mysql.com', 'w', 'Ww')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void reverseTest() throws Exception {
        String sql = "SELECT REVERSE('abc')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightTest() throws Exception {
        String sql = "SELECT RIGHT('foobarbar', 4)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightShift() throws Exception {
        String sql = "SELECT 4 >> 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rpadTest() throws Exception {
        String sql = "SELECT RPAD('hi',4,'??')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rtrimTest() throws Exception {
        String sql = "SELECT RTRIM('  barbar')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void secToTimeTest() throws Exception {
        String sql = "SELECT SEC_TO_TIME(2378)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void secondTest() throws Exception {
        String sql = "SELECT SECOND('10:05:03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void signTest() throws Exception {
        String sql = "SELECT sign(-100)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void sinTest() throws Exception {
        String sql = "SELECT sin(100)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void sleep() throws Exception {
        String sql = "SELECT sleep(0)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void soundex() throws Exception {
        String sql = "SELECT SOUNDEX('Hello')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void soundsLike() throws Exception {
        String sql = "SELECT 'Hello' sounds like 'hello'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void spaceLike() throws Exception {
        String sql = "SELECT SPACE(6)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void soundsLikeex() throws Exception {
        String sql = "SELECT 'Hello' sounds like 'hello'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void sqrtTest() throws Exception {
        String sql = "SELECT sqrt(16)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void strcmpTest() throws Exception {
        String sql = "SELECT strcmp('asa','dsad')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void substrTest() throws Exception {
        String sql = "SELECT SUBSTRING('Quadratically',5)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void substrIndexTest() throws Exception {
        String sql = "SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void subtimeTest() throws Exception {
        String sql = "SELECT SUBTIME('2007-12-31 23:59:59','1:1:1')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void subDateTest() throws Exception {
        String sql = "SELECT SUBDATE('2000-12-31 23:59:59', 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void sysdateTest() throws Exception {
        String sql = "SELECT SYSDATE()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void tanTest() throws Exception {
        String sql = "SELECT tan(15)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timeTest() throws Exception {
        String sql = "SELECT TIME('2003-12-31 01:02:03')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timeFormatTest() throws Exception {
        String sql = "SELECT TIME_FORMAT('23:11:00', '%H %k %h %I %l')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timeToSecTest() throws Exception {
        String sql = "SELECT TIME_TO_SEC('22:23:00')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void multiplicationTest() throws Exception {
        String sql = "SELECT 2 * 1.6";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void unhexTest() throws Exception {
        String sql = "SELECT unhex('4D7953514C')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT hex(unhex('4D7953514C'))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT unhex('123')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT hex(unhex('123'))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT unhex('GG')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void upperTest() throws Exception {
        String sql = "SELECT upper('asdsad')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void utcDateTest() throws Exception {
        String sql = "SELECT utc_date( )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void utcTimeTest() throws Exception {
        String timeSql = "SELECT time_format(utc_time(), '%H:%i:%s')";
        List<List<Object>> time = JdbcUtil.getAllResult(JdbcUtil.executeQuery(timeSql, tddlConnection));
        int seconds = Integer.parseInt(time.get(0).get(0).toString().split(":")[2]);
        if (seconds > 55) {
            //当前时间的秒数大于55，等6s再执行，防止分钟数不一样
            Thread.sleep(6000);
        }
        String sql = "SELECT time_format(utc_time(), '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void utcTimestampTest() throws Exception {
        String sql = "SELECT utc_timestamp()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    // @Test
    public void uuidTest() throws Exception {
        String sql = "SELECT uuid()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void uuidShortTest() throws Exception {
        String sql = "SELECT uuid_short()";
        assertContentLengthSame(sql, null, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void weekDayTest() throws Exception {
        String sql = "SELECT WEEKDAY('2008-02-03 22:23:00')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void yearTest() throws Exception {
        String sql = "SELECT YEAR('2008-02-03 22:23:00')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void yearWeekTest() throws Exception {
        String sql = "SELECT yearweek('2008-02-03 22:23:00')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void currentTimeTest() throws Exception {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }
        String sql = "SELECT time_format( '13:21:15', '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format( '13:21:22.33', '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void currentTimestampTest() throws Exception {
        String sql = "SELECT time_format( '2021-03-09 13:21:33', '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format( '2021-03-09 13:21:33.33', '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void localtimestampTest() throws Exception {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        String sql = "SELECT time_format(LOCALTIMESTAMP, '%H:%i')";
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format(LOCALTIMESTAMP(), '%H:%i')";
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format(LOCALTIMESTAMP(2), '%H:%i')";
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    // @Ignore("waiting for desai fix")
    // fixed by desai.
    public void localtimeTest() throws Exception {
        String sql = "SELECT time_format(LOCALTIME, '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format(LOCALTIME(), '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT time_format(LOCALTIME(2), '%H:%i')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void schemaTest() {
        String sql = "SELECT schema()";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        sql = "SELECT \n database()";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void geomfromtextTest() throws Exception {
        String sql = "select GEOMFROMTEXT('POINT(1 1)') from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_GEOMFROMTEXT('POINT(1 1)') from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void convertUsingTest() {
        String sql = "select convert('abc' USING gbk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void intervalPrimaryTest() {
        String sql = "select DATE_ADD('2018-05-01',INTERVAL pk+1 DAY) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void hintCurrentDateTest() {
        String sql = "/*+TDDL:node('0')*/ select CURRENT_DATE()";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void fixForSqrtAndAbs() {
        String sql = "select sqrt(-0.1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select abs(-2147483648)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void currentUserTest() {
        String sql = "select current_user;";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void fixForHex() {
        String sql = "select HEX(CHAR(1, 0, 0))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fixForMd5() {
        String sql = "select MD5('testing')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void fixForUserLimit1() {
        String sql = "select database(), user() limit 1;";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void fixForUser() {
        String sql = "/* ApplicationName=DataGrip 2019.2 */\n" +
            "select\n" +
            " database(),\n" +
            " schema(),\n" +
            " left(user(), instr(concat(user(), '@'), '@') -1),\n" +
            " @@version_comment;";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void userNotFunction() {
        String sql = "select user from (select 1 as `user`) a;";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void userPartFunction() {
        String sql = "select user(),user from (select 1 as `user`) a;";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void castNullTest() {
        String sql = "select - + 27 as col10, + cast(null as signed) col10;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void specialNullIFTest() {
        String sql = "select - + 27 as col10, + cast(null as signed) col10;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void nullIfDecimalTest() {
        String sql = " SELECT - NULLIF ( 40, + AVG ( - - 40 ) )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void twoParameterAtanTest() {
        String sql = " select ATAN(-2, 2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void oneParameterAtan2Test() {
        String sql = " select ATAN2(-2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void notDefaultLocaleFormatTest() {
        String sql = " select FORMAT(12332.2, 2, 'de_DE')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void timeFunctionWithMicrosecond() {
        String[] sqls = {
            "select extract(DAY_MICROSECOND FROM \"1999-01-02 10:11:12.000123\")",
            "select extract(HOUR_MICROSECOND FROM \"1999-01-02 10:11:12.000123\")",
            "select extract(MINUTE_MICROSECOND FROM \"1999-01-02 10:11:12.000123\")",
            "select extract(SECOND_MICROSECOND FROM \"1999-01-02 10:11:12.000123\")",
            "select extract(MICROSECOND FROM \"1999-01-02 10:11:12.000123\")",
            "select date_add(\"1997-12-31 23:59:59.000002\",INTERVAL \"10000 99:99:99.999999\" DAY_MICROSECOND)",
            "select date_add(\"1997-12-31 23:59:59.000002\",INTERVAL \"10000:99:99.999999\" HOUR_MICROSECOND)",
            "select date_add(\"1997-12-31 23:59:59.000002\",INTERVAL \"10000:99.999999\" MINUTE_MICROSECOND)",
            "select date_add(\"1997-12-31 23:59:59.000002\",INTERVAL \"10000.999999\" SECOND_MICROSECOND)",
            "select date_add(\"1997-12-31 23:59:59.000002\",INTERVAL \"999999\" MICROSECOND)",
            "select date_sub(\"1998-01-01 00:00:00.000001\",INTERVAL \"1 1:1:1.000002\" DAY_MICROSECOND)",
            "select date_sub(\"1998-01-01 00:00:00.000001\",INTERVAL \"1:1:1.000002\" HOUR_MICROSECOND)",
            "select date_sub(\"1998-01-01 00:00:00.000001\",INTERVAL \"1:1.000002\" MINUTE_MICROSECOND)",
            "select date_sub(\"1998-01-01 00:00:00.000001\",INTERVAL \"1.000002\" SECOND_MICROSECOND)",
            "select date_sub(\"1998-01-01 00:00:00.000001\",INTERVAL \"000002\" MICROSECOND)",
            "select date_add(\"1997-12-31\",INTERVAL \"10.09\" SECOND_MICROSECOND) as a",
            "select timestampdiff(SQL_TSI_WEEK, '2001-02-01', '2001-05-01') as a",
            "select timestampdiff(SQL_TSI_HOUR, '2001-02-01', '2001-05-01') as a",
            "select timestampdiff(SQL_TSI_DAY, '2001-02-01', '2001-05-01') as a",
            "select timestampdiff(SQL_TSI_MINUTE, '2001-02-01 12:59:59', '2001-05-01 12:58:59') as a",
            "select timestampdiff(SQL_TSI_SECOND, '2001-02-01 12:59:59', '2001-05-01 12:58:58') as a",
            "select timestampdiff(SQL_TSI_DAY, '1986-02-01', '1986-03-01') as a1,\n" +
                "timestampdiff(SQL_TSI_DAY, '1900-02-01', '1900-03-01') as a2,\n" +
                "timestampdiff(SQL_TSI_DAY, '1996-02-01', '1996-03-01') as a3,\n" +
                "timestampdiff(SQL_TSI_DAY, '2000-02-01', '2000-03-01') as a4",
            "select date_add('1000-01-01 00:00:00', interval '1.03:02:01.05' day_microsecond)",
            "select date_add('1000-01-01 00:00:00', interval '1.02' day_microsecond)",
            "SELECT EXTRACT(MICROSECOND FROM CAST(20010101235959.456 AS DATETIME(6))) AS a",
            "SELECT CAST(CAST('2006-08-10 10:11:12' AS DATETIME) + INTERVAL 14 MICROSECOND AS DECIMAL(20,6))",
            "SELECT CAST(CAST('2006-08-10 10:11:12' AS DATETIME) - INTERVAL 14 MICROSECOND AS DECIMAL(20,6))",
            "SELECT CAST(INTERVAL 14 MICROSECOND + CAST('2006-08-10 10:11:12' AS DATETIME) AS DECIMAL(20,6))",
            "SELECT EXTRACT(MICROSECOND FROM CAST(123.456 AS TIME(6)))",
            "SELECT EXTRACT(MICROSECOND FROM CAST(20010101235959.456 AS DATETIME(6)))",
            "SELECT EXTRACT(MINUTE_MICROSECOND FROM '0000-00-00 00:00:00.000000')"
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void funcWithNull() {
        String[] sqls = {
            "select mid('hello',1,null),mid('hello',null,1),mid(null,1,1)",
            "select substr('hello',null,2),substr('hello',2,null),substr(null,1,2)",
            "select substring_index('the king of the the null','the',null)"
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void stringDateFunctionTest() {
        String[] sqls = {
            "select addtime(\"1997-12-31 23:59:59.999999\", \"1 1:1:1.000002\")",
            "select subtime(\"1997-12-31 23:59:59.000001\", \"1 1:1:1.000002\")",
//                "SELECT GREATEST(date '2005-05-05', '20010101', '20040404', '20030303')",
            "SELECT GREATEST('20010101', date '2005-05-05', '20040404', '20030303')",
            "SELECT CAST(CASE WHEN 0 THEN DATE'2001-01-01' END AS DATE)",
//                "SELECT GREATEST(date '2005-05-05', 20010101, 20040404, 20030303) + 0",
//                "SELECT GREATEST('95-05-05', date '10-10-10') + 0",
//                "SELECT GREATEST(date '1995-05-05', '10-10-10') + 0",
//                "SELECT GREATEST(date '1995-05-05', 19910101, 20050505, 19930303) + 0.00",
//                "SELECT GREATEST('95-05-05', date '10-10-10') + 0.00",
//                "SELECT GREATEST(date '1995-05-05', '10-10-10') + 0.00",
//                "SELECT SUBTIME('0000-01-00 00:00','00:00')",
            "SELECT ADDTIME('9999-01-01 00:00:00', '.1')",
            "SELECT GREATEST('95-05-05', date '10-10-10')",
//                "SELECT LEAST(date '1995-05-05', '10-10-10')",
            "SELECT LEAST('10-10-10', date '1995-05-05')"
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    @Ignore
    public void complexCastJsonTest() {
        String sql = "SELECT CAST(CAST(0 AS DECIMAL) AS JSON) = CAST(-0.0e0 AS DECIMAL)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void castBytesToNumberTest() {
        String sql = "select CAST(0x8fffffffffffffff as signed)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testConstantFold() {
        String sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select integer_test + 1 + 2 from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/"
            + "select datetime_test <= date_add(cast('2018-12-01' as date ), Interval -90 day ) from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/"
            + "select decimal_test between 100 - 50 and 100 + 50 from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void rowTest() {
        String[] sqls = {
            "SELECT ROW(1,2,3)=ROW(1,2,3)",
            "SELECT ROW(1,2,4)=ROW(1,2,3)",
            "SELECT ROW(null,2,3)=ROW(1,2,3)",
            "SELECT ROW(1,2,3)=ROW(null,2,3)",
            "SELECT ROW(NULL,2,3)=ROW(NULL,2,3)",
            "SELECT ROW('test',2,ROW(3,33))=ROW('test',2,ROW(3,NULL))",
            "SELECT ROW('test',2,ROW(3,33))=ROW('test',2,ROW(3,NULL))"
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void randomBytesTest() throws SQLException {
        final String randBytesPattern = "select RANDOM_BYTES(%d)";
        final int maxLen = 1024;
        for (int i = 1; i < maxLen; i *= 2) {
            String sql = String.format(randBytesPattern, i);
            try (PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
                ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs)) {

                Assert.assertTrue(rs.next());
                byte[] res = rs.getBytes(1);
                Assert.assertEquals(i, res.length);
                Assert.assertFalse(rs.next());
            }

        }
        String sql = String.format(randBytesPattern, maxLen + 1);
        executeErrorAssert(tddlConnection, sql, null,
            "length value is out of range in 'random_bytes'");
    }

    @Test
    public void SHATest() {
        String sql = "select SHA('abc')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select SHA('SHA测试')";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select SHA(123456)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        // test binary input
        sql = "select sha(?)";
        byte[] bytes = {0x12, 0x34, 0x56, (byte) 0xab};
        List<Object> params = new ArrayList<>(1);
        params.add(bytes);
        selectContentSameAssert(sql, params, mysqlConnection, tddlConnection);

        sql = "select SHA(unhex('123456ab'))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void SHA2Test() {
        final int[] hashLenArr = {0, 224, 256, 384, 512};
        final String sha2IntPattern = "select SHA2(123456,%d)";
        final String sha2StrPattern = "select SHA2('abc',%d)";
        final String sha2BinaryPattern = "select SHA2(unhex('123456ab'),%d)";

        String sql;
        for (int hashLen : hashLenArr) {
            sql = String.format(sha2IntPattern, hashLen);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = String.format(sha2StrPattern, hashLen);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            sql = String.format(sha2BinaryPattern, hashLen);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        sql = String.format(sha2IntPattern, -1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void aesEncryptionTest() throws Exception {
        try {
            boolean supportOpenSSL = ConnectionManager.getInstance().isEnableOpenSSL();
            final int[] keylens = {128, 192, 256};
            final String[] algorithms = {"ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb"};
            for (int keylen : keylens) {
                for (String algo : algorithms) {
                    String mode = String.format("aes-%d-%s", keylen, algo);
                    try {
                        new BlockEncryptionMode(mode, supportOpenSSL);
                    } catch (BlockEncryptionMode.NotSupportOpensslException e) {
                        continue;
                    }
                    testEncryptionSameResult(mode, supportOpenSSL);
                    testBinaryEncryptionSameResult(mode, supportOpenSSL);
                }
            }
        } finally {
            restoreDefaultEncryptionMode();
        }
    }

    @Test
    public void aesDecryptionTest() throws Exception {
        try {
            boolean supportOpenSSL = ConnectionManager.getInstance().isEnableOpenSSL();
            final int[] keylens = {128, 192, 256};
            final String[] algorithms = {"ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb"};
            for (int keylen : keylens) {
                for (String algo : algorithms) {
                    String mode = String.format("aes-%d-%s", keylen, algo);
                    try {
                        new BlockEncryptionMode(mode, supportOpenSSL);
                    } catch (BlockEncryptionMode.NotSupportOpensslException e) {
                        continue;
                    }
                    testDecryption(mode);
                }
            }
        } finally {
            restoreDefaultEncryptionMode();
        }
    }

    private void restoreDefaultEncryptionMode() {
        String setModeSql = "set block_encryption_mode=default";
        JdbcUtil.updateDataTddl(tddlConnection, setModeSql, null);
        JdbcUtil.updateDataTddl(mysqlConnection, setModeSql, null);
    }

    /**
     * 测试AES加密结果与MySQL相同
     */
    private void testEncryptionSameResult(String mode, boolean supportOpenSSL) throws Exception {
        setEncryptionMode(mode, tddlConnection, mysqlConnection);

        final String varName = "iv";
        setRandomBytes(varName, tddlConnection, mysqlConnection);

        final List<String> randomTextList = genRandomStringList();
        final List<String> randomKeyList = genRandomStringList();

        for (String plainText : randomTextList) {
            for (String key : randomKeyList) {
                byte[] tddlRes = getEncryptionResult(tddlConnection, plainText, key, varName);
                byte[] mysqlRes = getEncryptionResult(mysqlConnection, plainText, key, varName);
                Assert.assertArrayEquals(
                    String.format("Failed in mode %s, plaint text: %s, key: %s", mode, plainText, key),
                    tddlRes, mysqlRes);
            }
        }
    }

    private void testBinaryEncryptionSameResult(String mode, boolean supportOpenSSL) throws Exception {
        final String varName = "iv";
        setRandomBytes(varName, tddlConnection, mysqlConnection);
        // test binary input
        String plainTextHex = "123456AB";
        final List<String> randomKeyList = genRandomStringList();
        for (String key : randomKeyList) {
            byte[] tddlRes = getEncryptionResultWithHexInput(tddlConnection, plainTextHex, key, varName);
            byte[] mysqlRes = getEncryptionResultWithHexInput(mysqlConnection, plainTextHex, key, varName);
            Assert.assertArrayEquals(String.format("Failed in mode %s, "
                    + "binary plaint text in hex: %s, key: %s", mode, plainTextHex, key),
                tddlRes, mysqlRes);
        }
    }

    /**
     * 测试AES能解密出自加密的正确结果
     */
    private void testDecryption(String mode) throws Exception {
        setEncryptionMode(mode, tddlConnection, mysqlConnection);

        final String ivVarName = "iv";
        setRandomBytes(ivVarName, tddlConnection, mysqlConnection);

        final String key = "key^%#$@#";
        final List<String> randomTextList = genRandomStringList();

        for (String plainText : randomTextList) {
            String tddlDecryptRes = getDecryptionResult(tddlConnection, plainText, key, ivVarName);
            Assert.assertEquals(plainText, tddlDecryptRes);
            String mysqlDecryptRes = getDecryptionResult(mysqlConnection, plainText, key, ivVarName);
            Assert.assertEquals(plainText, mysqlDecryptRes);
        }
    }

    private byte[] getEncryptionResult(Connection conn, String plainText, String key, String varName)
        throws Exception {
        String encryptSql = String.format("select aes_encrypt('%s','%s', @%s)",
            plainText, key, varName);
        return getSingleBytesResult(conn, encryptSql, null);
    }

    private byte[] getEncryptionResultWithHexInput(Connection conn, String hex, String key, String varName)
        throws Exception {
        String encryptSql = String.format("select aes_encrypt(unhex('%s'),'%s', @%s)",
            hex, key, varName);
        return getSingleBytesResult(conn, encryptSql, null);
    }

    /**
     * 绕开目前prepare不支持设置变量的问题
     */
    private String getDecryptionResult(Connection conn, String plainText, String key, String ivVarName)
        throws SQLException {
        String decryptSql =
            String.format("select aes_decrypt(aes_encrypt('%s','%s', @%s),'%s', @%s)", plainText, key, ivVarName,
                key, ivVarName);
        return getSingleStringResult(conn, decryptSql, null);
    }

    private String getSingleStringResult(Connection conn, String sql, List<Object> params) throws SQLException {
        PreparedStatement ps = JdbcUtil.preparedStatementSet(sql, params, conn);
        ResultSet rs = JdbcUtil.executeQuery(sql, ps);
        Assert.assertTrue(rs.next());
        String res = rs.getString(1);
        Assert.assertFalse(rs.next());
        return res;
    }

    private byte[] getSingleBytesResult(Connection conn, String sql, List<Object> params) throws SQLException {
        PreparedStatement ps = JdbcUtil.preparedStatementSet(sql, params, conn);
        ResultSet rs = JdbcUtil.executeQuery(sql, ps);
        Assert.assertTrue(rs.next());
        byte[] res = rs.getBytes(1);
        Assert.assertFalse(rs.next());
        return res;
    }

    /**
     * 设置相同的随机初始变量
     */
    private static void setRandomBytes(String varName, Connection... connections) {
        Random random = new Random();
        final int IV_LEN = 16;
        StringBuilder stringBuilder = new StringBuilder(IV_LEN);
        for (int i = 0; i < IV_LEN; i++) {
            stringBuilder.append(random.nextInt(9) + 1);
        }
        String setVarSql = String.format("SET @%s=%s", varName, stringBuilder.toString());
        for (Connection conn : connections) {
            JdbcUtil.updateDataTddl(conn, setVarSql, null);
        }
    }

    /**
     * 设置相同的aes加密模式
     */
    private void setEncryptionMode(String mode, Connection... connections) {
        String setModeSql = String.format("set block_encryption_mode=\"%s\"", mode);
        for (Connection conn : connections) {
            JdbcUtil.updateDataTddl(conn, setModeSql, null);
        }
    }

    /**
     * 不同大小的字符串列表
     * 测试分块加密的正确性
     * 包括明文与密钥
     */
    private List<String> genRandomStringList() {
        List<String> list = new ArrayList<>(3);
        // 不满1个block
        list.add(RandomStringUtils.randomAlphanumeric(15));
        // 刚好满1个block
        list.add(RandomStringUtils.randomAlphanumeric(16));
        // 大于1个block
        list.add(RandomStringUtils.randomAlphanumeric(31));
        return list;
    }

}
