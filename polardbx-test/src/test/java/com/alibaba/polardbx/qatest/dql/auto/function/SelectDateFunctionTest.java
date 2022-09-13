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

package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 日期函数
 *
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(CommonCaseRunner.class)
public class SelectDateFunctionTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectDateFunctionTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void to_daysTest() throws Exception {
        String sql = "select integer_test,varchar_test from " + baseOneTableName
            + " where TO_DAYS(date_test)-TO_DAYS('2011-05-15')>30";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select TO_DAYS(date_test) as da,varchar_test from " + baseOneTableName + " where pk=1";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void constant_to_daysTest() throws Exception {
        for (int i = 1000; i < 2018; i++) {
            String sql = String.format("select TO_DAYS('" + i + "-01-01')");
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    @FileStoreIgnore
    public void unPushed_to_daysTest() throws Exception {
        String sql = String.format("select TO_DAYS(t1.varchar_test) " +
                ", TO_DAYS(t1.integer_test)" +
                ", TO_DAYS(t1.char_test)" +
//                    ", TO_DAYS(t1.blob_test)" +
//                    ", TO_DAYS(t1.tinyint_test)" +
//                    ", TO_DAYS(t1.tinyint_1bit_test)" +
//                    ", TO_DAYS(t1.smallint_test)" +
//                    ", TO_DAYS(t1.mediumint_test)" +
//                    ", TO_DAYS(t1.bit_test)" +
//                    ", TO_DAYS(t1.bigint_test)" +
//                    ", TO_DAYS(t1.float_test)" +
//                    ", TO_DAYS(t1.double_test)" +
                ", TO_DAYS(t1.decimal_test)" +
                ", TO_DAYS(t1.date_test)" +
                ", TO_DAYS(t1.time_test)" +
                ", TO_DAYS(t1.datetime_test)" +
                ", TO_DAYS(t1.timestamp_test)" +
                ", TO_DAYS(t1.year_test)" +
                "from %s t1, %s t2 where t1.integer_test = t2.integer_test and t1.pk=0;", baseOneTableName,
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void from_daysTest() throws Exception {
        String sql = "select FROM_DAYS(TO_DAYS(date_test)) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dayofweekTest() throws Exception {
        String sql = "select DAYOFWEEK(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void weekdayTest() throws Exception {
        String sql = "select WEEKDAY(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dayofyearTest() throws Exception {
        String sql = "select DAYOFYEAR(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void monthTest() throws Exception {
        String sql = "select MONTH(date_test) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void monthnameTest() throws Exception {
        String sql = "select MONTHNAME(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void quarterTest() throws Exception {
        String sql = "select QUARTER(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void weekTest() throws Exception {
        String sql = "select WEEK(date_test) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void timeTest() throws Exception {
        String sql = "select YEAR(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select HOUR(date_test)  as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select MINUTE(date_test) as da from " + baseOneTableName + " where pk=0";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select SECOND(date_test) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void daynameTest() throws Exception {
        String sql = "select DAYNAME(date_test) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dayofmonthTest() throws Exception {
        String sql = "select DAYOFMONTH(date_test) as da from " + baseOneTableName + " where pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void TO_DAYSTest() throws Exception {
        String sql = "select TO_DAYS(date_test) as da from " + baseOneTableName + " where pk=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void timestampdiffTest() throws Exception {
        String sql =
            "select TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01') as da from " + baseOneTableName + " where pk=1";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dateTest() throws Exception {
        String sql = "select DATE(date_test)  as da from " + baseOneTableName + " where pk=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dateWithParamTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where date_test >=Date(?)";
        List<Object> param = new ArrayList<Object>();
        param.add("2011-1-1");
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nowTest() throws Exception {
        String sql = "select integer_test,now() as dd from " + baseOneTableName + " where pk=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where date_test = now()";
        //selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);

        sql = "select * from " + baseOneTableName + " where now()>date_test";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where timestamp_test = now()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        sql = "select * from " + baseOneTableName + " where now()>=timestamp_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where datetime_test = now()";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        sql = "select * from " + baseOneTableName + " where now()>=datetime_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void curDateTest() throws Exception {
        String sql = "select curdate() as date_info from dual";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void curTimeTest() throws Exception {
        String sql = "select curtime() as time_info from dual";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void microsecondTest1() throws Exception {
        String sql = "SELECT MICROSECOND('12:00:00.123456') from dual";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void microsecondTest2() throws Exception {
        String sql = "SELECT MICROSECOND('2009-12-31 23:59:59.000010') from dual";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void badTimestampDiffTest() throws Exception {
        final String sql = "SELECT TIMESTAMPDIFF(t.datetime_test,t.datetime_test) FROM " + baseOneTableName + " as t";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, "Unknown time unit");
    }
}
