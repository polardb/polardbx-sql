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

package com.alibaba.polardbx.qatest.dql.sharding.type;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class DateTimeFunctionTest extends ReadBaseTestCase {

    private String funcExpression;

    @Parameters(name = "{index}:func={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(new String[] {"cast(MAKETIME(12,15,30) as time)"},
            new String[] {"cast(TIME('-23:59:59.100') as time(3))"},
            new String[] {"cast(TIME('272:59:59.300') as time(3))"},
            new String[] {"TIMESTAMP('2019-10-01 10:00:01')"},
            new String[] {"TIMESTAMP('2019-10-10 23:59:59.300')"},
            new String[] {"HOUR(TIME('272:59:59.300'))"},
            new String[] {"UNIX_TIMESTAMP(TIME('272:59:59'))"},
            new String[] {"DATE(CURRENT_TIME())"},
            new String[] {"HOUR(TIMESTAMP(CURRENT_TIME()))"},
            new String[] {"cast(TIMEDIFF('20:00:02.300', '23:59:59.100') as time(3))"},
            new String[] {"cast(TIMEDIFF('272:59:59.300', '100:00:01.100') as time(3))"},
            new String[] {"cast(TIMEDIFF('272:59:59.300', '-10:00:01.100') as time(3))"},
            new String[] {"cast(TIMEDIFF('-23:59:59.100', '10:00:01.300') as time(3))"},
            new String[] {"cast(TIMEDIFF('2019-10-10 23:59:59.100', '2019-10-01 10:00:01.300') as time(3))"},
            new String[] {"cast(TIMEDIFF('2019-10-01 23:59:59.100', '2019-10-10 10:00:01.300') as time(3))"},
            new String[] {"cast(TIMEDIFF(CAST(CURDATE() AS TIME), '23:59:59.100') as time(3))"},
            new String[] {"cast(TIMEDIFF(CAST(CURDATE() AS TIME), '-10:00:01.300') as time(3))"},
            new String[] {"HOUR(TIMEDIFF('272:59:59.100', '10:00:01.300'))"},
            new String[] {"DAYOFYEAR(TIMEDIFF('2019-10-10 23:59:59.300', '2019-10-01 00:00:00.100'))"},
            new String[] {"CURDATE()"},
            new String[] {"UNIX_TIMESTAMP(CURDATE())"},
            new String[] {"DATE_ADD(CURDATE(), INTERVAL 1 DAY)"},
            new String[] {"DATEDIFF('2019-10-10 23:59:59.300', DATE_ADD('2019-10-01', INTERVAL 1 DAY))"},
            new String[] {"UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))"},
            new String[] {"cast(timediff('-923:59:59','-10:00:01') as time)"},
            new String[] {"cast(timediff('-923:59:59','10:00:01.') as time)"},
            new String[] {"cast(timediff('-839:00:00','-890:00:01') as time)"},
            new String[] {"cast(timediff('-840:00:00','-838:59:59') as time)"},
            new String[] {"cast(timediff('840:00:00','-890:00') as time)"},
            new String[] {"cast(timediff('2019-10-10 23:59:59.100', '10:00:01.300') as time)"},
            new String[] {"cast(' - 2443:59:59.100' as time)"},
            new String[] {"cast(' - 12:59:59' as time)"},
            new String[] {"cast(' 2443:59:59.100' as time)"},
            new String[] {"cast(' 3:59:59' as time)"},
            new String[] {"str_to_date('-20190220','%Y%m%d' )"},
            new String[] {"str_to_date('20190220','%Y%m%d' )"},
            new String[] {"STR_TO_DATE('05 1,2013','%M %d,%Y')"},
            new String[] {"STR_TO_DATE('10.31.2003',GET_FORMAT(DATE,'EUR'))"},
            new String[] {"STR_TO_DATE('May 1, 2013','%M %d,%Y')"},
            new String[] {"STR_TO_DATE('May 1noise, 2013','%M %dnoise,%Y')"},
            new String[] {"str_to_date('20190232','%Y%m%d' )"},
            new String[] {"str_to_date('2019-10-01 10:23:01', '%Y-%m-%d %H:%i:%s')"},
            new String[] {"str_to_date('10:23:01', '%H:%i:%s')"},
            new String[] {"str_to_date(space(2), '1')"},
            new String[] {"str_to_date('20200801120000', '%Y%m%d%H%i%s')"},
            new String[] {"ADDTIME('01:00:00', '02:00:00')"},
            new String[] {"ADDTIME('01:00:00', '838:00:00')"},
            new String[] {"ADDTIME('890:00:00', '10:05:00')"},
            new String[] {"ADDTIME('00:20:00', '838:00:00')"},
            new String[] {"ADDTIME('00:20:00', '839:00:00')"},
            new String[] {"ADDTIME('-989:20:00.999', '839:00:00.998')"},
            new String[] {"ADDTIME('-989:20:00.999999', '40:00:00')"},
            new String[] {"ADDTIME('2010-10-20 01:00:00', '839:00:00');"},
            new String[] {"ADDTIME('2010-10-20 01:00:00', '9:00:00');"},
            new String[] {"ADDTIME('23:59:59', '2 1:1:1');"},
            new String[] {"ADDTIME('23:59:59', '200 1:1:1');"},
            new String[] {"SUBTIME('2010-10-20 01:00:00', '-9:00:00');"},
            new String[] {"SUBTIME('01:00:00', '02:00:00')"},
            new String[] {"SUBTIME('01:00:00', '838:00:00')"},
            new String[] {"SUBTIME('890:00:00', '10:05:00')"},
            new String[] {"SUBTIME('00:20:00', '838:00:00')"},
            new String[] {"SUBTIME('00:20:00', '839:00:00')"},
            new String[] {"SUBTIME('-989:20:00.999', '839:00:00')"},
            new String[] {"SUBTIME('-989:20:00.999', '40:00:00')"},
            new String[] {"SUBTIME('2010-10-20 01:00:00', '839:00:00');"},
            new String[] {"SUBTIME('2010-10-20 01:00:00', '9:00:00');"},
            new String[] {"SUBTIME('2010-10-20 01:00:00', '-9:00:00');"},
            new String[] {"SUBTIME('23:59:59', '2 1:1:1');"},
            new String[] {"SUBTIME('23:59:59', '200 1:1:1');"},
            //new String[] { "CURTIME()+0" },
            new String[] {"cast(MAKETIME(12,15,30) as time)"},
            new String[] {"cast(MAKETIME(838,59,59) as time)"},
            new String[] {"cast(MAKETIME(839,15,30) as time)"},
            new String[] {"cast(MAKETIME(-12,15,30) as time)"},
            new String[] {"cast(MAKETIME(-838,59,59) as time)"},
            new String[] {"cast(MAKETIME(-839,15,30) as time)"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','-839:00:00')"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','839:00:00')"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','89:00:00')"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','-839:00:00')"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','12:00:00')"},
            new String[] {"TIMESTAMP('2003-12-31 12:00:00','-12:00:00')"},
            new String[] {"TIME_TO_SEC('838:8:00');"},
            new String[] {"TIME_TO_SEC('-838:8:00');"},
            new String[] {"TIME_TO_SEC('840:8:00');"},
            new String[] {"TIME_TO_SEC('-840:8:00');"},
            new String[] {"TIME_TO_SEC('22:8:00');"},
            new String[] {"TIME_TO_SEC('-22:8:00');"},
            new String[] {"hour('1110:12:23');"},
            new String[] {"minute('1110:12:23');"},
            new String[] {"second('1110:12:23');"}

        );
    }

    public DateTimeFunctionTest(String funcExpression) {
        this.funcExpression = funcExpression;
    }

    @Test
    public void testFunc() throws Exception {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        JdbcUtil.executeUpdate(mysqlConnection, "set session sql_mode = NO_ZERO_IN_DATE");//部分case需要设置这个mode
        selectContentSameAssert("show variables like 'time_zone'", null, mysqlConnection, tddlConnection);
        selectContentSameAssert("SELECT " + funcExpression, null, mysqlConnection, tddlConnection);
    }
}
