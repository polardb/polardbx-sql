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

package com.alibaba.polardbx.common.utils.convertor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class DateAndSqlDateTest {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testDateAndSqlDate() {
        Calendar c1 = Calendar.getInstance();
        c1.set(2010, 10 - 1, 01, 23, 59, 59);
        c1.set(Calendar.MILLISECOND, 0);
        Date timeDate = c1.getTime();

        Convertor dateToSql = helper.getConvertor(Date.class, java.sql.Date.class);
        java.sql.Date sqlDate = (java.sql.Date) dateToSql.convert(timeDate, java.sql.Date.class);
        Assert.assertNotNull(sqlDate);

        java.sql.Time sqlTime = (java.sql.Time) dateToSql.convert(timeDate, java.sql.Time.class);
        Assert.assertNotNull(sqlTime);

        java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) dateToSql.convert(timeDate, java.sql.Timestamp.class);
        Assert.assertNotNull(sqlTimestamp);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Assert.assertEquals(df.format(timeDate), df.format(sqlDate));

        DateFormat tf = new SimpleDateFormat("HH:mm:ss");
        Assert.assertEquals(tf.format(timeDate), tf.format(sqlTime));

        Convertor sqlToDate = helper.getConvertor(java.sql.Date.class, Date.class);
        Date date = (Date) sqlToDate.convert(sqlTimestamp, Date.class);
        Assert.assertEquals(timeDate, date);
    }

}
