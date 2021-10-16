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

package com.alibaba.polardbx.druid.bvt.sql.mysql.parseDate;

import com.alibaba.polardbx.druid.util.MySqlUtils;
import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class MySqlUtils_parseDate_16 extends TestCase {
    String[] months = {"1", "10", "12"};
    String[] days = {"1", "10", "28"};
    String[] hours = {"0", "1", "10", "23"};
    String[] minutes = {"0", "1", "10", "59"};
    String[] seconds = {"0", "1", "10", "59"};
    String[] millis = {"0", "1", "10", "100", "789"};

    public void test_datetime_0() throws Exception {
        String year = "2018";
        for (int m = 0; m < months.length; ++m) {
            for (int d = 0; d < days.length; ++d) {
                for (int h = 0; h < hours.length; ++h) {
                    for (int i = 0; i < minutes.length; ++i) {
                        for (int s = 0; s < seconds.length; ++s) {
                            String dateStr = year + "-" + months[m] + "-" + days[d] + " " + hours[h] + ":" + minutes[i] + ":" + seconds[s];

                            TimeZone timeZone = TimeZone.getDefault();
                            Date date = MySqlUtils.parseDate(dateStr, timeZone);
                            assertNotNull(dateStr, date);

                            SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
                            format.setTimeZone(timeZone);

                            String str2 = format.format(date);
                            assertEquals(dateStr, str2);
                        }
                    }
                }
            }
        }
    }

    public void test_datetime_1() throws Exception {
        String year = "2018";
        for (int m = 0; m < months.length; ++m) {
            for (int d = 0; d < days.length; ++d) {
                for (int h = 0; h < hours.length; ++h) {
                    for (int i = 0; i < minutes.length; ++i) {
                        for (int s = 0; s < seconds.length; ++s) {
                            for (int j = 0; j < millis.length; ++j) {
                                String dateStr = year + "-" + months[m] + "-" + days[d] + " " + hours[h] + ":" + minutes[i] + ":" + seconds[s] + "." + millis[j];

                                TimeZone timeZone = TimeZone.getDefault();
                                Date date = MySqlUtils.parseDate(dateStr, timeZone);
                                assertNotNull(dateStr, date);

                                SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s.S");
                                format.setTimeZone(timeZone);

                                String str2 = format.format(date);
                                assertEquals(dateStr, str2);
                            }
                        }
                    }
                }
            }
        }
    }

}
