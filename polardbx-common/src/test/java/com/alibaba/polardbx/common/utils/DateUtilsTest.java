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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import org.junit.Test;

import java.util.Date;

public class DateUtilsTest {

    @Test
    public void test_day() {
        String time = "20140817";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_date() {
        String time = "2014-03-26 12:12:12";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_timestamp() {
        String time = "2014-3-6 1:1:1.339";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_time() {
        String time = "200801";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_time2() {
        String time = "3 1:1:1";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }
}
