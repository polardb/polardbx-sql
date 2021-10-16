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

package com.alibaba.polardbx.common.async;

import org.junit.Test;

import java.time.LocalTime;

import static org.junit.Assert.assertEquals;

public class AsyncTaskUtilsTest {

    @Test
    public void test1() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("00:00-00:05");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(0, 0, 0),
                LocalTime.of(0, 5, 0)
        );
        assertEquals(expected, actual);
        assertEquals(300, actual.getDuration());
    }

    @Test
    public void test2() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("00:00 -00:30 ");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(0, 0, 0),
                LocalTime.of(0, 30, 0)
        );
        assertEquals(expected, actual);
        assertEquals(1800, actual.getDuration());
    }

    @Test
    public void test3() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("02:30 - 04:30");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(2, 30, 0),
                LocalTime.of(4, 30, 0)
        );
        assertEquals(expected, actual);
        assertEquals(7200, actual.getDuration());
    }

    @Test
    public void test4() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("22:30-04:30");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(22, 30, 0),
                LocalTime.of(4, 30, 0)
        );
        assertEquals(expected, actual);
        assertEquals(3600 * 6, actual.getDuration());
    }

    @Test
    public void test5() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("00:00-00:00");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(0, 0, 0),
                LocalTime.of(0, 0, 0)
        );
        assertEquals(expected, actual);
        assertEquals(3600 * 24, actual.getDuration());
    }

    @Test
    public void test6() throws Exception {
        TimeInterval actual = AsyncTaskUtils.parseTimeInterval("22:30:20-04:30:40");
        TimeInterval expected = new TimeInterval(
                LocalTime.of(22, 30, 20),
                LocalTime.of(4, 30, 40)
        );
        assertEquals(expected, actual);
        assertEquals(3600 * 6 + 20, actual.getDuration());
    }
}
