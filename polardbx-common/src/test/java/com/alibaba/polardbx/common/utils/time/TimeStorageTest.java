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

package com.alibaba.polardbx.common.utils.time;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.IntStream;

public class TimeStorageTest {
    @Test
    public void testDatetime() {
        IntStream.range(0, 1 << 20)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .map(
                l -> l.get(0)
            )
            .forEach(
                s -> {
                    MysqlDateTime t = StringTimeParser.parseDatetime(((String) s).getBytes());
                    if (t == null) {
                        return;
                    }
                    long l = TimeStorage.writeTimestamp(t);
                    MysqlDateTime t1 = TimeStorage.readTimestamp(l);
                    Assert.assertEquals(t.toString(), t1.toString());
                }
            );
    }

    @Test
    public void testTime() {
        IntStream.range(0, 1 << 20)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .map(
                l -> l.get(0)
            )
            .forEach(
                s -> {
                    MysqlDateTime t = StringTimeParser.parseTime(((String) s).getBytes());
                    if (t == null) {
                        return;
                    }
                    t.setMonth(0);
                    t.setDay(0);
                    long l = TimeStorage.writeTime(t);
                    MysqlDateTime t1 = TimeStorage.readTime(l);
                    Assert.assertEquals(t + " hour", t.getHour(), t1.getHour());
                    Assert.assertEquals(t + " minute", t.getMinute(), t1.getMinute());
                    Assert.assertEquals(t + " second", t.getSecond(), t1.getSecond());
                    Assert.assertEquals(t + " nano", t.getSecondPart(), t1.getSecondPart());
                }
            );
    }

    @Test
    public void testDate() {
        IntStream.range(0, 1 << 20)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .map(
                l -> l.get(0)
            )
            .forEach(
                s -> {
                    MysqlDateTime t = StringTimeParser.parseDatetime(((String) s).getBytes());
                    if (t == null) {
                        return;
                    }
                    t.setHour(0);
                    t.setMinute(0);
                    t.setSecond(0);
                    t.setSecondPart(0);
                    long l = TimeStorage.writeDate(t);
                    MysqlDateTime t1 = TimeStorage.readDate(l);
                    Assert.assertEquals(t.toString(), t1.toString());
                }
            );
    }
}
