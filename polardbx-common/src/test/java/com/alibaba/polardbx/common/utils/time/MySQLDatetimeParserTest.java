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
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

public class MySQLDatetimeParserTest extends TimeTestBase {

    protected void check(String from, String to) {
        Preconditions.checkNotNull(from);
        MysqlDateTime t = StringTimeParser.parseDatetime(from.getBytes());
        if (t == null) {
            Assert.assertTrue(to == null);
        } else {
            Assert.assertEquals("from = " + from + ", to = " + to, to, t.toString());
        }
    }

    @Test
    public void testRound() {
        check("2020-1-13 2:19:20.123456", "2020-01-13 02:19:20.123456");

        check("2020-1-13 2:19:20.1234567", "2020-01-13 02:19:20.123457");

        check("2020-1-13 2:19:20.123456789", "2020-01-13 02:19:20.123457");

        check("2020-1-13 2:19:20.123456789987654321", "2020-01-13 02:19:20.123457");

        check("2019-12-31 23:59:59.999999", "2019-12-31 23:59:59.999999");

        check("2019-12-31 23:59:59.9999991", "2019-12-31 23:59:59.999999");

        check("2019-12-31 23:59:59.9999999", "2020-01-01 00:00:00");

        check("2019-2-28 23:59:59.9999999", "2019-03-01 00:00:00");

        check("2000-2-28 23:59:59.9999999", "2000-02-29 00:00:00");
    }

    @Test
    public void testNormal() {
        check("2020-1-13 2:19:20.015423", "2020-01-13 02:19:20.015423");

        check("1999-12-03 12:19:20.115423", "1999-12-03 12:19:20.115423");

        check("1999-12-03 12:19:20.123456789", "1999-12-03 12:19:20.123457");

        check("2020-1-13", "2020-01-13 00:00:00");

        check("1999-12-03", "1999-12-03 00:00:00");

        check("1999-12-03", "1999-12-03 00:00:00");
    }

    @Test
    public void testZeroDate() {
        check("0000-1-13 2:19:20.015423", "0000-01-13 02:19:20.015423");

        check("0000-0-13 2:19:20.015423", "0000-00-13 02:19:20.015423");

        check("0000-0-0 2:19:20.015423", "0000-00-00 02:19:20.015423");

        check("0000-0-0 0:0:0.000000", "0000-00-00 00:00:00");

        check("0000-1-13", "0000-01-13 00:00:00");

        check("0000-0-13", "0000-00-13 00:00:00");

        check("0000-0-0", "0000-00-00 00:00:00");
    }

    @Test
    public void testCriticalValue() {
        check("9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999");

        check("0-0-0 1:1:1.000001", "0000-00-00 01:01:01.000001");

        check("1101-2-29 1:1:1.000001", null);
    }

    @Test
    public void testAbnormal() {
        check("2020-1-13 2:19:20.015423 -- ", "2020-01-13 02:19:20.015423");

        check("1999-12-03 12:19:20.115423   0r-", "1999-12-03 12:19:20.115423");

        check("1999-12-03 12:19:20.123456789 re", "1999-12-03 12:19:20.123457");

        check("2020-1-13 0", "2020-01-13 00:00:00");

        check("1999-12-03 09", "1999-12-03 09:00:00");

        check("1999-12-03 9:9", "1999-12-03 09:09:00");

        check("03-12-03 9:9", "2003-12-03 09:09:00");

        check("70-12-03 9:9", "1970-12-03 09:09:00");

        check("03-12-03", "2003-12-03 00:00:00");

        check("70-12-0", "1970-12-00 00:00:00");
    }

}
