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

public class StringTimeParserTest {
    protected void check(String from, String to) {
        Preconditions.checkNotNull(from);
        MysqlDateTime t = StringTimeParser.parseTime(from.getBytes());
        if (t == null) {
            Assert.assertTrue(to == null);
        } else {
            Assert.assertEquals("from = " + from + ", to = " + to, to, t.toStringBySqlType());
        }
    }

    @Test
    public void test() {
        check("20:0:4.342", "20:00:04.342000");

        check("0:1:2.12345678", "00:01:02.123457");

        check("00:40:18.927", "00:40:18.927000");
    }
}
