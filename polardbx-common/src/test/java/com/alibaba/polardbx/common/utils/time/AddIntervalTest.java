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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import org.junit.Test;

public class AddIntervalTest {
    @Test
    public void test1() {
        MysqlDateTime t = StringTimeParser.parseDatetime("0000-07-27 19:28:15.9642".getBytes());
        MySQLInterval interval = new MySQLInterval();
        interval.setSecond(1);
        MysqlDateTime ret = MySQLTimeCalculator.addInterval(t, MySQLIntervalType.INTERVAL_SECOND, interval);
        System.out.println(ret);
    }

    @Test
    public void test2() {
        MysqlDateTime t = StringTimeParser.parseDatetime("2019-12-31 23:59:59.1".getBytes());
        MySQLInterval interval = new MySQLInterval();
        interval.setSecond(1);
        MysqlDateTime ret = MySQLTimeCalculator.addInterval(t, MySQLIntervalType.INTERVAL_SECOND, interval);
        System.out.println(ret);
    }
}
