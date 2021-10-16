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

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import org.junit.Assert;
import org.junit.Test;

public class StringNumericParserTest {
    @Test
    public void test() {
        doTest("\n \r+555", "555.0");
        doTest(" 3e3", "3000.0");
        doTest(" 3000e-3", "3.0");
        doTest(" +3000e+3", "3000000.0");
        doTest(" \n-3000e+3", "-3000000.0");
        doTest(" \n<MM-3000e+3", "0.0");

        doTest("12345", "12345.0");
        doTest("12345.", "12345.0");
        doTest("123.45.", "123.45");
        doTest("-123.45.", "-123.45");
    }

    private void doTest(String toParse, String expected) {
        byte[] bytes = toParse.getBytes();
        double d = StringNumericParser.parseStringToDouble(bytes, 0, bytes.length);
        Assert.assertEquals(expected, String.valueOf(d));
    }
}
