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

package com.alibaba.polardbx.common.datatype;

import org.junit.Assert;
import org.junit.Test;

public class DecimalShiftTest {
    private void doTestShift(String from, int scale, String result, int error) {
        // string -> decimal
        DecimalStructure d = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertTrue(error1 == 0);

        int e = FastDecimalUtils.shift(d, d, scale);

        Assert.assertEquals(error, e);
        Assert.assertEquals(result, d.toString());
    }

    @Test
    public void testShift() {
        doTestShift("123.123", 1, "1231.23", 0);
        doTestShift("123457189.123123456789000", 1, "1234571891.23123456789", 0);
        doTestShift("123457189.123123456789000", 4, "1234571891231.23456789", 0);
        doTestShift("123457189.123123456789000", 8, "12345718912312345.6789", 0);
        doTestShift("123457189.123123456789000", 9, "123457189123123456.789", 0);
        doTestShift("123457189.123123456789000", 10, "1234571891231234567.89", 0);
        doTestShift("123457189.123123456789000", 17, "12345718912312345678900000", 0);
        doTestShift("123457189.123123456789000", 18, "123457189123123456789000000", 0);
        doTestShift("123457189.123123456789000", 19, "1234571891231234567890000000", 0);
        doTestShift("123457189.123123456789000", 26, "12345718912312345678900000000000000", 0);
        doTestShift("123457189.123123456789000", 27, "123457189123123456789000000000000000", 0);
        doTestShift("123457189.123123456789000", 28, "1234571891231234567890000000000000000", 0);
        doTestShift("000000000000000000000000123457189.123123456789000", 26, "12345718912312345678900000000000000", 0);
        doTestShift("00000000123457189.123123456789000", 27, "123457189123123456789000000000000000", 0);
        doTestShift("00000000000000000123457189.123123456789000", 28, "1234571891231234567890000000000000000", 0);
        doTestShift("123", 1, "1230", 0);
        doTestShift("123", 10, "1230000000000", 0);
        doTestShift(".123", 1, "1.23", 0);
        doTestShift(".123", 10, "1230000000", 0);
        doTestShift(".123", 14, "12300000000000", 0);
        doTestShift("000.000", 1000, "0.000", 0);
        doTestShift("000.", 1000, "0", 0);
        doTestShift(".000", 1000, "0.000", 0);
        doTestShift("1", 1000, "1", 2);
        doTestShift("123.123", -1, "12.3123", 0);
        doTestShift("123987654321.123456789000", -1, "12398765432.1123456789", 0);
        doTestShift("123987654321.123456789000", -2, "1239876543.21123456789", 0);
        doTestShift("123987654321.123456789000", -3, "123987654.321123456789", 0);
        doTestShift("123987654321.123456789000", -8, "1239.87654321123456789", 0);
        doTestShift("123987654321.123456789000", -9, "123.987654321123456789", 0);
        doTestShift("123987654321.123456789000", -10, "12.3987654321123456789", 0);
        doTestShift("123987654321.123456789000", -11, "1.23987654321123456789", 0);
        doTestShift("123987654321.123456789000", -12, "0.123987654321123456789", 0);
        doTestShift("123987654321.123456789000", -13, "0.0123987654321123456789", 0);
        doTestShift("123987654321.123456789000", -14, "0.00123987654321123456789", 0);
        doTestShift("00000087654321.123456789000", -14, "0.00000087654321123456789", 0);
    }
}
