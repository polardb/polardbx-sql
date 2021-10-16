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

import static com.alibaba.polardbx.common.datatype.DecimalRoundMod.*;

public class DecimalRoundTest {
    private void doTestRound(String from, int scale, DecimalRoundMod mode, String result, int error) {
        // string -> decimal
        DecimalStructure d = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertTrue(error1 == 0);

        DecimalStructure to = new DecimalStructure();
        int e = FastDecimalUtils.round(d, to, scale, mode);

        Assert.assertEquals(error, e);
        Assert.assertEquals(result, to.toString());
    }

    @Test
    public void testRound() {
        doTestRound("5678.123451", -4, TRUNCATE, "0", 0);
        doTestRound("5678.123451", -3, TRUNCATE, "5000", 0);
        doTestRound("5678.123451", -2, TRUNCATE, "5600", 0);
        doTestRound("5678.123451", -1, TRUNCATE, "5670", 0);
        doTestRound("5678.123451", 0, TRUNCATE, "5678", 0);
        doTestRound("5678.123451", 1, TRUNCATE, "5678.1", 0);
        doTestRound("5678.123451", 2, TRUNCATE, "5678.12", 0);
        doTestRound("5678.123451", 3, TRUNCATE, "5678.123", 0);
        doTestRound("5678.123451", 4, TRUNCATE, "5678.1234", 0);
        doTestRound("5678.123451", 5, TRUNCATE, "5678.12345", 0);
        doTestRound("5678.123451", 6, TRUNCATE, "5678.123451", 0);
        doTestRound("-5678.123451", -4, TRUNCATE, "0", 0);
        doTestRound("99999999999999999999999999999999999999", -31, TRUNCATE, "99999990000000000000000000000000000000",
            0);
        doTestRound("15.1", 0, HALF_UP, "15", 0);
        doTestRound("15.5", 0, HALF_UP, "16", 0);
        doTestRound("15.9", 0, HALF_UP, "16", 0);
        doTestRound("-15.1", 0, HALF_UP, "-15", 0);
        doTestRound("-15.5", 0, HALF_UP, "-16", 0);
        doTestRound("-15.9", 0, HALF_UP, "-16", 0);
        doTestRound("15.1", 1, HALF_UP, "15.1", 0);
        doTestRound("-15.1", 1, HALF_UP, "-15.1", 0);
        doTestRound("15.17", 1, HALF_UP, "15.2", 0);
        doTestRound("15.4", -1, HALF_UP, "20", 0);
        doTestRound("-15.4", -1, HALF_UP, "-20", 0);
        doTestRound("5.4", -1, HALF_UP, "10", 0);
        doTestRound(".999", 0, HALF_UP, "1", 0);
        doTestRound("999999999", -9, HALF_UP, "1000000000", 0);
        doTestRound("15.1", 0, HALF_EVEN, "15", 0);
        doTestRound("15.5", 0, HALF_EVEN, "16", 0);
        doTestRound("14.5", 0, HALF_EVEN, "14", 0);
        doTestRound("15.9", 0, HALF_EVEN, "16", 0);
        doTestRound("15.1", 0, CEILING, "16", 0);
        doTestRound("-15.1", 0, CEILING, "-15", 0);
        doTestRound("15.1", 0, FLOOR, "15", 0);
        doTestRound("-15.1", 0, FLOOR, "-16", 0);
        doTestRound("999999999999999999999.999", 0, CEILING, "1000000000000000000000", 0);
        doTestRound("-999999999999999999999.999", 0, FLOOR, "-1000000000000000000000", 0);
    }

}