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

import org.apache.commons.lang.math.IntRange;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class BigDecimalTest {

    private static final String[] numbers = {
        "1",
        "1.00",
        "-1.23",
        "863785426858238932.3467",
        "9223372036854775807",
        "-9223372036854775808",
        "3.141592653589793238462643383279502884197169",
        "70679821480865132823.0664709",
        "-0.38193261179310511850917153643"
    };
    private static final Random RANDOM = new Random();
    private static final int RANDOM_TEST_TIME = 1 << 12;

    @Test
    public void testFromString() {
        for (String s : numbers) {
            Decimal d1 = Decimal.fromString(s);
            assertEquals(new BigDecimal(s), d1.toBigDecimal());
        }
    }

    @Test
    public void testUnscaled() {
        IntStream.range(0, RANDOM_TEST_TIME).forEach(i -> doRandomTest());
    }

    private void doRandomTest() {
        // random long with sign
        final long unscaled = RANDOM.nextLong();

        // 0 ~ 30
        final int scale = RANDOM.nextInt(DecimalTypeBase.MAX_DECIMAL_SCALE + 1);

        Decimal decimal = new Decimal(unscaled, scale);
        BigDecimal bigDecimal = BigDecimal.valueOf(unscaled, scale);

        // use plain string to check
        Assert.assertEquals(decimal.toString(), bigDecimal.toPlainString());
    }
}