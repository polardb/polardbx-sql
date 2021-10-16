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

package com.alibaba.polardbx.optimizer.partition.datatype.iterator;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedLongs;
import org.junit.Test;

import java.util.Random;
import java.util.function.Supplier;

public class NumericPartitionFieldIteratorTest {
    private static final Random R = new Random();
    private static final String INT64_MIN_SIGNED = "-9223372036854775808";
    private static final String INT64_MAX_SIGNED = "9223372036854775807";
    private static final String INT64_MAX_UNSIGNED = "18446744073709551615";
    private static final String ZERO_VALUE = "0";

    private static final char[] DIGITS = "1234567890".toCharArray();

    private static String getDigits(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int j = R.nextInt(DIGITS.length);
            char ch = DIGITS[j];
            builder.append(ch);
        }
        return builder.toString();
    }

    private static final int MAX_PRECISION = 15;

    private static byte[] generateDecimal() {
        int precision = R.nextInt(MAX_PRECISION) + 1;
        int scale = Math.min(R.nextInt(precision) + 1, 5);
        if (precision == scale) {
            scale--;
        }
        byte[] res = new byte[scale == 0 ? precision : precision + 1];
        int i = 0;
        res[i++] = (byte) DIGITS[R.nextInt(9) + 1];
        for (; i < precision - scale; i++) {
            res[i] = (byte) DIGITS[R.nextInt(10)];
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1; i++) {
            res[i] = (byte) DIGITS[R.nextInt(10)];
        }
        return res;
    }

    private static String randomLong(String min, String max, boolean isUnsigned) {
        long rangeMin = isUnsigned ? Long.parseUnsignedLong(min) : Long.parseLong(min);
        long rangeMax = isUnsigned ? Long.parseUnsignedLong(max) : Long.parseLong(max);
        Preconditions.checkArgument(isUnsigned ? UnsignedLongs.compare(rangeMin, rangeMax) <= 0 : rangeMin <= rangeMax);
        long x = rangeMin + (int) (Math.random() * ((rangeMax - 2 - rangeMin)));
        return isUnsigned ? Long.toUnsignedString(x) : Long.toString(x);
    }

    private static String randomLong(String min, String max) {
        return randomLong(min, max, false);
    }

    private enum Int64Range {
        // v < signed_min
        LT_SIGNED_MIN(null, INT64_MIN_SIGNED, () -> INT64_MIN_SIGNED + R.nextInt(100)),

        // v = signed_min
        SIGNED_MIN(INT64_MIN_SIGNED, INT64_MIN_SIGNED, () -> INT64_MIN_SIGNED),

        // signed_min < v < 0
        SIGNED_MIN_ZERO(INT64_MIN_SIGNED, ZERO_VALUE, () -> randomLong(INT64_MIN_SIGNED, ZERO_VALUE)),

        // v = 0
        ZERO(ZERO_VALUE, ZERO_VALUE, () -> ZERO_VALUE),

        // 0 < v < signed_max
        ZERO_SIGNED_MAX(ZERO_VALUE, INT64_MAX_SIGNED, () -> randomLong(ZERO_VALUE, INT64_MAX_SIGNED)),

        // v = signed_max
        SIGNED_MAX(INT64_MAX_SIGNED, INT64_MAX_SIGNED, () -> INT64_MAX_SIGNED),

        // signed_max < v < unsigned_max
        SIGNED_MAX_UNSIGNED_MAX(
            INT64_MAX_SIGNED, INT64_MAX_UNSIGNED, () -> randomLong(INT64_MAX_SIGNED, INT64_MAX_UNSIGNED, true)),

        // v = unsigned_max
        UNSIGNED_MAX(INT64_MAX_UNSIGNED, INT64_MAX_UNSIGNED, () -> INT64_MAX_UNSIGNED),

        // > unsigned_max
        GT_UNSIGNED_MAX(INT64_MAX_UNSIGNED, null, () -> INT64_MAX_UNSIGNED + R.nextInt(100));

        final String lowerBound;
        final String upperBound;
        final Supplier<String> generator;

        Int64Range(String lowerBound, String upperBound, Supplier<String> generator) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.generator = generator;
        }
    }

    public static void main(String[] args) {
        for (Int64Range range : Int64Range.values()) {
            System.out.println("=======");
            System.out.println(range.name());
            System.out.println(range.generator.get());
            System.out.println();
        }
    }

    @Test
    public void test() {
        testInterval(true, false, false);
    }

    // for signed/unsigned
    // valid / invalid
    // valid: normal/inverted
    // numeric value: < min value =min value, <0, >0, =max value, > max value
    protected void testInterval(boolean isUnsigned, boolean isInValid, boolean isInverted) {
        DataType dataType = isUnsigned ? new ULongType() : new LongType();
        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(dataType);
        PartitionField f1 = PartitionFieldBuilder.createField(dataType);
        PartitionField f2 = PartitionFieldBuilder.createField(dataType);

        // normal:
        Int64Range[] int64Ranges = Int64Range.values();
        for(int i = 0; i < int64Ranges.length; i++) {
            for (int j = i + 1; j < int64Ranges.length; j++) {
                f1.reset();
                f2.reset();

                String lower = int64Ranges[i].generator.get();
                String upper = int64Ranges[j].generator.get();

                f1.store(lower, new VarcharType());
                f2.store(upper, new VarcharType());

                iterator.range(f1, f2, true, true);

                // to yml
                System.out.println("from: " + lower + ", to: " + upper);
                System.out.println(iterator.count());
            }
        }
    }
}
