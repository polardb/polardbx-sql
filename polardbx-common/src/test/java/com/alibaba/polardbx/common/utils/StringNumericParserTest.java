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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.stream.IntStream;

public class StringNumericParserTest {
    private static final Random R = new Random();
    private static final String NUMBER_STR = "0123456789";
    private static final int MAX_PRECISION = 15;
    private static byte[] generateDecimal() {
        int precision = R.nextInt(MAX_PRECISION) + 1;
        int scale = Math.min(R.nextInt(precision) + 1, 5);
        if (precision == scale) {
            scale--;
        }
        byte[] res = new byte[scale == 0 ? precision : precision + 1];
        int i = 0;
        res[i++] = (byte) NUMBER_STR.charAt(R.nextInt(9) + 1);
        for (; i < precision - scale; i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1; i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        return res;
    }

    @Test
    public void testParsingWithRound() {
        long[] result = new long[3];
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    StringNumericParser.parseStringWithRound(bytes, 0, bytes.length, false, result);
                    String actual = String.valueOf(result[StringNumericParser.NUMERIC_INDEX]);
                    String expect = new BigDecimal(new String(bytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "original bytes = " + new String(bytes) + ", actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundMinus() {
        long[] result = new long[3];
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    // change to negative number
                    byte[] newBytes = new byte[bytes.length + 1];
                    newBytes[0] = '-';
                    System.arraycopy(bytes, 0, newBytes, 1, bytes.length);

                    StringNumericParser.parseStringWithRound(newBytes, 0, newBytes.length, false, result);
                    String actual = String.valueOf(result[StringNumericParser.NUMERIC_INDEX]);
                    String expect = new BigDecimal(new String(newBytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundUnsigned() {
        long[] result = new long[3];
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    StringNumericParser.parseStringWithRound(bytes, 0, bytes.length, true, result);
                    String actual = String.valueOf(result[StringNumericParser.NUMERIC_INDEX]);
                    String expect = new BigDecimal(new String(bytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "original bytes = " + new String(bytes) + ", actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundMinusUnsigned() {
        long[] result = new long[3];
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    // change to negative number
                    byte[] newBytes = new byte[bytes.length + 1];
                    newBytes[0] = '-';
                    System.arraycopy(bytes, 0, newBytes, 1, bytes.length);

                    StringNumericParser.parseStringWithRound(newBytes, 0, newBytes.length, true, result);
                    String actual = String.valueOf(result[StringNumericParser.NUMERIC_INDEX]);
                    String expect = new BigDecimal(0).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "actual = " + actual + ", expect = " + expect);
                }
            );
    }
}
