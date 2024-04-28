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

import java.util.Random;

public class RawBytesDecimalUtilsTest {

    private void doTestHash(int hashCode, String... decimalStrings) {
        int[] hs = new int[decimalStrings.length];
        for (int i = 0; i < decimalStrings.length; i++) {
            DecimalStructure d = new DecimalStructure();
            byte[] decimalAsBytes = decimalStrings[i].getBytes();
            int error = DecimalConverter.parseString(decimalAsBytes, d, false);
            hs[i] = RawBytesDecimalUtils.hashCode(d.getDecimalMemorySegment());
            Assert.assertEquals(hashCode, hs[i]);
        }
    }

    @Test
    public void testHash() {
        doTestHash(100000992, "1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000");
        doTestHash(-100000992, "-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000");
        doTestHash(100000031, ".1", "0.1", "0.10", "000000.1", ".10000", "0000.10000", "000000000000000000.1");
        doTestHash(1, "0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000");
        doTestHash(-344349087, ".123456789123456789", ".1234567891234567890", ".12345678912345678900",
            ".123456789123456789000",
            ".1234567891234567890000", "0.123456789123456789",
            ".1234567891234567890000000000", "0000000.123456789123456789000");
        doTestHash(12376, "12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.",
            "12345.00", "12345.000000000", "000012345.0000");
        doTestHash(12300031, "123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000");
        doTestHash(230000992, "123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000");
    }

    private void doTestCmp(String from, String to, int expected) {
        DecimalStructure d1 = new DecimalStructure();
        DecimalConverter.parseString(from.getBytes(), d1, false);

        DecimalStructure d2 = new DecimalStructure();
        DecimalConverter.parseString(to.getBytes(), d2, false);

        boolean equal = RawBytesDecimalUtils.equals(d1.getDecimalMemorySegment(), d2.getDecimalMemorySegment());

        Assert.assertEquals(expected == 0, equal);
    }

    @Test
    public void testCmp() {
        doTestCmp("12", "13", -1);
        doTestCmp("13", "12", 1);
        doTestCmp("-10", "10", -1);
        doTestCmp("10", "-10", 1);
        doTestCmp("-12", "-13", 1);
        doTestCmp("0", "12", -1);
        doTestCmp("-10", "0", -1);
        doTestCmp("4", "4", 0);
        doTestCmp("-1.1", "-1.2", 1);
        doTestCmp("1.2", "1.1", 1);
        doTestCmp("1.1", "1.2", -1);

        doTestCmp("1.1", "11e-1", 0);
        doTestCmp("10.00004000", "00010.00004", 0);
    }

    @Test
    public void testDecimal64() {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            long l = random.nextInt(100_000_000);
            testDecimal64Hash(l, 0);
            testDecimal64Hash(l, 2);
            testDecimal64Hash(l, 5);
            testDecimal64Hash(l, 8);
        }
    }

    private void testDecimal64Hash(long decimal64, int scale) {
        Decimal decimal = new Decimal(decimal64, scale);
        int expectHash = RawBytesDecimalUtils.hashCode(decimal.getMemorySegment());
        int actualHash = RawBytesDecimalUtils.hashCode(decimal64, scale);
        Assert.assertEquals(expectHash, actualHash);
    }
}