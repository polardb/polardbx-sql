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

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DIV_ZERO;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OVERFLOW;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_TRUNCATED;

public class DecimalConverterTest {

    private void doTestString(String from, String to, int error) {
        DecimalStructure d = new DecimalStructure();
        int e = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertEquals(error, e);

        String res = d.toString();
        Assert.assertEquals(to, res);
    }

    @Test
    public void testString() {
        doTestString("12345", "12345", E_DEC_OK);
        doTestString("12345.", "12345", E_DEC_OK);
        doTestString("123.45.", "123.45", E_DEC_OK);
        doTestString("-123.45.", "-123.45", E_DEC_OK);
        doTestString(".00012345000098765", "0.00012345000098765", E_DEC_OK);
        doTestString(".12345000098765", "0.12345000098765", E_DEC_OK);
        doTestString("-.000000012345000098765", "-0.000000012345000098765", E_DEC_OK);
        doTestString("1234500009876.5", "1234500009876.5", E_DEC_OK);
        doTestString("123E5", "12300000", E_DEC_OK);
        doTestString("123E-2", "1.23", E_DEC_OK);

        doTestString("99999999999999999999999999999999999999999999999999999999999999999",
            "99999999999999999999999999999999999999999999999999999999999999999", E_DEC_OK);
    }

    private void doTestBinary(String from, int precision, int scale, String to, int error) {

        // string -> decimal
        DecimalStructure d1 = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d1, false);
        Assert.assertTrue(error1 == 0);

        // decimal -> binary
        byte[] result = new byte[DecimalConverter.binarySize(precision, scale)];
        int resError = DecimalConverter.decimalToBin(d1, result, precision, scale);

        // binary -> decimal
        DecimalStructure d2 = new DecimalStructure();
        DecimalConverter.binToDecimal(result, d2, precision, scale);

        Assert.assertEquals(error, resError);
        if (error != E_DEC_DIV_ZERO) {
            String expected = to;
            // decimal -> string
            String actual = d2.toString();
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void test() {
        doTestBinary("-10.55", 4, 2, "-10.55", 0);
        doTestBinary("0.0123456789012345678912345", 30, 25, "0.0123456789012345678912345", 0);
        doTestBinary("12345", 5, 0, "12345", 0);
        doTestBinary("12345", 10, 3, "12345.000", 0);
        doTestBinary("123.45", 10, 3, "123.450", 0);
        doTestBinary("-123.45", 20, 10, "-123.4500000000", 0);
        doTestBinary(".00012345000098765", 15, 14, "0.00012345000098", E_DEC_TRUNCATED);
        doTestBinary(".00012345000098765", 22, 20, "0.00012345000098765000", 0);
        doTestBinary(".12345000098765", 30, 20, "0.12345000098765000000", 0);
        doTestBinary("-.000000012345000098765", 30, 20, "-0.00000001234500009876", E_DEC_TRUNCATED);
        doTestBinary("1234500009876.5", 30, 5, "1234500009876.50000", 0);
        doTestBinary("111111111.11", 10, 2, "11111111.11", E_DEC_OVERFLOW);
        doTestBinary("000000000.01", 7, 3, "0.010", 0);
        doTestBinary("123.4", 10, 2, "123.40", 0);
        doTestBinary("1000", 3, 0, "0", E_DEC_OVERFLOW);
        doTestBinary("0.1", 1, 1, "0.1", 0);
        doTestBinary("0.100", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0.1000", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0.10000", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0.100000", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0.1000000", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0.10", 1, 1, "0.1", E_DEC_TRUNCATED);
        doTestBinary("0000000000000000000000000000000000000000000.000000000000123000000000000000", 15, 15,
            "0.000000000000123", E_DEC_TRUNCATED);
        doTestBinary("00000000000000000000000000000.00000000000012300", 15, 15, "0.000000000000123", E_DEC_TRUNCATED);
        doTestBinary("0000000000000000000000000000000000000000000.0000000000001234000000000000000", 16, 16,
            "0.0000000000001234", E_DEC_TRUNCATED);
        doTestBinary("00000000000000000000000000000.000000000000123400", 16, 16, "0.0000000000001234", E_DEC_TRUNCATED);
        doTestBinary("0.1", 2, 2, "0.10", 0);
        doTestBinary("0.10", 3, 3, "0.100", 0);
        doTestBinary("0.1", 3, 1, "0.1", 0);
        doTestBinary("0.0000000000001234", 32, 17, "0.00000000000012340", 0);
        doTestBinary("0.0000000000001234", 20, 20, "0.00000000000012340000", 0);
    }

    private void doTestToLong(String from, long to, int error) {
        // string -> decimal
        DecimalStructure d = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertTrue(error1 == 0);

        // decimal -> long
        long[] res = DecimalConverter.decimal2Long(d);
        Assert.assertEquals(error, res[1]);
        Assert.assertEquals(to, res[0]);
    }

    @Test
    public void testToLong() {
        doTestToLong("18446744073709551615", 9223372036854775807L, E_DEC_OVERFLOW);
        doTestToLong("-1", -1L, E_DEC_OK);
        doTestToLong("1", 1L, E_DEC_OK);
        doTestToLong("-1.23", -1L, E_DEC_TRUNCATED);
        doTestToLong("-9223372036854775807", -9223372036854775807L, E_DEC_OK);
        doTestToLong("-9223372036854775808", -9223372036854775808L, E_DEC_OK);
        doTestToLong("9223372036854775808", 9223372036854775807L, E_DEC_OVERFLOW);
        doTestToLong("-9223372036854775809", -9223372036854775808L, E_DEC_OVERFLOW);
    }

    private void doTestToULong(String from, long to, int error) {
        // string -> decimal
        DecimalStructure d = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertTrue(error1 == 0);

        // decimal -> unsigned long
        long[] res = DecimalConverter.decimalToULong(d);
        Assert.assertEquals(error, res[1]);
        Assert.assertEquals(to, res[0]);
    }

    @Test
    public void testToULong() {
        doTestToULong("12345", 12345, E_DEC_OK);
        doTestToULong("0", 0, E_DEC_OK);
        doTestToULong("18446744073709551615", Long.parseUnsignedLong("18446744073709551615"), E_DEC_OK);
        doTestToULong("18446744073709551616", Long.parseUnsignedLong("18446744073709551615"), E_DEC_OVERFLOW);
        doTestToULong("-1", 0, E_DEC_OVERFLOW);
        doTestToULong("1.23", 1, E_DEC_TRUNCATED);
        doTestToULong("9999999999999999999999999.000", Long.parseUnsignedLong("18446744073709551615"), E_DEC_OVERFLOW);
    }

    private void doTestToDouble(String from, double to) {
        // string -> decimal
        DecimalStructure d = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d, false);
        Assert.assertTrue(error1 == 0);

        // decimal -> double
        double res = DecimalConverter.decimalToDouble(d);
        Assert.assertTrue(to == res);
    }

    @Test
    public void testToDouble() {
        doTestToDouble("12345", 12345d);
        doTestToDouble("123.45", 123.45d);
        doTestToDouble("-123.45", -123.45d);
        doTestToDouble("0.00012345000098765", 0.00012345000098765d);
        doTestToDouble("1234500009876.5", 1234500009876.5d);
    }

    private void doTestLongToDecimal(long from, String to, int error) {
        // long -> decimal
        DecimalStructure d = new DecimalStructure();
        int e = DecimalConverter.longToDecimal(from, d);
        Assert.assertEquals(error, e);
        Assert.assertEquals(to, d.toString());
    }

    @Test
    public void testLongToDecimal() {
        doTestLongToDecimal(-12345L, "-12345", E_DEC_OK);
        doTestLongToDecimal(-1L, "-1", E_DEC_OK);
        doTestLongToDecimal(1L, "1", E_DEC_OK);
        doTestLongToDecimal(-9223372036854775807L, "-9223372036854775807", E_DEC_OK);
        doTestLongToDecimal(-9223372036854775808L, "-9223372036854775808", E_DEC_OK);
    }

    private void doTestUnsignedLongToDecimal(long from, String to, int error) {
        // long -> decimal
        DecimalStructure d = new DecimalStructure();
        int e = DecimalConverter.unsignedlongToDecimal(from, d);
        Assert.assertEquals(error, e);
        Assert.assertEquals(to, d.toString());
    }

    @Test
    public void testUnsignedToDecimal() {
        doTestUnsignedLongToDecimal(12345L, "12345", E_DEC_OK);
        doTestUnsignedLongToDecimal(0L, "0", E_DEC_OK);
        doTestUnsignedLongToDecimal(Long.parseUnsignedLong("18446744073709551615"), "18446744073709551615", E_DEC_OK);
    }

    @Test
    public void testGetUnscaledDecimal() {
        doTestGetUnscaledDecimal(new byte[] {-125, -50, 50, -111, 69}, 10, 2, 6384500969L);
    }

    private void doTestGetUnscaledDecimal(byte[] buffer, int precision, int scale,
                                          long expectResult) {
        long unscaledDecimal = DecimalConverter.getUnscaledDecimal(buffer, precision, scale);
        Assert.assertEquals(expectResult, unscaledDecimal);
    }

    @Test
    public void testGetDecimal() {
        doTestGetDecimal(new byte[] {-125, -50, 50, -111, 69}, 10, 2, Decimal.fromString("63845009.69"));
    }

    private void doTestGetDecimal(byte[] buffer, int precision, int scale,
                                  Decimal expectResult) {
        Decimal decimal = DecimalConverter.getDecimal(buffer, precision, scale);
        Assert.assertEquals(expectResult, decimal);
    }
}