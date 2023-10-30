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

import com.alibaba.polardbx.common.utils.Pair;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Random;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DIV_ZERO;

public class DecimalCalculatorTest {

    @Test
    public void testDes() {
        IntStream.range(0, 1 << 5).forEach(i -> {
            DecimalStructure d = new DecimalStructure();
            byte[] decimalAsBytes = generateDecimal();
            int error = DecimalConverter.parseString(decimalAsBytes, d, false);
            Assert.assertTrue(error == 0);
            Pair<byte[], Integer> result = DecimalConverter.decimal2String(d, 0, d.getFractions(), (byte) 0);
            Assert.assertArrayEquals(decimalAsBytes, result.getKey());
        });
    }

    @Test
    public void testMul() {
        IntStream.range(0, 1 << 10).forEach(i -> {
            DecimalStructure d1 = new DecimalStructure();
            byte[] decimalAsBytes1 = generateDecimal(20);
            BigDecimal bigDecimal1 = new BigDecimal(new String(decimalAsBytes1));
            int error1 = DecimalConverter.parseString(decimalAsBytes1, d1, false);
            Assert.assertTrue(error1 == 0);

            DecimalStructure d2 = new DecimalStructure();
            byte[] decimalAsBytes2 = generateDecimal(20);
            BigDecimal bigDecimal2 = new BigDecimal(new String(decimalAsBytes2));
            int error2 = DecimalConverter.parseString(decimalAsBytes2, d2, false);
            Assert.assertTrue(error2 == 0);

            DecimalStructure res = new DecimalStructure();
            FastDecimalUtils.mul(d1, d2, res);
            BigDecimal bigDecimalRes = bigDecimal1.multiply(bigDecimal2);
            String myRes = res.toString();
            try {
                Assert.assertEquals(bigDecimalRes.toPlainString().substring(0, myRes.length()), res.toString());
            } catch (Throwable t) {
                System.out.println(new String(decimalAsBytes1));
                System.out.println(new String(decimalAsBytes2));
                throw t;
            }

        });
    }

    @Test
    public void testAdd() {
        IntStream.range(0, 1 << 10).forEach(i -> {
            DecimalStructure d1 = new DecimalStructure();
            byte[] decimalAsBytes1 = generateDecimal(32);
            BigDecimal bigDecimal1 = new BigDecimal(new String(decimalAsBytes1));
            int error1 = DecimalConverter.parseString(decimalAsBytes1, d1, false);
            Assert.assertTrue(error1 == 0);

            DecimalStructure d2 = new DecimalStructure();
            byte[] decimalAsBytes2 = generateDecimal(32);
            BigDecimal bigDecimal2 = new BigDecimal(new String(decimalAsBytes2));
            int error2 = DecimalConverter.parseString(decimalAsBytes2, d2, false);
            Assert.assertTrue(error2 == 0);

            DecimalStructure res = new DecimalStructure();
            FastDecimalUtils.add(d1, d2, res);
            BigDecimal bigDecimalRes = bigDecimal1.add(bigDecimal2);
            String myRes = res.toString();
            try {
                String actual = res.toString();
                int resScale = actual.length() - actual.indexOf('.') - 1;
                String expected = bigDecimalRes.toPlainString();
                Assert.assertEquals(expected, actual);
            } catch (Throwable t) {
                System.out.println(new String(decimalAsBytes1));
                System.out.println(new String(decimalAsBytes2));
                throw t;
            }

        });
    }

    @Test
    public void testSub() {
        IntStream.range(0, 1 << 10).forEach(i -> {
            DecimalStructure d1 = new DecimalStructure();
            byte[] decimalAsBytes1 = generateDecimal(32);
            BigDecimal bigDecimal1 = new BigDecimal(new String(decimalAsBytes1));
            int error1 = DecimalConverter.parseString(decimalAsBytes1, d1, false);
            Assert.assertTrue(error1 == 0);

            DecimalStructure d2 = new DecimalStructure();
            byte[] decimalAsBytes2 = generateDecimal(32);
            BigDecimal bigDecimal2 = new BigDecimal(new String(decimalAsBytes2));
            int error2 = DecimalConverter.parseString(decimalAsBytes2, d2, false);
            Assert.assertTrue(error2 == 0);

            DecimalStructure res = new DecimalStructure();
            FastDecimalUtils.sub(d1, d2, res);
            BigDecimal bigDecimalRes = bigDecimal1.subtract(bigDecimal2);
            String myRes = res.toString();
            try {
                int len = myRes.length() - 1;
                Assert.assertEquals(bigDecimalRes.toPlainString().substring(0, myRes.length() - 1),
                    res.toString().substring(0, len));
            } catch (Throwable t) {
                System.out.println(new String(decimalAsBytes1));
                System.out.println(new String(decimalAsBytes2));
                throw t;
            }

        });
    }

    @Test
    public void doTestDiv() {
        doTestDiv("120", "10", "12.000000000", 0);
        doTestDiv("123", "0.01", "12300.000000000", 0);
        doTestDiv("120", "100000000000.00000", "0.000000001200000000", 0);
        doTestDiv("123", "0", "", 4);
        doTestDiv("0", "0", "", 4);
        doTestDiv("-12193185.1853376", "98765.4321", "-123.456000000000000000", 0);
        doTestDiv("121931851853376", "987654321", "123456.000000000", 0);
        doTestDiv("0", "987", "0.00000", 0);
        doTestDiv("1", "3", "0.333333333", 0);
        doTestDiv("1.000000000000", "3", "0.333333333333333333", 0);
        doTestDiv("1", "1", "1.000000000", 0);
        doTestDiv("0.0123456789012345678912345", "9999999999", "0.000000000001234567890246913578148141", 0);
        doTestDiv("10.333000000", "12.34500", "0.837019036046982584042122316", 0);
        doTestDiv("10.000000000060", "2", "5.000000000030000000", 0);
    }

    @Test
    public void doTestMod() {
        doTestMod("234", "10", "4", 0);
        doTestMod("234.567", "10.555", "2.357", 0);
        doTestMod("-234.567", "10.555", "-2.357", 0);
        doTestMod("234.567", "-10.555", "2.357", 0);
        doTestMod("99999999999999999999999999999999999999", "3", "0", 0);
        doTestMod("51", "0.003430", "0.002760", 0);
        doTestMod("0.0000000001", "1.0", "0.0000000001", 0);
        doTestMod("0.000", "0.1", "0.000", 0);
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
    }

    @Test
    public void testHash() {
        doTestHash(100000992, "1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000");
        doTestHash(-100000992,"-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000");
        doTestHash(100000031,".1", "0.1", "0.10", "000000.1", ".10000", "0000.10000", "000000000000000000.1");
        doTestHash(1, "0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000");
        doTestHash(-344349087,".123456789123456789", ".1234567891234567890", ".12345678912345678900", ".123456789123456789000",
            ".1234567891234567890000", "0.123456789123456789",
            ".1234567891234567890000000000", "0000000.123456789123456789000");
        doTestHash(12376,"12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.",
            "12345.00", "12345.000000000", "000012345.0000");
        doTestHash(12300031,"123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000");
        doTestHash(230000992,"123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000");
    }

    @Test
    public void testRandomHiveDecimal() {
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> new String(generateDecimal(38)))
            .forEach(s -> {
                HiveDecimalWritable h = new HiveDecimalWritable(s);
                DecimalStructure d = DecimalOrcConverter.transform(h);
                Assert.assertEquals(h.toString(), d.toString());
            });

    }

    @Test
    public void testHiveDecimal() {
        String s = "15296.5768757718839862819873737";

        HiveDecimalWritable h = new HiveDecimalWritable(s);
        DecimalStructure d = DecimalOrcConverter.transform(h);
        Assert.assertEquals(h.toString(), d.toString());
    }

    private void doTestHash(int hashCode, String... decimalStrings) {
        int[] hs = new int[decimalStrings.length];
        for (int i = 0; i < decimalStrings.length; i++) {
            DecimalStructure d = new DecimalStructure();
            byte[] decimalAsBytes = decimalStrings[i].getBytes();
            int error = DecimalConverter.parseString(decimalAsBytes, d, false);
            hs[i] = d.hashCode();
            Assert.assertEquals(hashCode, d.hashCode());
        }
    }

    private int hash(int[] array, int fromIndex, int toIndex) {
        if (array == null) {
            return 0;
        }

        int result = 1;
        for (int i = fromIndex; i != toIndex; i++) {
            int element = array[i];
            result = 31 * result + element;
        }

        return result;
    }

    private static final int TEST_SCALE_INCR = 5;

    private void doTestDiv(String dividend, String divisor, String quotient, int error) {
        DecimalStructure d1 = new DecimalStructure();
        int error1 = DecimalConverter.parseString(dividend.getBytes(), d1, false);
        Assert.assertTrue(error1 == 0);

        DecimalStructure d2 = new DecimalStructure();
        int error2 = DecimalConverter.parseString(divisor.getBytes(), d2, false);
        Assert.assertTrue(error2 == 0);

        DecimalStructure res = new DecimalStructure();
        int resError = FastDecimalUtils.div(d1, d2, res, TEST_SCALE_INCR);
        String myRes = res.toString();

        Assert.assertEquals(error, resError);
        if (error != E_DEC_DIV_ZERO) {
            Assert.assertEquals(quotient, myRes);
        }
    }

    private void doTestMod(String from1, String from2, String expectedRes, int error) {
        DecimalStructure d1 = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from1.getBytes(), d1, false);
        Assert.assertTrue(error1 == 0);

        DecimalStructure d2 = new DecimalStructure();
        int error2 = DecimalConverter.parseString(from2.getBytes(), d2, false);
        Assert.assertTrue(error2 == 0);

        DecimalStructure res = new DecimalStructure();
        int resError = FastDecimalUtils.mod(d1, d2, res);
        String myRes = res.toString();

        Assert.assertEquals(error, resError);
        if (error != E_DEC_DIV_ZERO) {
            Assert.assertEquals(expectedRes, myRes);
        }
    }

    private void doTestCmp(String from, String to, int result) {
        DecimalStructure d1 = new DecimalStructure();
        int error1 = DecimalConverter.parseString(from.getBytes(), d1, false);
        Assert.assertTrue(error1 == 0);

        DecimalStructure d2 = new DecimalStructure();
        int error2 = DecimalConverter.parseString(to.getBytes(), d2, false);
        Assert.assertTrue(error2 == 0);

        int myRes = FastDecimalUtils.compare(d1, d2);

        Assert.assertEquals(result, myRes);
    }

    private static final Random R = new Random();
    private static final String NUMBER_STR = "0123456789";

    private static byte[] generateDecimal() {
        return generateDecimal(65);
    }

    private static byte[] generateDecimal(int maxPrc) {
        int precision = R.nextInt(maxPrc) + 1;
        int scale = R.nextInt(precision) + 1;
        if (precision == scale) {
            scale--;
        }

        boolean isNeg = R.nextInt() % 2 == 0;

        byte[] res = new byte[(scale == 0 ? precision : precision + 1) + (isNeg ? 1 : 0)];
        int i = 0;
        if (isNeg) {
            res[i++] = '-';
        }
        res[i++] = (byte) NUMBER_STR.charAt(R.nextInt(9) + 1);
        for (; i < precision - scale + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1 + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        return res;
    }
}
