package com.alibaba.polardbx.common.utils;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

public class BigDecimalTest {

    @Test
    public void testBigDecimalFastToBytes() {
        testSpecialValues();
        testLongValues();
        testLargeValues();
    }

    private void testSpecialValues() {
        int SCALE_RANGE = 10;
        for (int scale = 0; scale < SCALE_RANGE; scale++) {
            testBigDecimalFastToBytes("0", scale);
            testBigDecimalFastToBytes(String.valueOf(Integer.MIN_VALUE), scale);
            testBigDecimalFastToBytes(String.valueOf(Integer.MAX_VALUE), scale);
            testBigDecimalFastToBytes(String.valueOf(Long.MIN_VALUE), scale);
            testBigDecimalFastToBytes(String.valueOf(Long.MAX_VALUE), scale);
        }
    }

    private void testLongValues() {
        final int ROUND = 100000;
        Random random = new Random(System.currentTimeMillis());
        int scale;
        String value;
        for (long i = 0; i < ROUND; i++) {
            scale = random.nextInt(10);
            value = String.valueOf(random.nextLong());

            testBigDecimalFastToBytes(value, scale);
        }
    }

    /**
     * random.nextLong() does not return all possible long values
     */
    private void testLargeValues() {
        final int ROUND = 1000;
        Random random = new Random(System.currentTimeMillis());
        int scale;
        for (long i = 0; i < ROUND; i++) {
            scale = random.nextInt(10);
            String value1 = String.valueOf(random.nextLong());
            String value2 = String.valueOf(Math.abs(random.nextLong()));

            testBigDecimalFastToBytes(value1 + value2, scale);

            String value3 = String.valueOf(Math.abs(random.nextLong()));
            testBigDecimalFastToBytes(value1 + value2 + value3, scale);
        }
    }

    private void testBigDecimalFastToBytes(String value, int scale) {
        BigDecimal bigDecimal = new BigDecimal(new BigInteger(value), scale);

        try {
            byte[] expectedBytes = bigDecimal.toString().getBytes();
            byte[] fastBytes;
            fastBytes = BigDecimalUtil.fastGetBigDecimalStringBytes(value.getBytes(), scale);
            Assert.assertArrayEquals(String.format("Failed at: %s, scale: %d", value, scale),
                expectedBytes, fastBytes);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(String.format("Failed at: %s, scale: %d", value, scale));
        }
    }

    @Test
    public void testFastBytesToInt() {
        final int ROUND = 10000;
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < ROUND; i++) {
            int val = random.nextInt();
            val = (val == Integer.MIN_VALUE) ?
                Integer.MAX_VALUE : Math.abs(val);
            byte[] valStrBytes = String.valueOf(val).getBytes();
            int result = LongUtil.fastStringBytesToInt(valStrBytes);
            Assert.assertEquals(val, result);
        }
    }

}
