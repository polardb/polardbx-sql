package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.datatype.Decimal;

import java.util.Random;

public class DecimalTestUtil {

    public static String gen128BitUnsignedNumStr(Random random) {
        return gen128BitUnsignedNumStr(random, 1, false);
    }

    public static String gen128BitUnsignedNumStr(Random random, int trimDigit, boolean padding) {
        long l1 = Math.abs(random.nextLong());
        l1 = (l1 < 0) ? Long.MAX_VALUE : l1;
        long l2 = Math.abs(random.nextLong());
        l2 = (l2 < 0) ? Long.MAX_VALUE : l2;
        String largeNumStr = String.format("%d%d", l1, l2);
        if (largeNumStr.length() > Decimal.MAX_128_BIT_PRECISION - trimDigit) {
            largeNumStr = largeNumStr.substring(0, Decimal.MAX_128_BIT_PRECISION - trimDigit);
        }
        if (padding && largeNumStr.length() < Decimal.MAX_128_BIT_PRECISION - trimDigit) {
            for (int i = largeNumStr.length(); i < Decimal.MAX_128_BIT_PRECISION - trimDigit; i++) {
                // just append String
                largeNumStr += "1";
            }
        }
        return largeNumStr;
    }

    public static String getLongBitsWithSep(long longValue) {
        String bitStr = Long.toBinaryString(longValue);
        bitStr = String.format("%64s", bitStr).replace(' ', '0');
        bitStr = bitStr.substring(0, 32) + " " + bitStr.substring(32);
        return bitStr;
    }

    public static String getLongBits(long longValue) {
        String bitStr = Long.toBinaryString(longValue);
        bitStr = String.format("%64s", bitStr).replace(' ', '0');
        return bitStr;
    }
}
