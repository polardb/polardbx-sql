package com.alibaba.polardbx.common.datatype;

import com.alibaba.polardbx.common.utils.BigDecimalUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;

public class Decimal128Test {

    private static final String[] expectedResults = {
        "0",
        "92233720368512053380",
        "922337203685120550243.23",
        "0.01",
        "0.000",
        "-9991421.5",
        "-9223372036851.1947090",
        "-83.01034833166075237401",
        "535654230258904491850165587133144.8496"
    };

    /**
     * Decimal128 都是在运算过程中由 Decimal64 产生的
     * 这里是人工计算出的结果放在用例中用以对比
     */
    private static final long[][] decimal128Bits = {
        new long[] {0, 0},
        new long[] {-35704700, 4},
        new long[] {-35703055677L, 4999},
        new long[] {1, 0},
        new long[] {0, 0},
        new long[] {-99914215, -1},
        new long[] {35810990, -5},
        new long[] {3222989799L, -450},
        new long[] {2675267201812608688L, 290378740073877438L},
    };

    private static final int[] scales = {
        0, 0, 2, 2, 3, 1, 7, 20, 4
    };

    @BeforeClass
    public static void beforeClass() {
        Assert.assertEquals(expectedResults.length, decimal128Bits.length);
        Assert.assertEquals(expectedResults.length, scales.length);
    }

    @Test
    public void testDecimal128ToDecimal() {
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        for (int i = 0; i < decimal128Bits.length; i++) {
            long[] decimal128 = decimal128Bits[i];
            FastDecimalUtils.setDecimal128WithScale(buffer, result, decimal128[0], decimal128[1], scales[i]);
            Decimal decimal = new Decimal(result);
            String resultStr = decimal.toString();
            Assert.assertEquals("Failed at round: " + i, expectedResults[i], resultStr);
        }
    }

    @Test
    public void testDecimalToDecimal128() {
        for (int i = 0; i < expectedResults.length; i++) {
            try {
                Decimal decimal = Decimal.fromString(expectedResults[i]);
                long[] decimal128 = FastDecimalUtils.convertToDecimal128(decimal);
                Assert.assertArrayEquals("Failed at round: " + i, decimal128Bits[i], decimal128);
            } catch (Throwable e) {
                if (e instanceof AssertionError) {
                    throw e;
                }
                Assert.fail("Failed at round: " + i + ", due to " + e.getMessage());
            }
        }
    }

    @Test
    public void testFastInt128ToBytes() {
        for (int i = 0; i < decimal128Bits.length; i++) {
            long lowBits = decimal128Bits[i][0];
            long highBits = decimal128Bits[i][1];
            BigInteger highBitsInt = BigInteger.valueOf(highBits).shiftLeft(64);
            BigInteger lowBitsInt = BigInteger.valueOf(lowBits & 0x7fffffffffffffffL);
            if (lowBits < 0) {
                lowBitsInt = lowBitsInt.setBit(63);
            }
            BigInteger targetBigInt = highBitsInt.add(lowBitsInt);

            byte[] bytes = BigDecimalUtil.fastInt128ToBytes(lowBits, highBits);
            Assert.assertArrayEquals("Failed at round: " + i, targetBigInt.toString().getBytes(), bytes);
        }
    }
}
