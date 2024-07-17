package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static com.alibaba.polardbx.executor.utils.DecimalTestUtil.gen128BitUnsignedNumStr;

public class Decimal128MathTest {

    private static final Random random = new Random();

    @Test
    public void test128MultiplyIntTo128() {
        final int scale = 2;
        final int range = 10000;

        for (int i = -range; i <= range; i++) {
            String decStr = gen128BitUnsignedNumStr(random, 5, false);
            Decimal decimal128Dec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(decimal128Dec.getDecimalStructure(), decimal128Dec.getDecimalStructure(), -scale);
            decimal128Dec.getDecimalStructure().setFractions(scale);

            validateMultiplyDec128ByInt(decimal128Dec, i, scale, false);

            // test negative decimal 128
            decStr = "-" + decStr;
            decimal128Dec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(decimal128Dec.getDecimalStructure(), decimal128Dec.getDecimalStructure(), -scale);
            decimal128Dec.getDecimalStructure().setFractions(scale);

            validateMultiplyDec128ByInt(decimal128Dec, i, scale, false);
        }

        // input larger decimal128 values and expect overflow
        for (int i = range; i < range * 2; i++) {
            String decStr = gen128BitUnsignedNumStr(random, 1, true);
            Decimal decimal128Dec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(decimal128Dec.getDecimalStructure(), decimal128Dec.getDecimalStructure(), -scale);
            decimal128Dec.getDecimalStructure().setFractions(scale);
            validateMultiplyDec128ByInt(decimal128Dec, i, scale, true);
            validateMultiplyDec128ByInt(decimal128Dec, -i, scale, true);

            decStr = "-" + decStr;
            decimal128Dec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(decimal128Dec.getDecimalStructure(), decimal128Dec.getDecimalStructure(), -scale);
            decimal128Dec.getDecimalStructure().setFractions(scale);
            validateMultiplyDec128ByInt(decimal128Dec, i, scale, true);
            validateMultiplyDec128ByInt(decimal128Dec, -i, scale, true);
        }
    }

    @Test
    public void test64MultiplyIntTo128() {
        final int scale = 2;

        validateMultiplyDec64ByInt(Long.MIN_VALUE, Integer.MIN_VALUE, scale);
        validateMultiplyDec64ByInt(Long.MIN_VALUE, Integer.MAX_VALUE, scale);
        validateMultiplyDec64ByInt(Long.MAX_VALUE, Integer.MIN_VALUE, scale);
        validateMultiplyDec64ByInt(Long.MAX_VALUE, Integer.MAX_VALUE, scale);
        validateMultiplyDec64ByInt(1, Integer.MIN_VALUE, scale);
        validateMultiplyDec64ByInt(-1, Integer.MIN_VALUE, scale);

        final int range = 10000;
        for (int i = -range; i <= range; i++) {
            long decimal64 = random.nextLong();
            validateMultiplyDec64ByInt(decimal64, i, scale);
            validateMultiplyDec64ByInt(-decimal64, i, scale);
            // boundary values
            validateMultiplyDec64ByInt(decimal64, Integer.MAX_VALUE, scale);
            validateMultiplyDec64ByInt(decimal64, Integer.MIN_VALUE, scale);
            validateMultiplyDec64ByInt(Long.MIN_VALUE, i, scale);
            validateMultiplyDec64ByInt(Long.MAX_VALUE, i, scale);
            validateMultiplyDec64ByInt(1, i, scale);
            validateMultiplyDec64ByInt(-1, i, scale);
        }
    }

    @Test
    public void test64Multiply64To128() {
        final int scale = 2;
        validateMultiplyDec64ByDec64(Long.MIN_VALUE, Long.MIN_VALUE, scale);
        validateMultiplyDec64ByDec64(Long.MIN_VALUE, Long.MAX_VALUE, scale);
        validateMultiplyDec64ByDec64(Long.MAX_VALUE, Long.MIN_VALUE, scale);
        validateMultiplyDec64ByDec64(Long.MAX_VALUE, Long.MAX_VALUE, scale);

        final int range = 10000;

        for (int i = -range; i <= range; i++) {
            long decimal64 = random.nextLong();
            long decimal64_2 = random.nextLong();
            validateMultiplyDec64ByDec64(decimal64, decimal64_2, scale);
            validateMultiplyDec64ByDec64(-decimal64, decimal64_2, scale);
            validateMultiplyDec64ByDec64(decimal64, -decimal64_2, scale);
            validateMultiplyDec64ByDec64(-decimal64, -decimal64_2, scale);
            // boundary values
            validateMultiplyDec64ByDec64(Long.MIN_VALUE, decimal64_2, scale);
            validateMultiplyDec64ByDec64(Long.MAX_VALUE, decimal64_2, scale);
            validateMultiplyDec64ByDec64(0, decimal64_2, scale);
            validateMultiplyDec64ByDec64(1, decimal64_2, scale);
            validateMultiplyDec64ByDec64(-1, decimal64_2, scale);
        }
    }

    @Test
    public void testLongSub64To128() {
        final int scale = 2;
        validateSub64ByDec64(Long.MIN_VALUE, Long.MIN_VALUE, scale);
        validateSub64ByDec64(Long.MIN_VALUE, Long.MAX_VALUE, scale);
        validateSub64ByDec64(Long.MAX_VALUE, Long.MIN_VALUE, scale);
        validateSub64ByDec64(Long.MAX_VALUE, Long.MAX_VALUE, scale);

        final int range = 10000;

        for (int i = -range; i <= range; i++) {
            long decimal64 = random.nextLong();
            long decimal64_2 = random.nextLong();
            validateSub64ByDec64(decimal64, decimal64_2, scale);
            validateSub64ByDec64(-decimal64, decimal64_2, scale);
            validateSub64ByDec64(decimal64, -decimal64_2, scale);
            validateSub64ByDec64(-decimal64, -decimal64_2, scale);
            // boundary values
            validateSub64ByDec64(Long.MIN_VALUE, decimal64_2, scale);
            validateSub64ByDec64(Long.MAX_VALUE, decimal64_2, scale);
            validateSub64ByDec64(0, decimal64_2, scale);
            validateSub64ByDec64(decimal64, 0, scale);
        }
    }

    @Test
    public void testLongSub128To128() {
        final int scale = 2;
        final int range = 10000;

        for (int i = -range; i <= range; i++) {
            String decStr = gen128BitUnsignedNumStr(random, 3, false);
            String decStr2 = gen128BitUnsignedNumStr(random, 3, false);
            String decStr3 = "-" + decStr2;
            Decimal decimal128Dec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(decimal128Dec.getDecimalStructure(), decimal128Dec.getDecimalStructure(), -scale);
            decimal128Dec.getDecimalStructure().setFractions(scale);
            Decimal decimal128Dec2 = Decimal.fromString(decStr2);
            FastDecimalUtils.shift(decimal128Dec2.getDecimalStructure(), decimal128Dec2.getDecimalStructure(), -scale);
            decimal128Dec2.getDecimalStructure().setFractions(scale);
            Decimal decimal128Dec3 = Decimal.fromString(decStr3);
            FastDecimalUtils.shift(decimal128Dec3.getDecimalStructure(), decimal128Dec3.getDecimalStructure(), -scale);
            decimal128Dec3.getDecimalStructure().setFractions(scale);

            validateSub128ByDec128(FastDecimalUtils.convertToDecimal128(decimal128Dec), decimal128Dec2, scale, false);
            validateSub128ByDec128(FastDecimalUtils.convertToDecimal128(decimal128Dec), decimal128Dec3, scale, false);
        }

        // test overflow by big value
        String decStr2 = "-" + gen128BitUnsignedNumStr(random, 0, true);
        Decimal decimal128Dec2 = Decimal.fromString(decStr2);
        FastDecimalUtils.shift(decimal128Dec2.getDecimalStructure(), decimal128Dec2.getDecimalStructure(), -scale);
        decimal128Dec2.getDecimalStructure().setFractions(scale);

        validateSub128ByDec128(new long[] {-1L, Long.MAX_VALUE}, decimal128Dec2, scale, true);
    }

    private void validateMultiplyDec128ByInt(Decimal decimal128Dec, int multiplier, int scale, boolean expectOverflow) {
        Decimal rightDecimal = new Decimal();
        DecimalConverter.longToDecimal(multiplier, rightDecimal.getDecimalStructure(), false);
        Decimal targetDecimal = new Decimal();
        FastDecimalUtils.mul(decimal128Dec.getDecimalStructure(),
            rightDecimal.getDecimalStructure(),
            targetDecimal.getDecimalStructure());

        long[] decimal128 = FastDecimalUtils.convertToDecimal128(decimal128Dec);
        long[] result128 = multiplyDecimal128(decimal128[0], decimal128[1], multiplier);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        FastDecimalUtils.setDecimal128WithScale(buffer, result, result128[0], result128[1], scale);
        Decimal resultDecimal = new Decimal(result);
        if (expectOverflow) {
            if (result128[2] != -1) {
                Assert.fail(String.format("Expect multiply overflow, %s * %d, got %s%n", decimal128Dec, multiplier,
                    resultDecimal));
            } else {
                // no need to check result
                return;
            }
        }
        if (result128[2] == -1) {
            Assert.fail(String.format("Expect multiply not overflow, %s * %d%n", decimal128Dec, multiplier));
        }
        Assert.assertEquals(String.format("Decimal is %s. Multiplier is %s, ", decimal128Dec, multiplier),
            targetDecimal, resultDecimal);
    }

    private void validateMultiplyDec64ByInt(long decimal64, int multiplier, int scale) {
        Decimal decimal64Dec = new Decimal(decimal64, scale);

        Decimal rightDecimal = new Decimal();
        DecimalConverter.longToDecimal(multiplier, rightDecimal.getDecimalStructure(), false);
        Decimal targetDecimal = new Decimal();
        FastDecimalUtils.mul(decimal64Dec.getDecimalStructure(),
            rightDecimal.getDecimalStructure(),
            targetDecimal.getDecimalStructure());

        long[] result128 = multiplyDecimal64(decimal64, multiplier);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        FastDecimalUtils.setDecimal128WithScale(buffer, result, result128[0], result128[1], scale);
        Decimal resultDecimal = new Decimal(result);
        Assert.assertEquals(String.format("Decimal is %s. Multiplier is %s, ", decimal64Dec, multiplier),
            targetDecimal, resultDecimal);
    }

    private void validateMultiplyDec64ByDec64(long decimal64, long multiplier, int scale) {
        Decimal decimal64Dec = new Decimal(decimal64, scale);

        Decimal rightDecimal = new Decimal(multiplier, scale);
        Decimal targetDecimal = new Decimal();
        FastDecimalUtils.mul(decimal64Dec.getDecimalStructure(),
            rightDecimal.getDecimalStructure(),
            targetDecimal.getDecimalStructure());

        long[] result128 = multiplyDecimal64(decimal64, multiplier);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        int resultScale = scale + scale;
        FastDecimalUtils.setDecimal128WithScale(buffer, result, result128[0], result128[1], resultScale);
        Decimal resultDecimal = new Decimal(result);
        Assert.assertEquals(String.format("Decimal is %s. Multiplier is %s, ", decimal64Dec, rightDecimal),
            targetDecimal, resultDecimal);
    }

    private void validateSub64ByDec64(long left, long decimal64, int scale) {
        Decimal decimal64Dec = new Decimal(decimal64, scale);

        Decimal leftDecimal = new Decimal(left, scale);
        Decimal targetDecimal = new Decimal();
        FastDecimalUtils.sub(leftDecimal.getDecimalStructure(),
            decimal64Dec.getDecimalStructure(),
            targetDecimal.getDecimalStructure());

        long[] result128 = subDecimal64(left, decimal64);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        FastDecimalUtils.setDecimal128WithScale(buffer, result, result128[0], result128[1], scale);
        Decimal resultDecimal = new Decimal(result);
        Assert.assertEquals(String.format("Decimal is %s. Left is %s, ", decimal64Dec, leftDecimal),
            targetDecimal, resultDecimal);
    }

    private void validateSub128ByDec128(long[] decimal128Left, Decimal decimal128Dec2, int scale,
                                        boolean expectOverflow) {
        Decimal decimal128Dec1 = FastDecimalUtils.convert128ToDecimal(decimal128Left, scale);
        if (decimal128Dec1.scale() != decimal128Dec2.scale()) {
            Assert.fail("Should input decimals with same scale");
        }
        Decimal targetDecimal = new Decimal();
        FastDecimalUtils.sub(decimal128Dec1.getDecimalStructure(),
            decimal128Dec2.getDecimalStructure(),
            targetDecimal.getDecimalStructure());

        long[] decimal128Right = FastDecimalUtils.convertToDecimal128(decimal128Dec2);
        long[] result128 = subDecimal128(decimal128Left[0], decimal128Left[1], decimal128Right[0], decimal128Right[1]);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        FastDecimalUtils.setDecimal128WithScale(buffer, result, result128[0], result128[1], decimal128Dec1.scale());
        Decimal resultDecimal = new Decimal(result);
        if (expectOverflow) {
            if (result128[2] != -1) {
                Assert.fail(String.format("Expect sub overflow,  %s - %s = %s%n", decimal128Dec1, decimal128Dec2,
                    resultDecimal));
            } else {
                // no need to check result
                return;
            }
        }
        if (result128[2] == -1) {
            Assert.fail(String.format("Expect sub not overflow, %s - %s = %s%n", decimal128Dec1, decimal128Dec2,
                resultDecimal));
        }
        Assert.assertEquals(String.format("Decimal1 is %s. Decimal2 is %s, ", decimal128Dec1, decimal128Dec2),
            targetDecimal, resultDecimal);
    }

    /**
     * long - long 不会溢出 decimal128
     */
    private long[] subDecimal64(long leftWithScale, long decimal64) {
        long[] result = new long[2];

        long leftHigh = leftWithScale >= 0 ? 0 : -1;
        long rightDec128Low = decimal64;
        long rightDec128High = rightDec128Low >= 0 ? 0 : -1;

        long newDec128High = leftHigh - rightDec128High;
        long newDec128Low = leftWithScale - rightDec128Low;
        long borrow = ((~leftWithScale & rightDec128Low)
            | (~(leftWithScale ^ rightDec128Low) & newDec128Low)) >>> 63;
        long resultHigh = newDec128High - borrow;

        result[0] = newDec128Low;
        result[1] = resultHigh;
        return result;
    }

    /**
     * decimal128 - decimal128 可能会溢出
     */
    private long[] subDecimal128(long leftLow, long leftHigh, long rightLow, long rightHigh) {
        long[] result = new long[3];

        long newDec128High = leftHigh - rightHigh;
        if (((leftHigh ^ rightHigh) & (leftHigh ^ newDec128High)) < 0) {
            result[2] = -1;
            return result;
        }
        long newDec128Low = leftLow - rightLow;
        long borrow = ((~leftLow & rightLow)
            | (~(leftLow ^ rightLow) & newDec128Low)) >>> 63;
        long resultHigh = newDec128High - borrow;

        result[0] = newDec128Low;
        result[1] = resultHigh;
        return result;
    }

    /**
     * decimal64 * int 不会溢出 decimal128
     */
    private long[] multiplyDecimal64(long decimal64, int multiplier) {
        long[] result = new long[2];
        if (decimal64 == 0 || multiplier == 0) {
            return result;
        }
        if (decimal64 == 1) {
            result[0] = multiplier;
            result[1] = multiplier >= 0 ? 0 : -1;
            return result;
        }
        if (decimal64 == -1) {
            long negMultiplier = -((long) multiplier);
            result[0] = negMultiplier;
            result[1] = negMultiplier >= 0 ? 0 : -1;
            return result;
        }
        if (multiplier == 1) {
            result[0] = decimal64;
            result[1] = decimal64 >= 0 ? 0 : -1;
            return result;
        }
        if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
            result[0] = -decimal64;
            result[1] = -decimal64 >= 0 ? 0 : -1;
            return result;
        }
        boolean positive;
        long multiplierAbs = multiplier;
        long decimal64Abs = Math.abs(decimal64);
        if (multiplier < 0) {
            multiplierAbs = -multiplierAbs;
            positive = decimal64 < 0;
        } else {
            positive = decimal64 >= 0;
        }
        long sum;

        int x1 = (int) decimal64Abs;
        int x2 = (int) (decimal64Abs >>> 32);
        int x3 = 0;
        sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
        x1 = (int) sum;
        sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
        x2 = (int) sum;
        sum = (sum >>> 32);
        x3 = (int) sum;
        if (positive) {
            result[0] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
            result[1] = (x3 & 0xFFFFFFFFL);
        } else {
            result[0] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
            result[1] = ~((x3 & 0xFFFFFFFFL));
            if (result[0] == 0) {
                result[1] += 1;
            }
        }
        return result;
    }

    /**
     * decimal64 * decimal64 不会溢出 decimal128
     */
    private long[] multiplyDecimal64(long decimal64, long multiplier) {
        long[] result = new long[2];
        if (decimal64 == 0 || multiplier == 0) {
            return result;
        }
        if (decimal64 == 1) {
            result[0] = multiplier;
            result[1] = multiplier >= 0 ? 0 : -1;
            return result;
        }
        if (decimal64 == -1 && multiplier != 0x8000000000000000L) {
            long negMultiplier = -multiplier;
            result[0] = negMultiplier;
            result[1] = negMultiplier >= 0 ? 0 : -1;
            return result;
        }
        if (multiplier == 1) {
            result[0] = decimal64;
            result[1] = decimal64 >= 0 ? 0 : -1;
            return result;
        }
        if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
            result[0] = -decimal64;
            result[1] = -decimal64 >= 0 ? 0 : -1;
            return result;
        }
        boolean positive;
        long multiplierAbs = multiplier;
        long decimal64Abs = Math.abs(decimal64);
        if (multiplier < 0) {
            multiplierAbs = -multiplier;
            positive = decimal64 < 0;
        } else {
            positive = decimal64 >= 0;
        }
        long res;
        int x1 = (int) decimal64Abs;
        int x2 = (int) (decimal64Abs >>> 32);
        int y1 = (int) multiplierAbs;
        int y2 = (int) (multiplierAbs >>> 32);

        res = (y1 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL);
        int z1 = (int) res;

        res = (y1 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL)
            + (y2 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL) + (res >>> 32);
        int z2 = (int) res;

        res = (y2 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL) + (res >>> 32);
        int z3 = (int) res;

        res = (res >>> 32);
        int z4 = (int) res;

        if (positive) {
            result[0] = (z1 & 0xFFFFFFFFL) | (((long) z2) << 32);
            result[1] = (z3 & 0xFFFFFFFFL) | (((long) z4) << 32);
        } else {
            result[0] = ~((z1 & 0xFFFFFFFFL) | (((long) z2) << 32)) + 1;
            result[1] = ~((z3 & 0xFFFFFFFFL) | (((long) z4) << 32));
            if (result[0] == 0) {
                result[1] += 1;
            }
        }
        return result;
    }

    /**
     * 乘法在内核不会封装成一个方法调用
     * 避免 long[] 对象的生成
     */
    private long[] multiplyDecimal128(long decimal128Low, long decimal128High, int multiplier) {
        long[] result = new long[3];
        if (multiplier == 0) {
            return result;
        }
        if (decimal128Low == 0 && decimal128High == 0) {
            return result;
        }
        if (multiplier == 1) {
            result[0] = decimal128Low;
            result[1] = decimal128High;
            return result;
        }
        if (multiplier == -1) {
            result[0] = ~decimal128Low + 1;
            result[1] = ~decimal128High;
            if (result[0] == 0) {
                result[1] += 1;
            }
            return result;
        }
        boolean positive;
        long multiplierAbs = multiplier;
        if (multiplier < 0) {
            multiplierAbs = -multiplier;
            positive = decimal128High < 0;
        } else {
            positive = decimal128High >= 0;
        }
        if (decimal128High < 0) {
            decimal128Low = ~decimal128Low + 1;
            decimal128High = ~decimal128High;
            decimal128High += (decimal128Low == 0) ? 1 : 0;
        }
        long sum;
        int x1 = (int) decimal128Low;
        int x2 = (int) (decimal128Low >>> 32);
        int x3 = (int) decimal128High;
        int x4 = (int) (decimal128High >>> 32);
        sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
        x1 = (int) sum;
        sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
        x2 = (int) sum;
        sum = (x3 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
        x3 = (int) sum;
        sum = (x4 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
        x4 = (int) sum;

        if ((sum >> 32) != 0) {
            result[2] = -1;
            return result;
        }
        if (positive) {
            result[0] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
            result[1] = (x3 & 0xFFFFFFFFL) | (((long) x4) << 32);
        } else {
            result[0] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
            result[1] = ~((x3 & 0xFFFFFFFFL) | (((long) x4) << 32));
            if (result[0] == 0) {
                result[1] += 1;
            }
        }

        return result;
    }
}
