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

package com.alibaba.polardbx.common.utils.convertor;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.alibaba.polardbx.common.datatype.Decimal;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jianghang 2011-6-21 下午09:46:42
 */
public class CommonAndCommonTest {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testInteger() {
        int value = 10;

        Object integerValue = helper.getConvertor(int.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Integer.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Integer.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Integer.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Integer.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Integer.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Integer.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Integer.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Integer.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Integer.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Integer.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));

    }

    @Test
    public void testLong() {
        long value = 10;
        Object integerValue = helper.getConvertor(Long.class, Integer.class).convert(value, Integer.class);

        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Long.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Long.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Long.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Long.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Long.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Long.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Long.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Long.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Long.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Long.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testByte() {
        byte value = 10;
        Object integerValue = helper.getConvertor(Byte.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Byte.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Byte.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Byte.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Byte.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Byte.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Byte.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Byte.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Byte.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Byte.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Byte.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testDouble() {
        double value = 10;
        Object integerValue = helper.getConvertor(Double.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Double.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Double.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Double.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Double.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Double.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Double.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Double.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf((long) value));

        Object bigDecimalValue = helper.getConvertor(Double.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromBigDecimal(BigDecimal.valueOf(value)));

        Object boolValue = helper.getConvertor(Double.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Double.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testFloat() {
        float value = 10;
        Object integerValue = helper.getConvertor(Float.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Float.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Float.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Float.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Float.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Float.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Float.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Float.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf((long) value));

        Object bigDecimalValue = helper.getConvertor(Float.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromBigDecimal(BigDecimal.valueOf(value)));

        Object boolValue = helper.getConvertor(Float.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Float.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testBoolean() {
        boolean boolTest = true;
        int value = boolTest ? 1 : 0;

        Object integerValue = helper.getConvertor(Boolean.class, Integer.class).convert(boolTest, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Boolean.class, int.class).convert(boolTest, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Boolean.class, byte.class).convert(boolTest, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Boolean.class, short.class).convert(boolTest, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Boolean.class, long.class).convert(boolTest, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Boolean.class, float.class).convert(boolTest, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Boolean.class, double.class).convert(boolTest, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Boolean.class, BigInteger.class).convert(boolTest,
            BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Boolean.class, Decimal.class).convert(boolTest, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Boolean.class, boolean.class).convert(boolTest, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(boolTest));

        Object charValue = helper.getConvertor(Boolean.class, char.class).convert(boolTest, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) (value)));
    }

    @Test
    public void testShort() {
        short value = 10;

        Object integerValue = helper.getConvertor(Short.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Short.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Short.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(Short.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(Short.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Short.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Short.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Short.class, BigInteger.class).convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Short.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Short.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Short.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testChar() {
        Character value = 10;

        Object integerValue = helper.getConvertor(Character.class, Integer.class).convert(value, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Character.class, int.class).convert(value, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Character.class, byte.class).convert(value, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) (char) value));

        Object shortValue = helper.getConvertor(Character.class, short.class).convert(value, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) (char) value));

        Object longValue = helper.getConvertor(Character.class, long.class).convert(value, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Character.class, float.class).convert(value, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Character.class, double.class).convert(value, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Character.class, BigInteger.class)
            .convert(value, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(Character.class, Decimal.class).convert(value, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(Character.class, boolean.class).convert(value, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Character.class, char.class).convert(value, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testBigInteger() {
        BigInteger oneValue = BigInteger.TEN;
        int value = oneValue.intValue();

        Object integerValue = helper.getConvertor(BigInteger.class, Integer.class).convert(oneValue, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(BigInteger.class, int.class).convert(oneValue, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(BigInteger.class, byte.class).convert(oneValue, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) (char) value));

        Object shortValue = helper.getConvertor(BigInteger.class, short.class).convert(oneValue, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) (char) value));

        Object longValue = helper.getConvertor(BigInteger.class, long.class).convert(oneValue, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(BigInteger.class, float.class).convert(oneValue, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(BigInteger.class, double.class).convert(oneValue, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Convertor nullConvertor = helper.getConvertor(BigInteger.class, BigInteger.class);
        Assert.assertNull(nullConvertor);// 相同类型，不需要转化

        Object bigDecimalValue = helper.getConvertor(BigInteger.class, Decimal.class).convert(oneValue, Decimal.class);
        Assert.assertEquals(bigDecimalValue, Decimal.fromLong(value));

        Object boolValue = helper.getConvertor(BigInteger.class, boolean.class).convert(oneValue, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(BigInteger.class, char.class).convert(oneValue, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testBigDecimal() {
        Decimal oneValue = Decimal.fromLong(10);
        int value = oneValue.intValue();

        Object integerValue = helper.getConvertor(Decimal.class, Integer.class).convert(oneValue, Integer.class);
        Assert.assertEquals(integerValue.getClass(), Integer.class);

        Object intValue = helper.getConvertor(Decimal.class, int.class).convert(oneValue, int.class);
        Assert.assertEquals(intValue.getClass(), Integer.class); // 也返回原型对应的Object

        Object byteValue = helper.getConvertor(Decimal.class, byte.class).convert(oneValue, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) (char) value));

        Object shortValue = helper.getConvertor(Decimal.class, short.class).convert(oneValue, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) (char) value));

        Object longValue = helper.getConvertor(Decimal.class, long.class).convert(oneValue, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(Decimal.class, float.class).convert(oneValue, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(Decimal.class, double.class).convert(oneValue, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(Decimal.class, BigInteger.class).convert(oneValue,
            BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Convertor nullConvertor = helper.getConvertor(Decimal.class, Decimal.class);
        Assert.assertNull(nullConvertor);// 相同类型，不需要转化

        Object boolValue = helper.getConvertor(Decimal.class, boolean.class).convert(oneValue, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(Decimal.class, char.class).convert(oneValue, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testError() {
        long maxLong = Long.MAX_VALUE;
        long minLong = Long.MIN_VALUE;

        double maxDouble = Double.MAX_VALUE;
        double minDouble = Double.MIN_VALUE;

        try {
            helper.getConvertor(Long.class, Integer.class).convert(maxLong, Integer.class);
            Assert.fail();// 不会走到这一步
        } catch (ConvertorException e) {
        }

        try {
            helper.getConvertor(Long.class, byte.class).convert(minLong, byte.class);
            Assert.fail();// 不会走到这一步
        } catch (ConvertorException e) {
        }

        try {
            helper.getConvertor(Long.class, short.class).convert(maxLong, short.class);
            Assert.fail();// 不会走到这一步
        } catch (ConvertorException e) {
        }

        try {
            helper.getConvertor(Long.class, long.class).convert(minLong, long.class);
        } catch (ConvertorException e) {
            Assert.fail();// 不会走到这一步
        }

        // try {
        // helper.getConvertor(Double.class, float.class).convert(maxDouble,
        // float.class);
        // Assert.fail();// 不会走到这一步
        // } catch (ConvertorException e) {
        // }

        try {
            helper.getConvertor(Double.class, double.class).convert(minDouble, double.class);
        } catch (ConvertorException e) {
            Assert.fail();// 不会走到这一步
        }

        Object bigIntegerValue = helper.getConvertor(Long.class, BigInteger.class).convert(maxLong, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(maxLong));

        try {
            helper.getConvertor(Long.class, boolean.class).convert(maxLong, float.class);
        } catch (ConvertorException e) {
            Assert.fail();// 不会走到这一步
        }

        try {
            helper.getConvertor(Long.class, char.class).convert(maxLong, char.class);
        } catch (ConvertorException e) {
            Assert.fail();// 不会走到这一步
        }

    }
}
