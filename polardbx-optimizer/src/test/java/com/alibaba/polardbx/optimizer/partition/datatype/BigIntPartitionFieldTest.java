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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.google.common.primitives.UnsignedLongs;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Random;
import java.util.stream.IntStream;

public class BigIntPartitionFieldTest {
    private static final char[] DIGITS = "1234567890".toCharArray();
    private static final Random R = new Random();

    @Test
    public void testUnsigned() {
        PartitionField f1 = new BigIntPartitionField(new ULongType());
        PartitionField f2 = new BigIntPartitionField(new ULongType());
        DataType resultType = new LongType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f1.reset();
                f2.reset();

                long l1 = Math.abs(R.nextLong());
                long l2 = Math.abs(R.nextLong());

                f1.store(l1, resultType);
                f2.store(l2, resultType);

                Assert.assertTrue((l1 < 0 && f1.longValue() == 0) || l1 == f1.longValue(), l1 + ", " + f1.longValue());
                Assert.assertTrue((l2 < 0 && f2.longValue() == 0) || l2 == f2.longValue(), l2 + ", " + f2.longValue());

                boolean cmp1 = UnsignedLongs.compare(l1, l2) > 0;
                boolean cmp2 = f1.compareTo(f2) > 0;

                Assert.assertTrue(cmp1 == cmp2);
            }
        );
    }

    @Test
    public void testSigned() {
        PartitionField f1 = new BigIntPartitionField(new LongType());
        PartitionField f2 = new BigIntPartitionField(new LongType());
        DataType resultType = new LongType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f1.reset();
                f2.reset();

                long l1 = R.nextLong();
                long l2 = R.nextLong();

                f1.store(l1, resultType);
                f2.store(l2, resultType);

                Assert.assertTrue(l1 == f1.longValue(), l1 + ", " + f1.longValue());
                Assert.assertTrue(l2 == f2.longValue(), l2 + ", " + f2.longValue());

                boolean cmp1 = l1 > l2;
                boolean cmp2 = f1.compareTo(f2) > 0;

                Assert.assertTrue(cmp1 == cmp2,
                    "l1 = " + l1 + ", l2 = " + l2 + ", f1 = " + f1.longValue() + ", f2 = " + f2.longValue());
            }
        );
    }

    @Test
    public void testDecimalHalfUp() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new DecimalType();

        String decStr1 = "999.33";
        String decStr2 = "-999.88";

        f.store(Decimal.fromString(decStr1), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals("999"));

        f.reset();

        f.store(Decimal.fromString(decStr2), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals("-1000"));
    }

    @Test
    public void testDecimalSigned() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new DecimalType();

        String decStr1 = "9223372036854775807";
        String decStr2 = "-9223372036854775808";

        f.store(Decimal.fromString(decStr1), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals(decStr1));

        f.reset();

        f.store(Decimal.fromString(decStr2), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals(decStr2));
    }

    @Test
    public void testDecimalUnsigned() {
        PartitionField f = new BigIntPartitionField(new ULongType());
        DataType resultType = new DecimalType();

        String decStr1 = "18446744073709551615";
        String decStr2 = "9223372036854775808";

        f.store(Decimal.fromString(decStr1), resultType);
        Assert.assertTrue(Long.toUnsignedString(f.longValue()).equals(decStr1));

        f.reset();

        f.store(Decimal.fromString(decStr2), resultType);
        Assert.assertTrue(Long.toUnsignedString(f.longValue()).equals(decStr2));
    }

    @Test
    public void testBigDecimalSigned() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new DecimalType();

        String decStr1 = "9223372036854775807";
        String decStr2 = "-9223372036854775808";

        f.store(new BigDecimal(decStr1), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals(decStr1));

        f.reset();

        f.store(new BigDecimal(decStr2), resultType);
        Assert.assertTrue(Long.toString(f.longValue()).equals(decStr2));
    }

    @Test
    public void testBigDecimalUnsigned() {
        PartitionField f = new BigIntPartitionField(new ULongType());
        DataType resultType = new DecimalType();

        String decStr1 = "18446744073709551615";
        String decStr2 = "9223372036854775808";

        f.store(new BigDecimal(decStr1), resultType);
        Assert.assertTrue(Long.toUnsignedString(f.longValue()).equals(decStr1));

        f.reset();

        f.store(new BigDecimal(decStr2), resultType);
        Assert.assertTrue(Long.toUnsignedString(f.longValue()).equals(decStr2));
    }

    @Test
    public void testUnsignedString() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f.reset();

                // simple long numeric test
                String numericStr = getDigits(R.nextInt(16) + 1);
                f.store(numericStr, resultType);

                Assert.assertTrue(f.longValue() == Long.valueOf(numericStr), "error numeric: " + numericStr);
            }
        );
    }

    @Test
    public void testSetNull() {
        PartitionField f1 = new BigIntPartitionField(new ULongType());
        f1.setNull();
        Assert.assertTrue(f1.isNull());

        PartitionField f2 = new BigIntPartitionField(new LongType());
        f2.setNull();
        Assert.assertTrue(f2.isNull());
    }

    private String getDigits(int length) {
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

    @Test
    public void testParsingWithRound() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    f.reset();

                    f.store(bytes, resultType);

                    String actual = String.valueOf(f.longValue());
                    String expect = new BigDecimal(new String(bytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect),
                        "original bytes = " + new String(bytes) + ", actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundMinus() {
        PartitionField f = new BigIntPartitionField(new LongType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    // change to negative number
                    byte[] newBytes = new byte[bytes.length + 1];
                    newBytes[0] = '-';
                    System.arraycopy(bytes, 0, newBytes, 1, bytes.length);

                    f.reset();
                    f.store(newBytes, resultType);

                    String actual = String.valueOf(f.longValue());
                    String expect =
                        new BigDecimal(new String(newBytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundUnsigned() {
        PartitionField f = new BigIntPartitionField(new ULongType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    f.reset();
                    f.store(bytes, resultType);

                    String actual = String.valueOf(f.longValue());
                    String expect = new BigDecimal(new String(bytes)).setScale(0, RoundingMode.HALF_UP).toPlainString();
                    Assert.assertTrue(actual.equals(expect),
                        "original bytes = " + new String(bytes) + ", actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testParsingWithRoundMinusUnsigned() {
        PartitionField f = new BigIntPartitionField(new ULongType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10)
            .mapToObj(i -> generateDecimal())
            .forEach(
                bytes -> {
                    // change to negative number
                    byte[] newBytes = new byte[bytes.length + 1];
                    newBytes[0] = '-';
                    System.arraycopy(bytes, 0, newBytes, 1, bytes.length);

                    f.reset();
                    f.store(newBytes, resultType);

                    String actual = String.valueOf(f.longValue());
                    String expect = new BigDecimal(0).toPlainString();
                    Assert.assertTrue(actual.equals(expect), "actual = " + actual + ", expect = " + expect);
                }
            );
    }

    @Test
    public void testDecimalLT() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {false, false}; // <

        // field < 10
        f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field < 10.3 => field <= 10
        f.store(new BigDecimal("10.3"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = false;

        // field < 10.6 => field <= 11
        f.store(new BigDecimal("10.6"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testDecimalLE() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {false, true}; //<=

        // field <= 10
        f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= 10.3 => field <= 10
        f.store(new BigDecimal("10.3"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= 10.6 => field <= 11
        f.store(new BigDecimal("10.6"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testDecimalGT() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {true, false}; // >

        // field > 10
        f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field > 10.3 => field > 10
        f.store(new BigDecimal("10.3"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field >= 10.6 => field >= 11
        f.store(new BigDecimal("10.6"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testDecimalGE() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {true, true}; // >=

        // field >= 10
        f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field >= 10.3 => field > 10
        f.store(new BigDecimal("10.3"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = true;

        // field >= 10.6 => field >= 11
        f.store(new BigDecimal("10.6"), new DecimalType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharLT() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {false, false}; // <

        // field < 10
        f.store("10", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field < 10.3 => field <= 10
        f.store("10.3", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = false;

        // field < 10.6 => field <= 11
        f.store("10.6", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharLE() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {false, true}; // <=

        // field <= 10
        f.store("10", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= 10.3 => field <= 10
        f.store("10.3", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= 10.6 => field <= 11
        f.store("10.6", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharGT() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {true, false}; // >

        // field > 10
        f.store("10", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field > 10.3 => field > 10
        f.store("10.3", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field > 10.6 => field >= 11
        f.store("10.6", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharGE() {
        PartitionField f = new BigIntPartitionField(new LongType());
        boolean[] endPoints = {true, true}; //>=

        // field >= 10
        f.store("10", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field >= 10.3 => field > 10
        f.store("10.3", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 10);
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = true;

        // field >= 10.6 => field >= 11
        f.store("10.6", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testBigIntPartitionFieldHashCode() {

        PartitionField f1 = new BigIntPartitionField(new LongType());
        PartitionField[] fields = new PartitionField[1];
        ExecutionContext ec = new ExecutionContext();
        ec.setSqlMode("ANSI");
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);
        f1.store("184467440737095516150", new VarcharType(), sessionProperties);
        fields[0] = f1;
        long v1 = f1.longValue();
        long f1HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f2 = new BigIntPartitionField(new LongType());
        f2.store("-184467440737095516150", new VarcharType(), sessionProperties);
        fields[0] = f2;
        long v2 = f2.longValue();
        long f2HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f3 = new BigIntPartitionField(new LongType());
        f3.store("9223372036854775807", new VarcharType(), sessionProperties);
        fields[0] = f3;
        long v3 = f3.longValue();
        long f3HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f4 = new BigIntPartitionField(new LongType());
        f4.store("-9223372036854775808", new VarcharType(), sessionProperties);
        fields[0] = f4;
        long v4 = f4.longValue();
        long f4HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        System.out.print(f1HashVal);
        System.out.print(f2HashVal);
        System.out.print(f3HashVal);
        System.out.print(f4HashVal);

        org.junit.Assert.assertTrue(v1==v3);
        org.junit.Assert.assertTrue(v2==v4);

        org.junit.Assert.assertTrue(f1HashVal==f3HashVal);
        org.junit.Assert.assertTrue(f2HashVal==f4HashVal);
    }

    @Test
    public void testBigIntPartitionFieldHashCode2() {
        
        PartitionField f1 = new BigIntPartitionField(new LongType());
        PartitionField[] fields = new PartitionField[1];
        ExecutionContext ec = new ExecutionContext();
        ec.setSqlMode("ANSI");
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);
        f1.store(new BigInteger("184467440737095516150"), new BigIntegerType(), sessionProperties);
        fields[0] = f1;
        long v1 = f1.longValue();
        long f1HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f2 = new BigIntPartitionField(new LongType());
        f2.store(new BigInteger("-184467440737095516150"), new BigIntegerType(), sessionProperties);
        fields[0] = f2;
        long v2 = f2.longValue();
        long f2HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f3 = new BigIntPartitionField(new LongType());
        f3.store(new BigInteger("9223372036854775807"), new BigIntegerType(), sessionProperties);
        fields[0] = f3;
        long v3 = f3.longValue();
        long f3HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        PartitionField f4 = new BigIntPartitionField(new LongType());
        f4.store(new BigInteger("-9223372036854775808"), new BigIntegerType(), sessionProperties);
        fields[0] = f4;
        long v4 = f4.longValue();
        long f4HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);

        System.out.print(f1HashVal);
        System.out.print(f2HashVal);
        System.out.print(f3HashVal);
        System.out.print(f4HashVal);

        org.junit.Assert.assertTrue(v1==v3);
        org.junit.Assert.assertTrue(v2==v4);

        org.junit.Assert.assertTrue(f1HashVal==f3HashVal);
        org.junit.Assert.assertTrue(f2HashVal==f4HashVal);
    }
}
