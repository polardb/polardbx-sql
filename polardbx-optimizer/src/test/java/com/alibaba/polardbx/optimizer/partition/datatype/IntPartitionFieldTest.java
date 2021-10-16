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

import com.google.common.primitives.UnsignedLongs;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.stream.IntStream;

public class IntPartitionFieldTest {
    private static final char[] DIGITS = "1234567890".toCharArray();
    private static final Random R = new Random();

    @Test
    public void testUnsigned() {
        PartitionField f1 = new IntPartitionField(new UIntegerType());
        PartitionField f2 = new IntPartitionField(new UIntegerType());
        DataType resultType = new IntegerType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f1.reset();
                f2.reset();

                int i1 = Math.abs(R.nextInt());
                int i2 = Math.abs(R.nextInt());

                f1.store(i1, resultType);
                f2.store(i2, resultType);

                Assert.assertTrue((i1 < 0 && f1.longValue() == 0) || i1 == f1.longValue(), i1 + ", " + f1.longValue());
                Assert.assertTrue((i2 < 0 && f2.longValue() == 0) || i2 == f2.longValue(), i2 + ", " + f2.longValue());

                boolean cmp1 = UnsignedLongs.compare(i1, i2) > 0;
                boolean cmp2 = f1.compareTo(f2) > 0;

                Assert.assertTrue(cmp1 == cmp2);
            }
        );
    }

    @Test
    public void testSigned() {
        PartitionField f1 = new IntPartitionField(new IntegerType());
        PartitionField f2 = new IntPartitionField(new IntegerType());
        DataType resultType = new IntegerType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f1.reset();
                f2.reset();

                int i1 = R.nextInt();
                int i2 = R.nextInt();

                f1.store(i1, resultType);
                f2.store(i2, resultType);

                Assert.assertTrue(i1 == f1.longValue(), i1 + ", " + f1.longValue());
                Assert.assertTrue(i2 == f2.longValue(), i2 + ", " + f2.longValue());

                boolean cmp1 = i1 > i2;
                boolean cmp2 = f1.compareTo(f2) > 0;

                Assert.assertTrue(cmp1 == cmp2,
                    "l1 = " + i1 + ", l2 = " + i2 + ", f1 = " + f1.longValue() + ", f2 = " + f2.longValue());
            }
        );
    }

    @Test
    public void testUnsignedString() {
        PartitionField f = new IntPartitionField(new IntegerType());
        DataType resultType = new VarcharType();
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                f.reset();

                // simple long numeric test
                String numericStr = getDigits(R.nextInt(7) + 1);
                f.store(numericStr, resultType);

                Assert.assertTrue(f.longValue() == Long.valueOf(numericStr), "error numeric: " + numericStr);
            }
        );
    }

    @Test
    public void testDecimalHalfUp() {
        PartitionField f = new IntPartitionField(new IntegerType());
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
        PartitionField f = new IntPartitionField(new IntegerType());
        DataType resultType = new DecimalType();

        String decStr1 = "2147483647";
        String decStr2 = "-2147483648";

        f.store(Decimal.fromString(decStr1), resultType);
        Assert.assertTrue(Integer.toString((int) f.longValue()).equals(decStr1));

        f.reset();

        f.store(Decimal.fromString(decStr2), resultType);
        Assert.assertTrue(Integer.toString((int) f.longValue()).equals(decStr2));
    }

    @Test
    public void testDecimalUnsigned() {
        PartitionField f = new BigIntPartitionField(new UIntegerType());
        DataType resultType = new DecimalType();

        String decStr1 = "4294967295";
        String decStr2 = "0";

        f.store(Decimal.fromString(decStr1), resultType);
        Assert.assertTrue(Integer.toUnsignedString((int) f.longValue()).equals(decStr1));

        f.reset();

        f.store(Decimal.fromString(decStr2), resultType);
        Assert.assertTrue(Integer.toUnsignedString((int) f.longValue()).equals(decStr2));
    }

    @Test
    public void testBigDecimalSigned() {
        PartitionField f = new IntPartitionField(new IntegerType());
        DataType resultType = new DecimalType();

        String decStr1 = "2147483647";
        String decStr2 = "-2147483648";

        f.store(new BigDecimal(decStr1), resultType);
        Assert.assertTrue(Integer.toString((int) f.longValue()).equals(decStr1));

        f.reset();

        f.store(new BigDecimal(decStr2), resultType);
        Assert.assertTrue(Integer.toString((int) f.longValue()).equals(decStr2));
    }

    @Test
    public void testBigDecimalUnsigned() {
        PartitionField f = new IntPartitionField(new UIntegerType());
        DataType resultType = new DecimalType();

        String decStr1 = "4294967295";
        String decStr2 = "0";

        f.store(new BigDecimal(decStr1), resultType);
        Assert.assertTrue(Integer.toUnsignedString((int) f.longValue()).equals(decStr1));

        f.reset();

        f.store(new BigDecimal(decStr2), resultType);
        Assert.assertTrue(Integer.toUnsignedString((int) f.longValue()).equals(decStr2));
    }

    @Test
    public void testSetNull() {
        PartitionField f1 = new IntPartitionField(new UIntegerType());
        f1.setNull();
        Assert.assertTrue(f1.isNull());

        PartitionField f2 = new IntPartitionField(new IntegerType());
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

    private static final int MAX_PRECISION = 9;

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
        PartitionField f = new IntPartitionField(new IntegerType());
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
        PartitionField f = new IntPartitionField(new IntegerType());
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
        PartitionField f = new IntPartitionField(new UIntegerType());
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
        PartitionField f = new IntPartitionField(new UIntegerType());
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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {false, false};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {false, true};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {true, false};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {true, true};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {false, false};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {false, true};

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
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {true, false};

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

        // field >= 10.6 => field >= 11
        f.store("10.6", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.longValue() == 11);
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharGE() {
        PartitionField f = new IntPartitionField(new IntegerType());
        boolean[] endPoints = {true, true};

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

}
