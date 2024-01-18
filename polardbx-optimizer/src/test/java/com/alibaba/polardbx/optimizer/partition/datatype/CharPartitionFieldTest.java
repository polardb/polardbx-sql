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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class CharPartitionFieldTest {
    @Test
    public void testPadding() {
        final int precision = 255;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals(s, res);

        long[] numbers = {1L, 4L};
        f.hash(numbers);

        Assert.assertEquals(Long.toUnsignedString(numbers[0]), "9866807973401057369");
    }

    @Test
    public void testTruncate() {
        final int precision = 8;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate1() {
        final int precision = 1;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 13L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("1", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate2() {
        final int precision = 4;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 13579L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("1357", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate3() {
        final int precision = 10;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = -4625795218133491210L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("-462579521", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncateUTF8() {
        final int precision = 20;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Slice value = Slices.utf8Slice("08211用户编号01066");
        TypeConversionStatus conversionStatus = f.store(value, new CharType(CollationName.UTF8_GENERAL_CI));

        String res = f.stringValue().toStringUtf8();

        Assert.assertTrue(conversionStatus == TypeConversionStatus.TYPE_OK);
        Assert.assertEquals("08211用户编号01066", res);
    }

    @Test
    public void testNotTruncate() {
        final int precision = 7;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 54758L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("54758", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testLatin1() {
        final int precision = 80;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals(s, res);

        long[] numbers = {1L, 4L};
        f.hash(numbers);

        Assert.assertEquals(Long.toUnsignedString(numbers[0]), "9866807973401057369");
    }

    @Test
    public void testGBKPadding() {
        final int precision = 80;
        DataType type = new CharType(CollationName.GBK_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals(s, res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGBKTruncate() {
        final int precision = 8;
        DataType type = new CharType(CollationName.GBK_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGB18030Padding() {
        final int precision = 80;
        DataType type = new CharType(CollationName.GB18030_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals(s, res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGB18030Truncate() {
        final int precision = 8;
        DataType type = new CharType(CollationName.GB18030_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testSetNull() {
        final int precision = 255;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);
        f.setNull();
        com.alibaba.polardbx.common.utils.Assert.assertTrue(f.isNull());
    }

    @Test
    public void testLongToChar() {
        final long maxNumber = 10000000000L; // 10^10
        final long minNumber = 1L - 10000000000L; // -10^10+1
        final int precision = 10;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        new Random().longs(1 << 10).filter(l -> l > minNumber && l < maxNumber).forEach(
            l -> {
                // 1. test input and output equality
                f.reset();
                f.store(l, new LongType());
                long x = f.longValue();
                Assert.assertEquals(l, x);

                // 2. test hash results equality
                long[] hash1 = new long[] {1L, 4L};
                f.hash(hash1);
                long[] hash2 = new long[] {1L, 4L};
                f.reset();
                f.store(Slices.utf8Slice(String.valueOf(l)), new VarcharType());
                f.hash(hash2);
                Assert.assertEquals("l = " + l, hash1[0], hash2[0]);
                Assert.assertEquals("l = " + l, hash1[1], hash2[1]);
            }
        );
    }

    @Test
    public void testUnsignedLongToChar() {
        final long maxNumber = 10000000000L; // 10^10
        final long minNumber = 1L - 10000000000L; // -10^10+1
        final int precision = 10;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        new Random().longs(1 << 10).filter(l -> l > minNumber && l < maxNumber).forEach(
            l -> {
                f.reset();
                f.store(l, new ULongType());
                long x = f.longValue();
                Assert.assertEquals(l, x);
            }
        );
    }
}
