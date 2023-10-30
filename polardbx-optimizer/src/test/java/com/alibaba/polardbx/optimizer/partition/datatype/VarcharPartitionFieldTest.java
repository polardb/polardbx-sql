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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.avatica.util.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

public class VarcharPartitionFieldTest {
    @Test
    public void testPadding() {
        final int precision = 255;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

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
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is ", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate1() {
        final int precision = 1;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 18L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("1", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate2() {
        final int precision = 7;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 157893338L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("1578933", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncate3() {
        final int precision = 10;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = -5892578293540489325L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("-589257829", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testTruncateUTF8() {
        final int precision = 20;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Slice value = Slices.utf8Slice("08211用户编号01066");
        TypeConversionStatus conversionStatus = f.store(value, new CharType(CollationName.UTF8_GENERAL_CI));

        String res = f.stringValue().toStringUtf8();

        Assert.assertTrue(conversionStatus == TypeConversionStatus.TYPE_OK);
        Assert.assertEquals("08211用户编号01066", res);
    }

    @Test
    public void testNotTruncate() {
        final int precision = 5;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        Long x = 1888L;
        f.store(x, DataTypes.LongType);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("1888", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testLatin1() {
        final int precision = 80;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

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
    public void testGBK() {
        final int precision = 80;
        DataType type = new VarcharType(CollationName.GBK_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        ExecutionContext context = new ExecutionContext();
        context.setEncoding("GBK");

        ByteString bs = new ByteString(new byte[] {(byte) 0xCA, (byte) 0xC0, (byte) 0xBD, (byte) 0xE7});
        f.store(bs, type, SessionProperties.fromExecutionContext(context));

        String res = f.stringValue().toStringUtf8();

        Assert.assertTrue(Arrays.equals(bs.getBytes(), res.getBytes(Charset.forName("GBK"))));

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGBKPadding() {
        final int precision = 80;
        DataType type = new VarcharType(CollationName.GBK_CHINESE_CI, precision);

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
        DataType type = new VarcharType(CollationName.GBK_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is ", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGB18030() {
        final int precision = 80;
        DataType type = new VarcharType(CollationName.GB18030_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        ExecutionContext context = new ExecutionContext();
        context.setEncoding("GB18030");

        ByteString bs = new ByteString(new byte[] {(byte) 0xCA, (byte) 0xC0, (byte) 0xBD, (byte) 0xE7});
        f.store(bs, type, SessionProperties.fromExecutionContext(context));

        String res = f.stringValue().toStringUtf8();

        Assert.assertTrue(Arrays.equals(bs.getBytes(), res.getBytes(Charset.forName("GB18030"))));

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testGB18030Padding() {
        final int precision = 80;
        DataType type = new VarcharType(CollationName.GB18030_CHINESE_CI, precision);

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
        DataType type = new VarcharType(CollationName.GB18030_CHINESE_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        String s = "this is test string 中文字符串";
        f.store(s, type);

        String res = f.stringValue().toStringUtf8();

        Assert.assertEquals("this is ", res);

        f.hash(new long[] {1L, 4L});
    }

    @Test
    public void testSetNull() {
        final int precision = 255;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

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
                f.reset();
                f.store(l, new LongType());
                long x = f.longValue();
                Assert.assertEquals(l, x);
            }
        );
    }

    @Test
    public void testUnsignedLongToChar() {
        final long maxNumber = 10000000000L; // 10^10
        final long minNumber = 1L - 10000000000L; // -10^10+1
        final int precision = 10;
        DataType type = new VarcharType(CollationName.UTF8MB4_GENERAL_CI, precision);

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
