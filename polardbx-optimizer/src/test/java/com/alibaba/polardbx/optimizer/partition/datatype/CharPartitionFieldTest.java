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
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
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
    public void testSetNull() {
        final int precision = 255;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);
        f.setNull();
        com.alibaba.polardbx.common.utils.Assert.assertTrue(f.isNull());
    }

    @Test
    public void testLongToChar() {
        final int precision = 10;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        new Random().longs(1 << 10).forEach(
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
        final int precision = 10;
        DataType type = new CharType(CollationName.UTF8MB4_GENERAL_CI, precision);

        PartitionField f = PartitionFieldBuilder.createField(type);

        new Random().longs(1 << 10).forEach(
            l -> {
                f.reset();
                f.store(l, new ULongType());
                long x = f.longValue();
                Assert.assertEquals(l, x);
            }
        );
    }
}
