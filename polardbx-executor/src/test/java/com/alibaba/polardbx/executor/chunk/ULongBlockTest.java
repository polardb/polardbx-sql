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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ULongBlockTest extends BaseBlockTest {
    @Test
    public void testSizeInBytes() {
        ULongBlock block = new ULongBlock(new ULongType(), 1024);
        Assert.assertEquals(9216, block.getElementUsedBytes());
    }

    @Test
    public void testNullValues() {
        final BigInteger[] values = new BigInteger[] {
            new BigInteger("314"),
            BigInteger.ZERO,
            BigInteger.ONE,
            BigInteger.TEN,
            null,
            new BigInteger("9999999"),
            new BigInteger("1567"),
        };

        BlockBuilder blockBuilder = new ULongBlockBuilder(CHUNK_SIZE);
        for (BigInteger value : values) {
            if (value != null) {
                blockBuilder.writeBigInteger(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals(values[i], blockBuilder.getBigInteger(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals(values[i], block.getBigInteger(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new ULongBlockBuilder(CHUNK_SIZE);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals(values[i], anotherBuilder.getBigInteger(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }
    }

    @Test
    public void testEncoding() {
        final int size = 5;
        ULongBlockBuilder blockBuilder = new ULongBlockBuilder(size);
        blockBuilder.writeUInt64(UInt64.MAX_UINT64);
        blockBuilder.writeObject(null);
        blockBuilder.writeUInt64(UInt64.UINT64_ZERO);
        blockBuilder.writeUInt64(UInt64.fromLong(-2L));
        blockBuilder.writeObject(null);

        ULongBlock block = (ULongBlock) blockBuilder.build();
        ULongBlockEncoding encoding = new ULongBlockEncoding();
        SliceOutput output = new DynamicSliceOutput(size);

        encoding.writeBlock(output, block);

        SliceInput input = output.slice().getInput();
        ULongBlock block1 = (ULongBlock) encoding.readBlock(input);

        for (int i = 0; i < size; i++) {
            Assert.assertEquals(block.getObject(i), block1.getObject(i));
        }
    }

    @Test
    public void test() {
        int count = 100;
        ULongBlockBuilder builder1 = new ULongBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            builder1.writeLong(i);
        }
        builder1.appendNull();
        ULongBlock ulongBlock = (ULongBlock) builder1.build();
        ULongBlockBuilder builder2 = new ULongBlockBuilder(1);
        for (int i = 0; i < count; i++) {
            ulongBlock.writePositionTo(i, builder2);
        }
        ULongBlock newBlock = (ULongBlock) builder2.build();
        Assert.assertSame(DataTypes.ULongType, newBlock.getType());
        int[] hashes = newBlock.hashCodeVector();
        int[] hashes2 = new int[hashes.length];
        newBlock.hashCodeVector(hashes2, hashes2.length);
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(ulongBlock.getObject(i), newBlock.getObject(i));
            Assert.assertEquals(ulongBlock.getElementAtUnchecked(i), newBlock.getElementAtUnchecked(i));
            Assert.assertEquals(ulongBlock.getLong(i), newBlock.getLong(i));
            Assert.assertEquals(ulongBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals(ulongBlock.hashCode(i), hashes[i]);
            Assert.assertEquals(ulongBlock.hashCode(i), hashes2[i]);
            Assert.assertEquals(ulongBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
        }

        ULongBlock newBlock2 = new ULongBlock(DataTypes.ULongType, count);
        newBlock.shallowCopyTo(newBlock2);
        Assert.assertSame(newBlock.longArray(), newBlock2.longArray());

        int[] sel = new int[] {0, 1, 2, 3, 4, 11, 12, 13, 14};
        ULongBlock newBlock3 = new ULongBlock(DataTypes.ULongType, count);
        newBlock.copySelected(false, null, count, newBlock3);
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(newBlock3.getObject(i), newBlock.getObject(i));
        }
        newBlock.copySelected(true, sel, sel.length, newBlock3);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            Assert.assertEquals(newBlock3.getObject(j), newBlock.getObject(j));
        }
        LongBlock longBlock = new LongBlock(DataTypes.LongType, count);
        newBlock.copySelected(false, null, count, longBlock);
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(longBlock.getLong(i), newBlock.getLong(i));
        }
        // compact should work
        newBlock.compact(sel);
        Assert.assertEquals(sel.length, newBlock.getPositionCount());
    }
}