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

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongBlockTest extends BaseBlockTest {
    @Test
    public void testSizeInBytes() {
        LongBlock block = new LongBlock(new LongType(), 1024);
        MemoryCountable.checkDeviation(block, .05d, true);
        Assert.assertEquals(9312, block.getElementUsedBytes());
    }

    @Test
    public void test() {
        final Long[] values = new Long[] {
            -123L, 0L, 123L, null, Long.MAX_VALUE, Long.MIN_VALUE
        };

        LongBlockBuilder blockBuilder = new LongBlockBuilder(CHUNK_SIZE);
        for (Long value : values) {
            if (value != null) {
                blockBuilder.writeLong(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals((long) values[i], blockBuilder.getLong(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        MemoryCountable.checkDeviation(blockBuilder, .05d, true);
        Block block = blockBuilder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals((long) values[i], block.getLong(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new LongBlockBuilder(CHUNK_SIZE);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals((long) values[i], anotherBuilder.getLong(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }
        MemoryCountable.checkDeviation(anotherBuilder, .05d, true);
        MemoryCountable.checkDeviation(anotherBuilder.build(), .05d, true);
    }

    @Test
    public void testNotSupportedOperation() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 3L, 4L);
        expectUnsupportedException(() -> longBlock.getByte(0));
        expectUnsupportedException(() -> longBlock.getShort(0));
        expectUnsupportedException(() -> longBlock.getDouble(0));
        expectUnsupportedException(() -> longBlock.getFloat(0));
        expectUnsupportedException(() -> longBlock.getTimestamp(0));
        expectUnsupportedException(() -> longBlock.getDate(0));
        expectUnsupportedException(() -> longBlock.getTime(0));
        expectUnsupportedException(() -> longBlock.getString(0));
        expectUnsupportedException(() -> longBlock.getDecimal(0));
        expectUnsupportedException(() -> longBlock.getBigInteger(0));
        expectUnsupportedException(() -> longBlock.getBoolean(0));
        expectUnsupportedException(() -> longBlock.getByteArray(0));
        expectUnsupportedException(() -> longBlock.getBlob(0));
        expectUnsupportedException(() -> longBlock.getClob(0));
        MemoryCountable.checkDeviation(longBlock, .05d, true);
    }

    @Test
    public void testBuilderNotSupportedOperation() {
        LongBlockBuilder longBlockBuilder = new LongBlockBuilder(4);
        expectUnsupportedException(() -> longBlockBuilder.writeByte((byte) 0));
        expectUnsupportedException(() -> longBlockBuilder.writeShort((short) 0));
        expectUnsupportedException(() -> longBlockBuilder.writeDouble(0D));
        expectUnsupportedException(() -> longBlockBuilder.writeFloat(0F));
        expectUnsupportedException(() -> longBlockBuilder.writeTimestamp(null));
        expectUnsupportedException(() -> longBlockBuilder.writeDate(null));
        expectUnsupportedException(() -> longBlockBuilder.writeTime(null));
        expectUnsupportedException(() -> longBlockBuilder.writeString(null));
        expectUnsupportedException(() -> longBlockBuilder.writeDecimal(null));
        expectUnsupportedException(() -> longBlockBuilder.writeBigInteger(null));
        expectUnsupportedException(() -> longBlockBuilder.writeBoolean(true));
        expectUnsupportedException(() -> longBlockBuilder.writeByteArray(null));
        expectUnsupportedException(() -> longBlockBuilder.writeBlob(null));
        expectUnsupportedException(() -> longBlockBuilder.writeClob(null));
        MemoryCountable.checkDeviation(longBlockBuilder.build(), .05d, true);
    }

    @Test
    public void illegalCastTest() {
        LongBlock longBlock = new LongBlock(new LongType(), 1024);
        try {
            Block res = longBlock.cast(ShortBlock.class);
            MemoryCountable.checkDeviation(res, .05d, true);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("failed to cast"));
        }
    }

    private void expectUnsupportedException(FunctionInterface notSupportedFunc) {
        try {
            notSupportedFunc.execute();
            Assert.fail("Expect not supported");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertTrue(e.getMessage().contains(LongBlock.class.getName()));
        }
    }

    @FunctionalInterface
    interface FunctionInterface {
        void execute();
    }
}
