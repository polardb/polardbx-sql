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

import com.alibaba.druid.mock.MockBlob;
import com.alibaba.druid.mock.MockClob;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import org.junit.Assert;
import com.alibaba.polardbx.executor.chunk.BlobBlockBuilder;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.ClobBlockBuilder;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlockBuilder;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReferenceBlockTest {
    @Test
    public void testSizeInBytes() {
        ReferenceBlock<Long> block = new ReferenceBlock<>(new LongType(), 1024);
        Assert.assertEquals(5208, block.getElementUsedBytes());
    }

    @Test
    public void test() {
        final Object[] values = new Object[] {
            "Hello", 1234L, new BigDecimal("3.14"), null
        };

        ObjectBlockBuilder blockBuilder = new ObjectBlockBuilder(10);
        for (Object value : values) {
            blockBuilder.writeObject(value);
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals(values[i], blockBuilder.getReference(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals(values[i], block.getObject(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new ObjectBlockBuilder(10);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals(values[i], anotherBuilder.getObject(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }
    }

    /**
     * Following classes are only for test purpose
     */
    static class ObjectBlock extends ReferenceBlock<Object> {

        ObjectBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values) {
            super(arrayOffset, positionCount, valueIsNull, values, null);
        }

        @Override
        public Object getObject(int position) {
            return isNull(position) ? null : getReference(position);
        }
    }

    static class ObjectBlockBuilder extends ReferenceBlockBuilder<Object> {

        ObjectBlockBuilder(int capacity) {
            super(capacity);
        }

        @Override
        public Object getObject(int position) {
            return getReference(position);
        }

        @Override
        public Block build() {
            return new ObjectBlock(0, getPositionCount(), valueIsNull.elements(), values.elements());
        }

        @Override
        public void writeObject(Object value) {
            if (value == null) {
                appendNull();
                return;
            }
            writeReference(value);
        }

        @Override
        public BlockBuilder newBlockBuilder() {
            return new ObjectBlockBuilder(initialCapacity);
        }
    }

    @Test
    public void testBlobBlock() {
        final Blob[] values = new Blob[] {
            new MockBlob(), null, new MockBlob(), null
        };

        BlobBlockBuilder blockBuilder = new BlobBlockBuilder(5);
        for (Blob value : values) {
            if (value != null) {
                blockBuilder.writeBlob(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals(values[i], block.getObject(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }
    }

    @Test
    public void testClobBlock() {
        final Clob[] values = new Clob[] {
            new MockClob(), null, new MockClob(), null
        };

        ClobBlockBuilder blockBuilder = new ClobBlockBuilder(5);
        for (Clob value : values) {
            if (value != null) {
                blockBuilder.writeClob(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals(values[i], block.getObject(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }
    }
}
