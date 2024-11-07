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

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.ShortType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class ShortBlockTest extends BaseBlockTest {
    @Test
    public void testSizeInBytes() {
        ShortBlock shortBlock = new ShortBlock(new ShortType(), 1024);
        Assert.assertEquals(3072, shortBlock.getElementUsedBytes());
    }

    @Test
    public void test() {
        final Short[] values = new Short[] {
            -123, 0, 123, null, Short.MAX_VALUE, Short.MIN_VALUE
        };

        ShortBlockBuilder blockBuilder = new ShortBlockBuilder(CHUNK_SIZE);
        for (Short value : values) {
            if (value != null) {
                blockBuilder.writeShort(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals((int) values[i], blockBuilder.getShort(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals((int) values[i], block.getShort(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new ShortBlockBuilder(CHUNK_SIZE);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals((int) values[i], anotherBuilder.getShort(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }

        ShortBlock shortBlock = (ShortBlock) block;
        ShortBlock newBlock1 = new ShortBlock(DataTypes.ShortType, block.getPositionCount());
        shortBlock.shallowCopyTo(newBlock1);
        int[] hashCodes1 = block.hashCodeVector();
        int[] hashCodes2 = new int[hashCodes1.length];
        block.hashCodeVector(hashCodes2, hashCodes2.length);
        for (int i = 0; i < newBlock1.positionCount; i++) {
            assertEquals(block.hashCodeUseXxhash(i), newBlock1.hashCodeUseXxhash(i));
            assertEquals(block.getObject(i), newBlock1.getObject(i));
            assertEquals(hashCodes1[i], newBlock1.hashCode(i));
            assertEquals(hashCodes2[i], newBlock1.hashCode(i));
        }

        int[] sel = new int[] {1, 2, 3};
        ShortBlock newBlock2 = new ShortBlock(DataTypes.ShortType, block.getPositionCount());
        shortBlock.copySelected(false, null, shortBlock.positionCount, newBlock2);
        for (int i = 0; i < newBlock1.positionCount; i++) {
            assertEquals(shortBlock.getObject(i), newBlock2.getObject(i));
        }
        ShortBlock newBlock3 = new ShortBlock(DataTypes.ShortType, block.getPositionCount());
        shortBlock.copySelected(true, sel, sel.length, newBlock3);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            assertEquals(shortBlock.getObject(j), newBlock3.getObject(j));
        }
        IntegerBlock newBlock4 = new IntegerBlock(DataTypes.IntegerType, block.getPositionCount());
        shortBlock.copySelected(true, sel, sel.length, newBlock4);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            assertEquals(shortBlock.getShort(j), newBlock4.getInt(j));
        }

        shortBlock.compact(null);
        // compact should work
        shortBlock.compact(sel);
        Assert.assertEquals(sel.length, shortBlock.positionCount);
    }
}
