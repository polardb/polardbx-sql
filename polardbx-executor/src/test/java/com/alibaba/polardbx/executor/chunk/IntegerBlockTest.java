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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryUsageReport;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntegerBlockTest extends BaseBlockTest {
    @Test
    public void testSizeInBytes() {
        IntegerBlock block = new IntegerBlock(new IntegerType(), 1024);
        MemoryCountable.checkDeviation(block, .05d, true);
        Assert.assertEquals(5216, block.getElementUsedBytes());
    }

    @Test
    public void test() {
        final Integer[] values = new Integer[] {
            -123, 0, 123, null, Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        IntegerBlockBuilder blockBuilder = new IntegerBlockBuilder(CHUNK_SIZE);
        for (Integer value : values) {
            if (value != null) {
                blockBuilder.writeInt(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals((int) values[i], blockBuilder.getInt(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }
        MemoryCountable.checkDeviation(blockBuilder, .05d, true);
        Block block = blockBuilder.build();
        MemoryUsageReport report = FastMemoryCounter.parseInstance(block, 16, true, true, true);
        System.out.println("memory usage = " + report.getTotalSize());
        System.out.println("memory verbose = " + report.getFieldSizeMap());
        System.out.println("memory usage tree = \n" + report.getMemoryUsageTree());

        MemoryCountable.checkDeviation(block, .05d, true);

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals((int) values[i], block.getInt(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new IntegerBlockBuilder(CHUNK_SIZE);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals((int) values[i], anotherBuilder.getInt(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }
        MemoryCountable.checkDeviation(anotherBuilder, .05d, true);
        MemoryCountable.checkDeviation(anotherBuilder.build(), .05d, true);
    }
}
