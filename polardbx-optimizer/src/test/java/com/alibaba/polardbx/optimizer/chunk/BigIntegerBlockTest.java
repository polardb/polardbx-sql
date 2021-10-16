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

package com.alibaba.polardbx.optimizer.chunk;

import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BigIntegerBlockTest extends BaseBlockTest {

    @Test
    public void testNullValues() {
        final BigInteger[] values = new BigInteger[] {
            new BigInteger("314"),
            BigInteger.ZERO,
            BigInteger.ONE,
            BigInteger.TEN,
            null,
            new BigInteger("99999999999999999999999999999999999999999999999999999999999999999"),
            new BigInteger("-99999999999999999999999999999999999999999999999999999999999999999"),
        };

        BigIntegerBlockBuilder blockBuilder = new BigIntegerBlockBuilder(CHUNK_SIZE);
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

        BlockBuilder anotherBuilder = new BigIntegerBlockBuilder(CHUNK_SIZE);

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
}
