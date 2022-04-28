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

import com.alibaba.polardbx.common.datatype.Decimal;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DecimalBlockTest extends BaseBlockTest {

    @Test
    public void testNullValues() {
        final Decimal[] values = new Decimal[] {
            Decimal.fromString("3.14"),
            Decimal.ZERO,
            Decimal.fromLong(1L),
            Decimal.fromBigDecimal(new BigDecimal("-4.2")),
            Decimal.ZERO,
            null,
            Decimal.fromLong(10L),
            Decimal.fromString("99999999999999999999999999999999999999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999999999999999999999999999999999"),
            Decimal.fromString("99999999999999999999999999999999999.999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999.999999999999999999999999999999"),
            Decimal.fromString("-123456"),
            Decimal.fromString("-123.456"),
        };

        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(CHUNK_SIZE);
        for (Decimal value : values) {
            if (value != null) {
                blockBuilder.writeDecimal(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertEquals(values[i], blockBuilder.getDecimal(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertEquals(values[i], block.getDecimal(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new DecimalBlockBuilder(CHUNK_SIZE);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertEquals(values[i], anotherBuilder.getDecimal(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }

        Slice slice = Slices.allocate(10000);
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();

        encoding.writeBlock(slice.getOutput(), block);
        Block deserializedBlock = encoding.readBlock(slice.getInput());

        assertEquals(values.length, deserializedBlock.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertEquals(values[i], block.getDecimal(i));
            } else {
                assertTrue(block.isNull(i));
            }
        }
    }

    @Test
    public void testEncodingDeicmal() {
        final Decimal[] values = new Decimal[] {
            Decimal.fromString("99999999999999999999999999999999999999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999999999999999999999999999999999"),
            Decimal.fromString("99999999999999999999999999999999999.999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999.999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999.999999999999999999999999999999"),
            Decimal.fromString("-99999999999999999999999999999999999.999999999999999999999999999999")
        };

        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(5);
        for (Decimal value : values) {
            if (value != null) {
                blockBuilder.writeDecimal(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        Slice slice = Slices.allocate(10000);
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();

        encoding.writeBlock(slice.getOutput(), blockBuilder.build());
        Block deserializedBlock = encoding.readBlock(slice.getInput());

        assertEquals(values.length, deserializedBlock.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertEquals(values[i], deserializedBlock.getDecimal(i));
            } else {
                assertTrue(deserializedBlock.isNull(i));
            }
        }
    }
}
