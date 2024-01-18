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
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static com.alibaba.polardbx.executor.chunk.DecimalBlock.DecimalBlockState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DecimalBlockTest extends BaseBlockTest {

    @Test
    public void testSizeInBytes() {
        DecimalBlock block = new DecimalBlock(new DecimalType(), 1024);
        Assert.assertEquals(41984, block.getElementUsedBytes());
    }

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

    @Test
    public void testIsSimple() {
        // Simple if all decimal values are not negative and have the same small frac-pos and int-pos
        DecimalBlockBuilder builder = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        builder.writeDecimal(from("1.22"));
        builder.writeDecimal(from("111.353898989"));
        builder.writeDecimal(from("1111.04"));
        builder.writeDecimal(from("12345678.999999999"));
        DecimalBlock block = (DecimalBlock) builder.build();

        assertTrue(block.isSimple());

        DecimalBlockBuilder builder1 = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        builder1.writeDecimal(from("1.22"));
        builder1.writeDecimal(from("-111.353898989"));
        builder1.writeDecimal(from("1111.04"));
        builder1.writeDecimal(from("-12345678.999999999"));
        DecimalBlock block1 = (DecimalBlock) builder1.build();

        assertTrue(!block1.isSimple());

        DecimalBlockBuilder builder2 = new DecimalBlockBuilder(10, new DecimalType(15, 2));
        builder2.writeDecimal(from("1.22"));
        builder2.writeDecimal(from("111.33"));
        builder2.writeDecimal(from("1111.00000000000004"));
        builder2.writeDecimal(from("12345678.9999999999"));
        DecimalBlock block2 = (DecimalBlock) builder2.build();

        assertTrue(!block2.isSimple());
    }

    @Test
    public void testIsSimpleInShift() {
        DecimalBlock.DecimalBlockState state;

        state = DecimalBlock.DecimalBlockState.stateOf(from(4, 2).getDecimalStructure());
        Assert.assertTrue(state == SIMPLE_MODE_1); // 40000000 * 10^-9

        state = DecimalBlock.DecimalBlockState.stateOf(from("0.04").getDecimalStructure());
        Assert.assertTrue(state == SIMPLE_MODE_2); // 0 + 40000000 * 10^-9

        state = DecimalBlock.DecimalBlockState.stateOf(from(144, 2).getDecimalStructure());
        Assert.assertTrue(state == SIMPLE_MODE_2); // 1 + 440000000 * 10^-9

        state = DecimalBlock.DecimalBlockState.stateOf(from("1.44").getDecimalStructure());
        Assert.assertTrue(state == SIMPLE_MODE_2); // 1 + 440000000 * 10^-9

        DecimalBlockBuilder builder;
        DecimalBlock block;

        builder = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        // simple_mode_2
        builder.writeDecimal(from(1122L, 3));
        builder.writeDecimal(from(111353898989L, 9));

        // simple_mode_1
        builder.writeDecimal(from(4, 2));
        builder.writeDecimal(from(16, 5));
        block = (DecimalBlock) builder.build();

        assertTrue(!block.isSimple());

        builder = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        builder.writeDecimal(from(122L, 3));
        builder.writeDecimal(from(111353898989L, 9));
        builder.writeDecimal(from(4, 2));

        // not_simple
        builder.writeDecimal(from(-16, 5));
        block = (DecimalBlock) builder.build();

        assertTrue(!block.isSimple());

        builder = new DecimalBlockBuilder(10, new DecimalType(15, 2));
        builder.writeDecimal(from(122, 2));
        builder.writeDecimal(from(11133, 2));

        // not_simple
        builder.writeDecimal(from(111100000000000004L, 14));
        builder.writeDecimal(from(123456789999999999L, 10));
        block = (DecimalBlock) builder.build();

        assertTrue(!block.isSimple());
    }

    private Decimal from(String decStr) {
        return Decimal.fromString(decStr);
    }

    private Decimal from(long unscaled, int scale) {
        final com.alibaba.polardbx.rpc.result.chunk.Decimal decimal =
            new com.alibaba.polardbx.rpc.result.chunk.Decimal(unscaled, scale);
        return new Decimal(decimal.getUnscaled(), decimal.getScale());
    }
}
