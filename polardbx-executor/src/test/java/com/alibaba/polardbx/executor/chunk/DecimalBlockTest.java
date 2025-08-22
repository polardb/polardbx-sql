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
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.SIMPLE_MODE_1;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.SIMPLE_MODE_2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DecimalBlockTest extends BaseBlockTest {

    private final Random random = new Random();

    @Test
    public void testSizeInBytes() {
        DecimalBlock block = new DecimalBlock(new DecimalType(), 1024);
        MemoryCountable.checkDeviation(block, .05d, true);
        Assert.assertEquals("delay memory allocation should contains the nulls array", 1240, block.getElementUsedBytes());

        block.setElementAt(0, Decimal.fromString("3.14"));
        MemoryCountable.checkDeviation(block, .05d, true);
        Assert.assertEquals("should allocate memory after setting an element", 42232, block.getElementUsedBytes());
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

        MemoryCountable.checkDeviation(blockBuilder, .05d, true);
        Block block = blockBuilder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

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
        MemoryCountable.checkDeviation(anotherBuilder, .05d, true);
        MemoryCountable.checkDeviation(anotherBuilder.build(), .05d, true);

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
        MemoryCountable.checkDeviation(deserializedBlock, .05d, true);
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
        MemoryCountable.checkDeviation(deserializedBlock, .05d, true);

        assertEquals(values.length, deserializedBlock.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertEquals(values[i], deserializedBlock.getDecimal(i));
            } else {
                assertTrue(deserializedBlock.isNull(i));
            }
        }
        MemoryCountable.checkDeviation(blockBuilder, .05d, true);
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
        MemoryCountable.checkDeviation(block, .05d, true);

        assertTrue(block.isSimple());

        DecimalBlockBuilder builder1 = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        builder1.writeDecimal(from("1.22"));
        builder1.writeDecimal(from("-111.353898989"));
        builder1.writeDecimal(from("1111.04"));
        builder1.writeDecimal(from("-12345678.999999999"));
        DecimalBlock block1 = (DecimalBlock) builder1.build();
        MemoryCountable.checkDeviation(block1, .05d, true);

        assertTrue(!block1.isSimple());

        DecimalBlockBuilder builder2 = new DecimalBlockBuilder(10, new DecimalType(15, 2));
        builder2.writeDecimal(from("1.22"));
        builder2.writeDecimal(from("111.33"));
        builder2.writeDecimal(from("1111.00000000000004"));
        builder2.writeDecimal(from("12345678.9999999999"));
        DecimalBlock block2 = (DecimalBlock) builder2.build();
        MemoryCountable.checkDeviation(block2, .05d, true);

        assertTrue(!block2.isSimple());
        MemoryCountable.checkDeviation(builder, .05d, true);
        MemoryCountable.checkDeviation(builder1, .05d, true);
        MemoryCountable.checkDeviation(builder2, .05d, true);
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
        MemoryCountable.checkDeviation(block, .05d, true);

        assertTrue(!block.isSimple());

        builder = new DecimalBlockBuilder(10, new DecimalType(65, 30));
        builder.writeDecimal(from(122L, 3));
        builder.writeDecimal(from(111353898989L, 9));
        builder.writeDecimal(from(4, 2));

        // not_simple
        builder.writeDecimal(from(-16, 5));
        block = (DecimalBlock) builder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

        assertTrue(!block.isSimple());

        builder = new DecimalBlockBuilder(10, new DecimalType(15, 2));
        builder.writeDecimal(from(122, 2));
        builder.writeDecimal(from(11133, 2));

        // not_simple
        builder.writeDecimal(from(111100000000000004L, 14));
        builder.writeDecimal(from(123456789999999999L, 10));
        block = (DecimalBlock) builder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

        assertTrue(!block.isSimple());
        MemoryCountable.checkDeviation(builder, .05d, true);
    }

    @Test
    public void testWriteDecimal64() {
        final int count = 1024;
        DecimalBlockBuilder builderWithDefaultScale = new DecimalBlockBuilder(count);
        Assert.assertTrue(builderWithDefaultScale.isUnset());
        DecimalType actualDecimalType = new DecimalType(16, 2);
        long[] decimal64Values = new long[count];
        Arrays.fill(decimal64Values, 12345);
        DecimalBlock decimal64Block = new DecimalBlock(actualDecimalType, count, false,
            null, decimal64Values);
        for (int i = 0; i < count; i++) {
            decimal64Block.writePositionTo(i, builderWithDefaultScale);
        }
        Assert.assertTrue(builderWithDefaultScale.isDecimal64());
        Assert.assertEquals(actualDecimalType.getScale(), builderWithDefaultScale.getDecimalType().getScale());
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(decimal64Values[i], builderWithDefaultScale.getLong(i));
        }
        MemoryCountable.checkDeviation(decimal64Block, .05d, true);
        MemoryCountable.checkDeviation(builderWithDefaultScale, .05d, true);
    }

    @Test
    public void testDecimal64BlockWriteToBuilder() {
        final int count = 1024;
        DecimalType actualDecimalType = new DecimalType(16, 2);

        DecimalBlockBuilder decimal64Builder = new DecimalBlockBuilder(count, actualDecimalType);

        long[] decimal64Values = new long[count];
        for (int i = 0; i < count; i++) {
            decimal64Values[i] = random.nextLong();
        }
        decimal64Values[0] = 0;

        for (int i = 0; i < count - 1; i++) {
            decimal64Builder.writeLong(decimal64Values[i]);
        }
        decimal64Builder.appendNull();

        DecimalBlock decimal64Block = (DecimalBlock) decimal64Builder.build();
        MemoryCountable.checkDeviation(decimal64Block, .05d, true);
        Assert.assertTrue(decimal64Block.isDecimal64());

        // writePositionTo decimal64Builder
        DecimalBlockBuilder toWriteDecimal64Builder = new DecimalBlockBuilder(count, actualDecimalType);
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            decimal64Block.writePositionTo(i, toWriteDecimal64Builder);
        }
        Assert.assertTrue(toWriteDecimal64Builder.isDecimal64());
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            if (decimal64Block.isNull(i)) {
                Assert.assertTrue(toWriteDecimal64Builder.isNull(i));
                continue;
            }
            Assert.assertEquals("WritePosition to decimal64Builder result differs",
                decimal64Block.getDecimal(i), toWriteDecimal64Builder.getDecimal(i));
        }

        // writePositionTo decimal128Builder
        DecimalBlockBuilder toWriteDecimal128Builder = new DecimalBlockBuilder(count, actualDecimalType);
        toWriteDecimal128Builder.writeDecimal128(0, 0);     // convert to a decimal128 builder
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            decimal64Block.writePositionTo(i, toWriteDecimal128Builder);
        }
        Assert.assertTrue(toWriteDecimal128Builder.isDecimal128());
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            if (decimal64Block.isNull(i)) {
                Assert.assertTrue(toWriteDecimal128Builder.isNull(i + 1));
                continue;
            }
            Assert.assertEquals("WritePosition to decimal128Builder result differs",
                decimal64Block.getDecimal(i), toWriteDecimal128Builder.getDecimal(i + 1));
        }

        // writePositionTo normal decimalBuilder
        DecimalBlockBuilder toWriteNormalDecimalBuilder = new DecimalBlockBuilder(count, actualDecimalType);
        toWriteNormalDecimalBuilder.writeDecimal(Decimal.ZERO);
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            decimal64Block.writePositionTo(i, toWriteNormalDecimalBuilder);
        }
        Assert.assertTrue(toWriteNormalDecimalBuilder.isNormal());
        for (int i = 0; i < decimal64Block.getPositionCount(); i++) {
            if (decimal64Block.isNull(i)) {
                Assert.assertTrue(toWriteNormalDecimalBuilder.isNull(i + 1));
                continue;
            }
            Assert.assertEquals("WritePosition to normal decimalBuilder result differs",
                decimal64Block.getDecimal(i), toWriteNormalDecimalBuilder.getDecimal(i + 1));
        }

        MemoryCountable.checkDeviation(toWriteNormalDecimalBuilder, .05d, true);
        MemoryCountable.checkDeviation(toWriteDecimal64Builder, .05d, true);
        MemoryCountable.checkDeviation(toWriteDecimal128Builder, .05d, true);
    }

    @Test
    public void testDecimal64HashCode() {
        final int count = 1024;
        DecimalType actualDecimalType = new DecimalType(16, 2);

        DecimalBlockBuilder decimal64Builder = new DecimalBlockBuilder(count, actualDecimalType);
        DecimalBlockBuilder normalBuilder = new DecimalBlockBuilder(count);

        long[] decimal64Values = new long[count];
        for (int i = 0; i < count; i++) {
            decimal64Values[i] = random.nextLong();
        }
        decimal64Values[0] = 0;

        for (int i = 0; i < count; i++) {
            decimal64Builder.writeLong(decimal64Values[i]);
            normalBuilder.writeDecimal(new Decimal(decimal64Values[i], actualDecimalType.getScale()));
        }
        DecimalBlock decimal64Block = (DecimalBlock) decimal64Builder.build();
        DecimalBlock normalBlock = (DecimalBlock) normalBuilder.build();
        MemoryCountable.checkDeviation(decimal64Block, .05d, true);
        MemoryCountable.checkDeviation(normalBlock, .05d, true);

        Assert.assertTrue(decimal64Block.isDecimal64());
        Assert.assertTrue(normalBlock.getState().isNormal());

        for (int i = 0; i < count; i++) {
            Assert.assertEquals("HashCode does not equal: " + normalBlock.getDecimal(i).toString(),
                normalBlock.hashCode(i), decimal64Block.hashCode(i));
        }
        MemoryCountable.checkDeviation(decimal64Builder, .05d, true);
        MemoryCountable.checkDeviation(normalBuilder, .05d, true);
    }

    @Test
    public void testDecimal64XxHashCode() {
        final int count = 1024;
        DecimalType actualDecimalType = new DecimalType(16, 2);

        DecimalBlockBuilder decimal64Builder = new DecimalBlockBuilder(count, actualDecimalType);
        DecimalBlockBuilder normalBuilder = new DecimalBlockBuilder(count);

        long[] decimal64Values = new long[count];
        for (int i = 0; i < count; i++) {
            decimal64Values[i] = random.nextLong();
        }
        decimal64Values[0] = 0;

        for (int i = 0; i < count; i++) {
            decimal64Builder.writeLong(decimal64Values[i]);
            normalBuilder.writeDecimal(new Decimal(decimal64Values[i], actualDecimalType.getScale()));
        }
        DecimalBlock decimal64Block = (DecimalBlock) decimal64Builder.build();
        DecimalBlock normalBlock = (DecimalBlock) normalBuilder.build();
        MemoryCountable.checkDeviation(decimal64Block, .05d, true);
        MemoryCountable.checkDeviation(normalBlock, .05d, true);

        Assert.assertTrue(decimal64Block.isDecimal64());
        Assert.assertTrue(normalBlock.getState().isNormal());

        for (int i = 0; i < count; i++) {
            Assert.assertEquals("HashCode using XxHash does not equal: " + normalBlock.getDecimal(i).toString(),
                normalBlock.hashCodeUseXxhash(i), decimal64Block.hashCodeUseXxhash(i));
        }
    }

    @Test
    public void testDecimal128HashCode() {
        final int count = 1024;
        DecimalType actualDecimalType = new DecimalType(16, 2);

        DecimalBlockBuilder decimal128Builder = new DecimalBlockBuilder(count, actualDecimalType);
        DecimalBlockBuilder normalBuilder = new DecimalBlockBuilder(count);

        decimal128Builder.writeDecimal128(0, 0);
        normalBuilder.writeDecimal(Decimal.ZERO);
        for (int i = 1; i < count; i++) {
            String decStr = gen128BitUnsignedNumStr();
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(),
                -actualDecimalType.getScale());
            writeDec.getDecimalStructure().setFractions(actualDecimalType.getScale());
            normalBuilder.writeDecimal(writeDec);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
            decimal128Builder.writeDecimal128(decimal128[0], decimal128[1]);
        }
        DecimalBlock decimal128Block = (DecimalBlock) decimal128Builder.build();
        DecimalBlock normalBlock = (DecimalBlock) normalBuilder.build();

        MemoryCountable.checkDeviation(decimal128Block, .05d, true);
        MemoryCountable.checkDeviation(normalBlock, .05d, true);

        Assert.assertTrue(decimal128Block.isDecimal128());
        Assert.assertTrue(normalBlock.getState().isNormal());

        for (int i = 0; i < count; i++) {
            Assert.assertEquals("HashCode does not equal: " + normalBlock.getDecimal(i).toString(),
                normalBlock.hashCode(i), decimal128Block.hashCode(i));
        }
    }

    @Test
    public void testDecimal128XxHashCode() {
        final int count = 1024;
        DecimalType actualDecimalType = new DecimalType(16, 2);

        DecimalBlockBuilder decimal128Builder = new DecimalBlockBuilder(count, actualDecimalType);
        DecimalBlockBuilder normalBuilder = new DecimalBlockBuilder(count);

        decimal128Builder.writeDecimal128(0, 0);
        normalBuilder.writeDecimal(Decimal.ZERO);
        for (int i = 1; i < count; i++) {
            String decStr = gen128BitUnsignedNumStr();
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(),
                -actualDecimalType.getScale());
            writeDec.getDecimalStructure().setFractions(actualDecimalType.getScale());
            normalBuilder.writeDecimal(writeDec);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
            decimal128Builder.writeDecimal128(decimal128[0], decimal128[1]);
        }
        DecimalBlock decimal128Block = (DecimalBlock) decimal128Builder.build();
        DecimalBlock normalBlock = (DecimalBlock) normalBuilder.build();

        MemoryCountable.checkDeviation(decimal128Block, .05d, true);
        MemoryCountable.checkDeviation(normalBlock, .05d, true);

        Assert.assertTrue(decimal128Block.isDecimal128());
        Assert.assertTrue(normalBlock.getState().isNormal());

        for (int i = 0; i < count; i++) {
            Assert.assertEquals("HashCode using XxHash does not equal: " + normalBlock.getDecimal(i).toString(),
                normalBlock.hashCodeUseXxhash(i), decimal128Block.hashCodeUseXxhash(i));
        }
    }

    /**
     * 混合写入多种不同类型的 decimal
     */
    @Test
    public void testDecimalBlockBuilderState() {
        final int scale = 2;
        int rowIdx = 0;
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(CHUNK_SIZE, new DecimalType(20, scale));
        // write a decimal64 which is simple
        blockBuilder.appendNull();
        blockBuilder.writeLong(1234);
        Assert.assertTrue(blockBuilder.isDecimal64());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(new Decimal(1234, scale), blockBuilder.getDecimal(rowIdx++));

        // write a decimal128 which is simple
        blockBuilder.appendNull();
        long[] decimal128 = new long[] {1001, 0};    // 10.01
        blockBuilder.writeDecimal128(decimal128[0], decimal128[1]);
        Assert.assertTrue(blockBuilder.isDecimal128());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(decimal128[0], blockBuilder.getDecimal128Low(rowIdx));
        Assert.assertEquals(decimal128[1], blockBuilder.getDecimal128High(rowIdx));
        Assert.assertEquals(Decimal.fromString("10.01"), blockBuilder.getDecimal(rowIdx++));
        // 验证之前写入的 Decimal64 读取正常
        Assert.assertEquals(new Decimal(1234, scale), blockBuilder.getDecimal(1));

        // write another decimal128 which is simple
        blockBuilder.appendNull();
        decimal128 = new long[] {9876, 0};    // 98.76
        blockBuilder.writeDecimal128(decimal128[0], decimal128[1]);
        Assert.assertTrue(blockBuilder.isDecimal128());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(Decimal.fromString("98.76"), blockBuilder.getDecimal(rowIdx++));

        // write a simple decimal
        blockBuilder.appendNull();
        blockBuilder.writeDecimal(new Decimal(5678, scale));
        Assert.assertTrue(blockBuilder.state.isSimple());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(new Decimal(5678, scale), blockBuilder.getDecimal(rowIdx++));

        // write a decimal64 which is simple
        blockBuilder.appendNull();
        blockBuilder.writeLong(4321);
        Assert.assertTrue(blockBuilder.state.isSimple());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(new Decimal(4321, scale), blockBuilder.getDecimal(rowIdx++));

        // write a decimal128 which is full
        blockBuilder.appendNull();
        decimal128 = new long[] {35810990, -5};    // -922337203685119470.90
        blockBuilder.writeDecimal128(decimal128[0], decimal128[1]);
        Assert.assertTrue(blockBuilder.state.isFull());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(Decimal.fromString("-922337203685119470.90"), blockBuilder.getDecimal(rowIdx++));

        // write a FULL decimal
        blockBuilder.appendNull();
        String fullDecimalStr = "12378932165498711.32";
        blockBuilder.writeDecimal(Decimal.fromString(fullDecimalStr));
        Assert.assertTrue(blockBuilder.state.isFull());
        Assert.assertTrue(blockBuilder.isNull(rowIdx++));
        Assert.assertEquals(Decimal.fromString(fullDecimalStr), blockBuilder.getDecimal(rowIdx));
    }

    @Test
    public void testDecimal128Block() {
        final int scale = 4;
        final int count = 1000;

        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(CHUNK_SIZE, new DecimalType(38, scale));
        List<Decimal> expectedResult = new ArrayList<>(count);
        List<String> decStrList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (i % 10 == 0) {
                blockBuilder.appendNull();
                expectedResult.add(null);
                decStrList.add(null);
                continue;
            }
            // append positive/negative numbers
            String decStr = gen128BitUnsignedNumStr();
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
            writeDec.getDecimalStructure().setFractions(scale);
            expectedResult.add(writeDec);
            decStrList.add(decStr);
            try {
                long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                blockBuilder.writeDecimal128(decimal128[0], decimal128[1]);
            } catch (Exception e) {
                Assert.fail("Failed to write " + writeDec + ", due to: " + e.getMessage());
            }
        }

        DecimalBlock decimalBlock = (DecimalBlock) blockBuilder.build();
        MemoryCountable.checkDeviation(decimalBlock, .05d, true);
        Assert.assertTrue(decimalBlock.isDecimal128());
        for (int i = 0; i < count; i++) {
            Decimal expectDec = expectedResult.get(i);
            Decimal actualDec;
            if (decimalBlock.isNull(i)) {
                actualDec = null;
            } else {
                actualDec = decimalBlock.getDecimal(i);
            }
            Assert.assertEquals(String.format("Failed at round: %d, unsigned string: %s, decimal128: [%d, %d]",
                    i, decStrList.get(i), decimalBlock.decimal64Values[i], decimalBlock.decimal128HighValues[i]),
                expectDec, actualDec);
            Assert.assertEquals("Failed at round: " + i + ", unsigned string: " + decStrList.get(i),
                expectDec, actualDec);
        }
    }

    @Test
    public void testDecimalEquals() {
        final int count = 2000;
        final int scale1 = 2;
        final int scale2 = 4;
        DecimalType decimal64Type1 = new DecimalType(16, scale1);
        DecimalType decimal64Type2 = new DecimalType(17, scale2);
        DecimalType decimal128Type1 = new DecimalType(30, scale1);
        DecimalType decimal128Type2 = new DecimalType(30, scale2);
        final int scaleDiff = scale2 - scale1;
        final long power = DecimalTypeBase.POW_10_LONG[scaleDiff];

        DecimalBlockBuilder decimal64Builder1 = new DecimalBlockBuilder(count, decimal64Type1);
        DecimalBlockBuilder decimal64Builder2 = new DecimalBlockBuilder(count, decimal64Type2);
        DecimalBlockBuilder decimal128Builder1 = new DecimalBlockBuilder(count, decimal128Type1);
        DecimalBlockBuilder decimal128Builder2 = new DecimalBlockBuilder(count, decimal128Type2);

        DecimalBlockBuilder normalBuilder1 = new DecimalBlockBuilder(count);
        DecimalBlockBuilder normalBuilder2 = new DecimalBlockBuilder(count);

        long[] decimal64Values = new long[count];
        for (int i = 0; i < count; i++) {
            int val = random.nextInt();
            val = (val == 0) ? Integer.MAX_VALUE : val;
            decimal64Values[i] = val;
        }
        decimal64Values[0] = 0;

        for (int i = 0; i < count; i++) {
            long high = decimal64Values[i] >= 0 ? 0 : -1;
            if (i % 4 == 0) {
                // write equals
                decimal64Builder1.writeLong(decimal64Values[i]);
                decimal64Builder2.writeLong(decimal64Values[i] * power);
                decimal128Builder1.writeDecimal128(decimal64Values[i], high);
                decimal128Builder2.writeDecimal128(decimal64Values[i] * power, high);
                normalBuilder1.writeDecimal(new Decimal(decimal64Values[i], decimal64Type1.getScale()));
                normalBuilder2.writeDecimal(new Decimal(decimal64Values[i], decimal64Type1.getScale()));
            } else if (i % 4 == 1) {
                // write diff sign
                decimal64Builder1.writeLong(decimal64Values[i]);
                decimal64Builder2.writeLong(decimal64Values[i] * -1);
                decimal128Builder1.writeDecimal128(decimal64Values[i] * -1, -1);
                decimal128Builder2.writeDecimal128(decimal64Values[i] * -1, -1);
                normalBuilder1.writeDecimal(new Decimal(decimal64Values[i], decimal64Type1.getScale()));
                normalBuilder2.writeDecimal(new Decimal(-decimal64Values[i], decimal64Type1.getScale()));
            } else if (i % 4 == 2) {
                // write smaller value
                decimal64Builder1.writeLong(decimal64Values[i]);
                decimal64Builder2.writeLong(decimal64Values[i] * power - 1);
                decimal128Builder1.writeDecimal128(decimal64Values[i] * power - 2, high);
                decimal128Builder2.writeDecimal128(decimal64Values[i] * power - 3, high);
                normalBuilder1.writeDecimal(new Decimal(decimal64Values[i], decimal64Type1.getScale()));
                normalBuilder2.writeDecimal(new Decimal(-decimal64Values[i], decimal64Type1.getScale()));
            } else {
                // write larger value
                decimal64Builder1.writeLong(decimal64Values[i]);
                decimal64Builder2.writeLong(decimal64Values[i] * power + 1);
                decimal128Builder1.writeDecimal128(decimal64Values[i] * power + 2, high);
                decimal128Builder2.writeDecimal128(decimal64Values[i] * power + 3, high);
                normalBuilder1.writeDecimal(new Decimal(decimal64Values[i], decimal64Type1.getScale()));
                normalBuilder2.writeDecimal(new Decimal(-decimal64Values[i], decimal64Type1.getScale()));
            }
        }
        DecimalBlock decimal64Block1 = (DecimalBlock) decimal64Builder1.build();
        DecimalBlock decimal64Block2 = (DecimalBlock) decimal64Builder2.build();
        DecimalBlock decimal128Block1 = (DecimalBlock) decimal128Builder1.build();
        DecimalBlock decimal128Block2 = (DecimalBlock) decimal128Builder2.build();
        DecimalBlock normalBlock1 = (DecimalBlock) normalBuilder1.build();
        DecimalBlock normalBlock2 = (DecimalBlock) normalBuilder2.build();

        MemoryCountable.checkDeviation(decimal64Block1, .05d, true);
        MemoryCountable.checkDeviation(decimal64Block2, .05d, true);
        MemoryCountable.checkDeviation(decimal128Block1, .05d, true);
        MemoryCountable.checkDeviation(decimal128Block2, .05d, true);
        MemoryCountable.checkDeviation(normalBlock1, .05d, true);
        MemoryCountable.checkDeviation(normalBlock2, .05d, true);

        Assert.assertTrue(decimal64Block1.isDecimal64());
        Assert.assertTrue(decimal64Block2.isDecimal64());
        Assert.assertTrue(decimal128Block1.isDecimal128());
        Assert.assertTrue(decimal128Block2.isDecimal128());
        Assert.assertTrue(normalBlock1.getState().isNormal());
        Assert.assertTrue(normalBlock2.getState().isNormal());

        for (int i = 0; i < count; i++) {
            if (i % 4 == 0) {
                Assert.assertTrue("Failed at : " + i, decimal64Block1.equals(i, decimal64Block2, i));
                Assert.assertTrue("Failed at : " + i, decimal64Block2.equals(i, decimal64Block1, i));
                Assert.assertTrue("Failed at : " + i, decimal64Block1.equals(i, decimal128Block1, i));
                Assert.assertTrue("Failed at : " + i, decimal128Block1.equals(i, decimal128Block2, i));
                Assert.assertTrue("Failed at : " + i, decimal128Block1.equals(i, decimal64Block1, i));
                Assert.assertTrue("Failed at : " + i, decimal128Block2.equals(i, decimal128Block1, i));
                Assert.assertTrue("Failed at : " + i, decimal64Block1.equals(i, normalBlock1, i));
                Assert.assertTrue("Failed at : " + i, decimal128Block1.equals(i, normalBlock1, i));
                Assert.assertTrue("Failed at : " + i, normalBlock1.equals(i, decimal64Block1, i));
                Assert.assertTrue("Failed at : " + i, normalBlock1.equals(i, normalBlock2, i));
            } else {
                Assert.assertFalse("Failed at : " + i, decimal64Block1.equals(i, decimal64Block2, i));
                Assert.assertFalse("Failed at : " + i, decimal64Block2.equals(i, decimal64Block1, i));
                Assert.assertFalse("Failed at : " + i, decimal64Block1.equals(i, decimal128Block1, i));
                Assert.assertFalse("Failed at : " + i, decimal128Block1.equals(i, decimal128Block2, i));
                Assert.assertFalse("Failed at : " + i, decimal128Block1.equals(i, decimal64Block1, i));
                Assert.assertFalse("Failed at : " + i, decimal128Block2.equals(i, decimal128Block1, i));
                Assert.assertFalse("Failed at : " + i, decimal64Block1.equals(i, normalBlock2, i));
                Assert.assertFalse("Failed at : " + i, decimal128Block1.equals(i, normalBlock1, i));
                Assert.assertFalse("Failed at : " + i, normalBlock2.equals(i, decimal64Block1, i));
                Assert.assertFalse("Failed at : " + i, normalBlock1.equals(i, decimal128Block1, i));
            }
        }
    }

    /**
     * generate random unsigned decimal128 String
     */
    private String gen128BitUnsignedNumStr() {
        long l1 = Math.abs(random.nextLong());
        l1 = (l1 < 0) ? Long.MAX_VALUE : l1;
        long l2 = Math.abs(random.nextLong());
        l2 = (l2 < 0) ? Long.MAX_VALUE : l2;
        String largeNumStr = String.format("%d%d", l1, l2);
        if (largeNumStr.length() > Decimal.MAX_128_BIT_PRECISION) {
            largeNumStr = largeNumStr.substring(0, Decimal.MAX_128_BIT_PRECISION);
        }
        return largeNumStr;
    }

    @Test
    public void testCopyDecimal64() {
        final int count = 1000;
        final int selCount = 500;
        final int[] sel = new int[count];
        for (int i = 0; i < selCount; i++) {
            sel[i] = i * 2;
        }
        DecimalType decimalType = new DecimalType(16, 2);
        DecimalBlockBuilder decimal64Builder = new DecimalBlockBuilder(count, decimalType);

        long[] decimal64Values = new long[count];
        for (int i = 0; i < count; i++) {
            decimal64Values[i] = random.nextLong();
        }
        decimal64Values[0] = 0;

        for (int i = 0; i < count; i++) {
            decimal64Builder.writeLong(decimal64Values[i]);
        }
        DecimalBlock decimal64Block = (DecimalBlock) decimal64Builder.build();
        MemoryCountable.checkDeviation(decimal64Block, .05d, true);

        DecimalBlock unallocOutput = new DecimalBlock(decimalType, count);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.isUnalloc());
        testCopySelected(decimal64Block, unallocOutput, selCount);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.isDecimal64());
        // test with selection
        unallocOutput = new DecimalBlock(decimalType, count);
        testCopySelected(decimal64Block, unallocOutput, sel, selCount);
        unallocOutput = null;

        DecimalBlock decimal64Output = new DecimalBlock(decimalType, count);
        decimal64Output.allocateDecimal64();
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.isDecimal64());
        testCopySelected(decimal64Block, decimal64Output, selCount);
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.isDecimal64());
        // test with selection
        decimal64Output = new DecimalBlock(decimalType, count);
        testCopySelected(decimal64Block, decimal64Output, sel, selCount);
        decimal64Output = null;

        DecimalBlock decimal128Output = new DecimalBlock(decimalType, count);
        decimal128Output.allocateDecimal128();
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.isDecimal128());
        testCopySelected(decimal64Block, decimal128Output, selCount);
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.isDecimal128());
        // test with selection
        decimal128Output = new DecimalBlock(decimalType, count);
        testCopySelected(decimal64Block, decimal128Output, sel, selCount);
        decimal128Output = null;

        DecimalBlock normalOutput = new DecimalBlock(decimalType, count);
        normalOutput.allocateNormalDecimal();
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isUnset());
        testCopySelected(decimal64Block, normalOutput, selCount);
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isNormal());
        try {
            normalOutput.allocateDecimal64();
            Assert.fail("Should fail since it is in normal state");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Should not allocate decimal64"));
        }

        // test with selection
        normalOutput = new DecimalBlock(decimalType, count);
        testCopySelected(decimal64Block, normalOutput, sel, selCount);
        normalOutput = null;
    }

    @Test
    public void testCopyDecimal128() {
        final int count = 1000;
        final int selCount = 500;
        final int scale = 3;
        final int[] sel = new int[count];
        for (int i = 0; i < selCount; i++) {
            sel[i] = i * 2;
        }
        DecimalType decimalType = new DecimalType(16, scale);

        DecimalBlock decimal128Block = genRandomDecimal128BlockWithNull(count, scale);

        DecimalBlock unallocOutput = new DecimalBlock(decimalType, count);
        MemoryCountable.checkDeviation(unallocOutput, .05d, true);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.isUnalloc());
        testCopySelected(decimal128Block, unallocOutput, selCount);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.isDecimal128());
        // test with selection
        unallocOutput = new DecimalBlock(decimalType, count);
        testCopySelected(decimal128Block, unallocOutput, sel, selCount);
        unallocOutput = null;

        DecimalBlock decimal64Output = new DecimalBlock(decimalType, count);
        decimal64Output.allocateDecimal64();
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.isDecimal64());
        testCopySelected(decimal128Block, decimal64Output, selCount);
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.isDecimal128());
        // test with selection
        decimal64Output = new DecimalBlock(decimalType, count);
        testCopySelected(decimal128Block, decimal64Output, sel, selCount);
        decimal64Output = null;

        DecimalBlock decimal128Output = new DecimalBlock(decimalType, count);
        decimal128Output.allocateDecimal128();
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.isDecimal128());
        testCopySelected(decimal128Block, decimal128Output, selCount);
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.isDecimal128());
        // test with selection
        decimal128Output = new DecimalBlock(decimalType, count);
        testCopySelected(decimal128Block, decimal128Output, sel, selCount);
        decimal128Output = null;

        DecimalBlock normalOutput = new DecimalBlock(decimalType, count);
        normalOutput.allocateNormalDecimal();
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isUnset());
        testCopySelected(decimal128Block, normalOutput, selCount);
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isNormal());
        try {
            normalOutput.allocateDecimal128();
            Assert.fail("Should fail since it is in normal state");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Should not allocate decimal128"));
        }

        // test with selection
        normalOutput = new DecimalBlock(decimalType, count);
        testCopySelected(decimal128Block, normalOutput, sel, selCount);
        normalOutput = null;
    }

    private void testCopySelected(DecimalBlock fromBlock, DecimalBlock outputBlock, int selCount) {
        fromBlock.copySelected(false, null, selCount, outputBlock);
        for (int i = 0; i < selCount; i++) {
            Assert.assertTrue(fromBlock.equals(i, outputBlock, i));
            if (fromBlock.isNull(i)) {
                Assert.assertTrue(outputBlock.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getDecimal(i), outputBlock.getDecimal(i));
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getElementAtUnchecked(i), outputBlock.getElementAtUnchecked(i));
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getRegion(i), outputBlock.getRegion(i));
        }
    }

    private void testCopySelected(DecimalBlock fromBlock, DecimalBlock outputBlock, int[] sel, int selCount) {
        fromBlock.copySelected(true, sel, selCount, outputBlock);
        for (int i = 0; i < selCount; i++) {
            int pos = sel[i];
            Assert.assertTrue(outputBlock.equals(pos, outputBlock, pos));
            if (fromBlock.isNull(pos)) {
                Assert.assertTrue(outputBlock.isNull(pos));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getDecimal(pos), outputBlock.getDecimal(pos));
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getElementAtUnchecked(pos), outputBlock.getElementAtUnchecked(pos));
            Assert.assertEquals("Failed at pos: " + i,
                fromBlock.getRegion(pos), outputBlock.getRegion(pos));
        }
    }

    @Test
    public void testCopyNormalDecimal() {
        final int count = 1000;
        final int selCount = 500;
        final int scale = 4;
        final int[] sel = new int[count];
        for (int i = 0; i < selCount; i++) {
            sel[i] = i * 2;
        }
        DecimalType decimalType = new DecimalType(16, scale);

        DecimalBlock normalDecimalBlock = genRandomDecimalBlockWithNull(count, scale);

        DecimalBlock unallocOutput = new DecimalBlock(decimalType, count);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.isUnalloc());
        testCopySelected(normalDecimalBlock, unallocOutput, selCount);
        Assert.assertTrue("Actual state: " + unallocOutput.getState(),
            unallocOutput.getState().isNormal());
        // test with selection
        unallocOutput = new DecimalBlock(decimalType, count);
        testCopySelected(normalDecimalBlock, unallocOutput, sel, selCount);
        unallocOutput = null;

        DecimalBlock decimal64Output = new DecimalBlock(decimalType, count);
        decimal64Output.allocateDecimal64();
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.isDecimal64());
        testCopySelected(normalDecimalBlock, decimal64Output, selCount);
        Assert.assertTrue("Actual state: " + decimal64Output.getState(),
            decimal64Output.getState().isNormal());
        // test with selection
        decimal64Output = new DecimalBlock(decimalType, count);
        testCopySelected(normalDecimalBlock, decimal64Output, sel, selCount);
        decimal64Output = null;

        DecimalBlock decimal128Output = new DecimalBlock(decimalType, count);
        decimal128Output.allocateDecimal128();
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.isDecimal128());
        testCopySelected(normalDecimalBlock, decimal128Output, selCount);
        Assert.assertTrue("Actual state: " + decimal128Output.getState(),
            decimal128Output.getState().isNormal());
        // test with selection
        decimal128Output = new DecimalBlock(decimalType, count);
        testCopySelected(normalDecimalBlock, decimal128Output, sel, selCount);
        decimal128Output = null;

        DecimalBlock normalOutput = new DecimalBlock(decimalType, count);
        normalOutput.allocateNormalDecimal();
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isUnset());
        testCopySelected(normalDecimalBlock, normalOutput, selCount);
        Assert.assertTrue("Actual state: " + normalOutput.getState(),
            normalOutput.getState().isNormal());
        // test with selection
        normalOutput = new DecimalBlock(decimalType, count);
        testCopySelected(normalDecimalBlock, normalOutput, sel, selCount);
        normalOutput = null;
    }

    private DecimalBlock genRandomDecimal128BlockWithNull(int count, int scale) {
        DecimalType decimalType = new DecimalType(Decimal.MAX_128_BIT_PRECISION, scale);
        DecimalBlockBuilder decimal128Builder = new DecimalBlockBuilder(count, decimalType);

        for (int i = 0; i < count; i++) {
            if (i % 10 == 0) {
                decimal128Builder.appendNull();
                continue;
            }

            String decStr = gen128BitUnsignedNumStr();
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
            writeDec.getDecimalStructure().setFractions(scale);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
            decimal128Builder.writeDecimal128(decimal128[0], decimal128[1]);
        }
        Assert.assertTrue("Actual state: " + decimal128Builder.state, decimal128Builder.isDecimal128());
        DecimalBlock decimalBlock = (DecimalBlock) decimal128Builder.build();
        Assert.assertTrue(decimalBlock.isDecimal128());
        return decimalBlock;
    }

    /**
     * @return normal DecimalBlock
     */
    private DecimalBlock genRandomDecimalBlockWithNull(int count, int scale) {
        DecimalType decimalType = new DecimalType(Decimal.MAX_128_BIT_PRECISION, scale);
        DecimalBlockBuilder decimalBuilder = new DecimalBlockBuilder(count, decimalType);

        for (int i = 0; i < count; i++) {
            if (i % 10 == 0) {
                decimalBuilder.appendNull();
                continue;
            }

            String decStr = gen128BitUnsignedNumStr();
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
            writeDec.getDecimalStructure().setFractions(scale);
            decimalBuilder.writeDecimal(writeDec);
        }
        Assert.assertTrue("Actual state: " + decimalBuilder.state, decimalBuilder.isNormal());
        DecimalBlock decimalBlock = (DecimalBlock) decimalBuilder.build();
        Assert.assertTrue(decimalBlock.getState().isNormal());
        return decimalBlock;
    }

    private Decimal from(String decStr) {
        return Decimal.fromString(decStr);
    }

    private Decimal from(long unscaled, int scale) {
        return new Decimal(unscaled, scale);
    }
}
