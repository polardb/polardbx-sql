package com.alibaba.polardbx.executor.accumulator.sum;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.accumulator.DecimalSumAccumulator;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class DecimalSumV2Test {

    protected static final int COUNT = 1024;
    protected static final int SEL_COUNT = COUNT / 2;
    protected static final int BLOCK_COUNT = 10;
    protected static final int INT_PART_LEN = 8;  // 整数部分长度

    protected static final int INT_BASE = 5000_0000;
    protected static final int INT_RAND_BOUND = 1000_0000;    // exclusive

    protected final int groupCount;
    protected final int scale;
    protected final boolean overflowDec64;
    protected final boolean overflowDec128;
    protected final BigDecimal[] targetSums;
    protected final DataType[] dataTypes;
    protected final int[] selection;

    protected List<Block> inputBlocks;
    protected int[] groupIds;
    protected Random random;

    protected DecimalSumAccumulator accumulator;

    public DecimalSumV2Test(int scale, boolean overflowDecimal64,
                            boolean overflowDecimal128, int groupCount,
                            boolean withSelection) {
        this.scale = scale;
        this.overflowDec64 = overflowDecimal64;
        this.overflowDec128 = overflowDecimal128;
        this.groupCount = groupCount;
        this.targetSums = new BigDecimal[groupCount];
        for (int i = 0; i < groupCount; i++) {
            targetSums[i] = BigDecimal.ZERO;
        }
        DecimalType decimal64Type = new DecimalType(Decimal.MAX_64_BIT_PRECISION, scale);
        this.dataTypes = new DataType[] {decimal64Type};
        if (withSelection) {
            this.selection = new int[COUNT];

        } else {
            this.selection = null;
        }

        this.groupIds = new int[COUNT];
        fillSelection();
        fillGroupIds();
    }

    @Parameterized.Parameters(name = "scale={0},overflowDec64={1},overflowDec128={2},group={3},selection={4}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        int overflowDec64Scale = Decimal.MAX_64_BIT_PRECISION - INT_PART_LEN - 1;
        int overflowDec128Scale = Decimal.MAX_128_BIT_PRECISION - INT_PART_LEN - 1;
        int[] scales = {0, 1, 2, 4, 5, overflowDec64Scale, overflowDec128Scale};
        for (int scale : scales) {
            boolean overflowDecimal64 = (scale >= overflowDec64Scale);
            boolean overflowDecimal128 = (scale >= overflowDec128Scale);
            list.add(new Object[] {scale, overflowDecimal64, overflowDecimal128, 1, true});
            list.add(new Object[] {scale, overflowDecimal64, overflowDecimal128, 1, false});
            list.add(new Object[] {scale, overflowDecimal64, overflowDecimal128, 4, true});
            list.add(new Object[] {scale, overflowDecimal64, overflowDecimal128, 4, false});
        }
        return list;
    }

    protected void fillGroupIds() {
        for (int i = 0; i < COUNT; i++) {
            this.groupIds[i] = i % groupCount;
        }
    }

    protected void fillSelection() {
        if (selection != null) {
            for (int i = 0; i < SEL_COUNT; i++) {
                this.selection[i] = i * 2;
            }
        }
    }

    @Before
    public void before() {
        this.random = new Random();
        this.accumulator = new DecimalSumAccumulator(groupCount, dataTypes[0]);
        this.inputBlocks = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            this.accumulator.appendInitValue();
        }

    }

    protected void buildDecimal64Blocks() {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            this.inputBlocks.add(getDecimal64Block());
        }
        computeResult();
    }

    protected void buildDecimal128Blocks() {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            this.inputBlocks.add(getDecimal128Block());
        }
        computeResult();
    }

    protected void buildMixedBlocks() {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            if (i % 2 == 0) {
                if (overflowDec128) {
                    this.inputBlocks.add(getDecimal128Block());
                } else {
                    this.inputBlocks.add(getDecimal64Block());
                }
            } else {
                this.inputBlocks.add(getNormalDecimalBlock());
            }
        }
        computeResult();
    }

    protected void buildSimpleBlocks() {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            this.inputBlocks.add(getSimpleBlock());
        }
        computeResult();
    }

    /**
     * 预计算结果
     */
    protected void computeResult() {
        for (Block inputBlock : inputBlocks) {
            for (int j = 0; j < inputBlock.getPositionCount(); j++) {
                Decimal decimal = inputBlock.getDecimal(j);
                targetSums[j % groupCount] = targetSums[j % groupCount].add(decimal.toBigDecimal());
            }
        }
    }

    protected Block getDecimal64Block() {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(COUNT, dataTypes[0]);
        for (int i = 0; i < COUNT; i++) {
            int intPart = random.nextInt(INT_RAND_BOUND) + INT_BASE;
            if (scale == 0) {
                blockBuilder.writeLong(intPart);
            } else {
                long fracBase = (long) Math.pow(10, scale - 1);
                long fracPart = RandomUtils.nextLong(fracBase, fracBase * 10);
                long longVal = Long.parseLong(intPart + "" + fracPart);
                String decimalStrVal = intPart + "." + fracPart;
                Assert.assertEquals(decimalStrVal.length(), scale + INT_PART_LEN + 1);
                blockBuilder.writeLong(longVal);
            }
        }
        Assert.assertTrue(blockBuilder.isDecimal64());
        Assert.assertFalse(blockBuilder.isUnset());
        Assert.assertFalse(blockBuilder.isSimple());

        DecimalBlock dec64Block = (DecimalBlock) blockBuilder.build();

        if (this.selection != null) {
            dec64Block = DecimalBlock.from(dec64Block, SEL_COUNT, selection, true);
            Assert.assertNotNull(dec64Block.getSelection());
        }
        Assert.assertTrue(dec64Block.isDecimal64());
        return dec64Block;
    }

    protected Block getDecimal128Block() {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(COUNT, dataTypes[0]);

        for (int i = 0; i < COUNT; i++) {
            String decStr = gen128BitUnsignedNumStr(overflowDec128);
            if (i % 2 == 0) {
                decStr = "-" + decStr;
            }

            Decimal writeDec = Decimal.fromString(decStr);
            FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
            writeDec.getDecimalStructure().setFractions(scale);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
            blockBuilder.writeDecimal128(decimal128[0], decimal128[1]);
        }

        Assert.assertTrue(blockBuilder.isDecimal128());
        Assert.assertFalse(blockBuilder.isDecimal64());
        Assert.assertFalse(blockBuilder.isUnset());
        Assert.assertFalse(blockBuilder.isSimple());

        DecimalBlock dec128Block = (DecimalBlock) blockBuilder.build();

        if (this.selection != null) {
            dec128Block = DecimalBlock.from(dec128Block, SEL_COUNT, selection, true);
            Assert.assertNotNull(dec128Block.getSelection());
        }
        Assert.assertTrue(dec128Block.isDecimal128());
        return dec128Block;
    }

    /**
     * generate random unsigned decimal128 String
     */
    private String gen128BitUnsignedNumStr(boolean overflow) {
        long l1 = Math.abs(random.nextLong());
        l1 = (l1 < 0) ? Long.MAX_VALUE : l1;
        long l2 = Math.abs(random.nextLong());
        l2 = (l2 < 0) ? Long.MAX_VALUE : l2;
        String largeNumStr = String.format("%d%d", l1, l2);
        if (!overflow) {
            if (largeNumStr.length() > Decimal.MAX_128_BIT_PRECISION - 4) {
                largeNumStr = largeNumStr.substring(0, Decimal.MAX_128_BIT_PRECISION - 4);
            }
        } else {
            if (largeNumStr.length() <= Decimal.MAX_128_BIT_PRECISION - 4) {
                StringBuilder sb = new StringBuilder(largeNumStr);
                while (sb.length() <= Decimal.MAX_128_BIT_PRECISION - 4) {
                    sb.append('9');
                }
                largeNumStr = sb.toString();
            }
        }
        return largeNumStr;
    }

    protected Block getNormalDecimalBlock() {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(COUNT, dataTypes[0]);
        for (int i = 0; i < COUNT; i++) {
            int intPart = random.nextInt(INT_RAND_BOUND) + INT_BASE;
            if (scale == 0) {
                blockBuilder.writeDecimal(new Decimal(intPart, scale));
            } else {
                String decimalStrVal;
                if (scale <= 16) {
                    long fracBase = (long) Math.pow(10, scale - 1);
                    long fracPart = RandomUtils.nextLong(fracBase, fracBase * 10);
                    decimalStrVal = intPart + "." + fracPart;
                } else {
                    BigInteger minLimit = BigInteger.TEN.pow(scale - 1);
                    BigInteger maxLimit = BigInteger.TEN.pow(scale).subtract(BigInteger.ONE);
                    BigInteger bigIntegerRange = maxLimit.subtract(minLimit);
                    BigInteger fracPart =
                        minLimit.add(new BigInteger(bigIntegerRange.bitLength(), random).mod(bigIntegerRange));
                    decimalStrVal = intPart + "." + fracPart;
                }

                Assert.assertEquals(decimalStrVal.length(), scale + INT_PART_LEN + 1);
                blockBuilder.writeDecimal(Decimal.fromString(decimalStrVal));
            }
        }
        Assert.assertTrue(blockBuilder.isNormal());
        Assert.assertFalse(blockBuilder.isUnset());
        Assert.assertFalse(blockBuilder.isDecimal64());
        Assert.assertFalse(blockBuilder.isDecimal128());

        DecimalBlock normalDecBlock = (DecimalBlock) blockBuilder.build();
        if (this.selection != null) {
            normalDecBlock = DecimalBlock.from(normalDecBlock, SEL_COUNT, selection, true);
            Assert.assertNotNull(normalDecBlock.getSelection());
        }
        Assert.assertTrue(normalDecBlock.getState().isNormal());
        return normalDecBlock;
    }

    protected Block getSimpleBlock() {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(COUNT, dataTypes[0]);
        for (int i = 0; i < COUNT; i++) {
            int intPart = random.nextInt(INT_RAND_BOUND) + INT_BASE;
            if (scale == 0) {
                blockBuilder.writeDecimal(new Decimal(intPart, scale));
            } else {
                long fracBase = (long) Math.pow(10, scale - 1);
                long fracPart = RandomUtils.nextLong(fracBase, fracBase * 10);
                long longVal = Long.parseLong(intPart + "" + fracPart);
                String decimalStrVal = intPart + "." + fracPart;
                Assert.assertEquals(decimalStrVal.length(), scale + INT_PART_LEN + 1);
                blockBuilder.writeDecimal(new Decimal(longVal, scale));
            }
        }
        Assert.assertTrue(blockBuilder.isNormal());
        Assert.assertTrue(blockBuilder.isSimple());
        Assert.assertFalse(blockBuilder.isUnset());
        Assert.assertFalse(blockBuilder.isDecimal64());

        DecimalBlock simpleBlock = (DecimalBlock) blockBuilder.build();
        if (this.selection != null) {
            simpleBlock = DecimalBlock.from(simpleBlock, SEL_COUNT, selection, true);
            Assert.assertNotNull(simpleBlock.getSelection());
        }
        Assert.assertFalse(simpleBlock.isDecimal64());
        return simpleBlock;
    }

    /**
     * 仅 Decimal64 block求和
     */
    @Test
    public void testDecimal64Sum() {
        if (overflowDec128) {
            return;
        }
        buildDecimal64Blocks();

        for (Block inputBlock : inputBlocks) {
            Assert.assertTrue(((DecimalBlock) inputBlock).isDecimal64());
            Chunk inputChunk = new Chunk(inputBlock);
            accumulator.accumulate(groupIds, inputChunk, inputChunk.getPositionCount());
        }

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount, dataTypes[0]);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            // 溢出检查
            Assert.assertEquals("Expect overflowDec64=" + overflowDec64, overflowDec64,
                accumulator.isOverflowDecimal64(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }

        validateResult(resultBlockBuilder);
    }

    @Test
    public void testDecimal128Sum() {
        buildDecimal128Blocks();
        for (Block inputBlock : inputBlocks) {
            Assert.assertTrue(((DecimalBlock) inputBlock).isDecimal128());
            Chunk inputChunk = new Chunk(inputBlock);
            accumulator.accumulate(groupIds, inputChunk, inputChunk.getPositionCount());
        }

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount, dataTypes[0]);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            // 溢出检查
            Assert.assertEquals("Expect overflowDec128=" + overflowDec128, overflowDec128,
                accumulator.isOverflowDecimal128(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }

        validateResult(resultBlockBuilder);
    }

    /**
     * Decimal64 与 NormalDecimal 混合求和
     * NormalDecimal 既有SIMPLE 又有FULL
     */
    @Test
    public void testMixedDecimalInputSum() {
        buildMixedBlocks();

        int flag = 0;
        for (Block inputBlock : inputBlocks) {
            if (((DecimalBlock) inputBlock).isDecimal64()) {
                flag |= 0x001;
            } else if (((DecimalBlock) inputBlock).isDecimal128()) {
                flag |= 0x010;
            } else {
                flag |= 0x100;
            }
            Chunk inputChunk = new Chunk(inputBlock);
            accumulator.accumulate(groupIds, inputChunk, inputChunk.getPositionCount());
        }
        Assert.assertTrue(Integer.bitCount(flag) > 1);    // 校验 mixed 输入

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount, dataTypes[0]);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            // Mixed 肯定溢出 Decimal64 与 Decimal128
            Assert.assertTrue("Expect overflowDec64", accumulator.isOverflowDecimal64(groupId));
            Assert.assertTrue("Expect overflowDec128", accumulator.isOverflowDecimal128(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }
        validateResult(resultBlockBuilder);
    }

    /**
     * 当输入仅为 NormalDecimal 且 SIMPLE时
     * 走的是 DecimalBox 求和
     */
    @Test
    public void testDecimalBoxSum() {
        if (overflowDec128) {
            return;
        }
        if (scale == 0) {
            // scale == 0 时非simple
            return;
        }
        buildSimpleBlocks();

        for (Block inputBlock : inputBlocks) {
            Assert.assertFalse(((DecimalBlock) inputBlock).isDecimal64());
            Assert.assertTrue(((DecimalBlock) inputBlock).isSimple());
            Chunk inputChunk = new Chunk(inputBlock);
            accumulator.accumulate(groupIds, inputChunk, inputChunk.getPositionCount());
        }

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            Assert.assertTrue("Expect decimal box", accumulator.isDecimalBox(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }
        validateResult(resultBlockBuilder);
    }

    /**
     * 结果校验
     */
    protected void validateResult(DecimalBlockBuilder resultBlockBuilder) {
        DecimalBlock resultBlock = (DecimalBlock) resultBlockBuilder.build();

        for (int groupId = 0; groupId < groupCount; groupId++) {
            Assert.assertEquals(targetSums[groupId].toPlainString(), resultBlock.getDecimal(groupId).toString());
        }
    }
}
