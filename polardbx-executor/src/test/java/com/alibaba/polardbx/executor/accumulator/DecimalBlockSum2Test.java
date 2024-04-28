package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/**
 * Do sum inside a DecimalBlock with groupIdSelection
 */
@RunWith(Parameterized.class)
public class DecimalBlockSum2Test extends DecimalSumV2Test {

    private final int[][] groupIdSelections;
    private final int groupSize;
    private final int selOffset;

    public DecimalBlockSum2Test(int scale, boolean overflowDecimal64,
                                boolean overflowDecimal128, int groupCount,
                                boolean withSelection) {
        super(scale, overflowDecimal64, overflowDecimal128, groupCount, withSelection);
        this.groupIdSelections = new int[groupCount][COUNT];
        if (withSelection) {
            this.groupSize = SEL_COUNT / groupCount;
            this.selOffset = COUNT / 2;
            fillSelection();
        } else {
            this.groupSize = COUNT / groupCount;
            this.selOffset = 0;
        }
        for (int i = 0; i < groupCount; i++) {
            int[] groupIdSelection = groupIdSelections[i];
            if (withSelection) {
                for (int j = 0; j < groupSize; j++) {
                    groupIdSelection[j] = j * groupCount + i;
                }
            } else {
                for (int j = 0; j < groupSize; j++) {
                    groupIdSelection[j] = j * groupCount + i;
                }
            }
        }
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

    @Override
    protected void fillSelection() {
        if (selection != null) {
            for (int i = 0; i < SEL_COUNT; i++) {
                this.selection[i] = i + selOffset;
            }
        }
    }

    /**
     * 预计算结果
     */
    @Override
    protected void computeResult() {
        for (Block inputBlock : inputBlocks) {
            for (int j = 0; j < inputBlock.getPositionCount(); j++) {
                DecimalBlock decimalBlock = (DecimalBlock) inputBlock;
                Decimal decimal = decimalBlock.getDecimal(j);
                int group;
                if (selection == null) {
                    group = groupIds[j];
                } else {
                    group = groupIds[selection[j]];
                }
                targetSums[group] = targetSums[group].add(decimal.toBigDecimal());
            }
        }
    }

    /**
     * 仅 Decimal64 block求和
     */
    @Override
    @Test
    public void testDecimal64Sum() {
        if (overflowDec128) {
            return;
        }
        buildDecimal64Blocks();

        for (Block inputBlock : inputBlocks) {
            Assert.assertTrue(((DecimalBlock) inputBlock).isDecimal64());
            Chunk inputChunk = new Chunk(inputBlock);
            for (int i = 0; i < groupCount; i++) {
                accumulator.accumulate(i, inputChunk, groupIdSelections[i], groupSize);
            }
        }

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount, dataTypes[0]);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            // 溢出检查
            Assert.assertEquals("Expect overflowDec64=" + overflowDec64, overflowDec64,
                accumulator.isOverflowDecimal64(groupId));
            Assert.assertFalse("Expect not overflowDec128 even when overflowDec64",
                accumulator.isOverflowDecimal128(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }

        validateResult(resultBlockBuilder);
    }

    @Override
    @Test
    public void testDecimal128Sum() {
        buildDecimal128Blocks();
        for (Block inputBlock : inputBlocks) {
            Assert.assertTrue(((DecimalBlock) inputBlock).isDecimal128());
            Chunk inputChunk = new Chunk(inputBlock);
            for (int i = 0; i < groupCount; i++) {
                accumulator.accumulate(i, inputChunk, groupIdSelections[i], groupSize);
            }
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
    @Override
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
            for (int i = 0; i < groupCount; i++) {
                accumulator.accumulate(i, inputChunk, groupIdSelections[i], groupSize);
            }
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
    @Override
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
            for (int i = 0; i < groupCount; i++) {
                accumulator.accumulate(i, inputChunk, groupIdSelections[i], groupSize);
            }
        }

        DecimalBlockBuilder resultBlockBuilder = new DecimalBlockBuilder(groupCount);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            Assert.assertTrue("Expect decimal box", accumulator.isDecimalBox(groupId));
            accumulator.writeResultTo(groupId, resultBlockBuilder);
        }
        validateResult(resultBlockBuilder);
    }
}
