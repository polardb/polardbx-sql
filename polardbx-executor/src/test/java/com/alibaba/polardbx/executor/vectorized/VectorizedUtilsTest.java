package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class VectorizedUtilsTest {

    private final int count = 100;
    private final int[] sel;
    private final boolean withSelection;

    public VectorizedUtilsTest(boolean withSelection) {
        this.withSelection = withSelection;
        if (withSelection) {
            this.sel = new int[count / 2];
            for (int i = 0; i < this.sel.length; i++) {
                this.sel[i] = i * 2 + 1;
            }
        } else {
            this.sel = null;
        }
    }

    @Parameterized.Parameters(name = "sel={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {true});
        list.add(new Object[] {false});
        return list;
    }

    @Test
    public void testPropagateNull() {
        RandomAccessBlock leftBlock = buildAllValuesBlock();
        RandomAccessBlock rightBlock = buildAllValuesBlock();
        RandomAccessBlock outputBlock = newOutputBlock(count);
        if (!withSelection) {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertFalse(((LongBlock) outputBlock).isNull(i));
            }
        } else {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertFalse(((LongBlock) outputBlock).isNull(j));
            }
        }

        // expect all nulls after merge
        leftBlock = buildAllValuesBlock();
        rightBlock = buildAllNullBlock();
        outputBlock = newOutputBlock(count);
        VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
            count, null, false);
        if (!withSelection) {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(((LongBlock) outputBlock).isNull(i));
            }
        } else {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertTrue(((LongBlock) outputBlock).isNull(j));
            }
        }

        // expect all nulls after merge
        leftBlock = buildAllNullBlock();
        rightBlock = buildAllValuesBlock();
        outputBlock = newOutputBlock(count);
        VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
            count, null, false);
        if (!withSelection) {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(((LongBlock) outputBlock).isNull(i));
            }
        } else {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertTrue(((LongBlock) outputBlock).isNull(j));
            }
        }

        // expect all nulls after merge
        leftBlock = buildHalfNullBlock(true);
        rightBlock = buildHalfNullBlock(false);
        outputBlock = newOutputBlock(count);
        VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
            count, null, false);
        if (!withSelection) {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(((LongBlock) outputBlock).isNull(i));
            }
        } else {
            VectorizedExpressionUtils.propagateNullState(outputBlock, leftBlock, rightBlock,
                sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertTrue(((LongBlock) outputBlock).isNull(j));
            }
        }
    }

    @Test
    public void testHandleLongNull() {
        LongBlock longBlock = (LongBlock) buildAllValuesBlock();
        if (!withSelection) {
            VectorizedExpressionUtils.handleLongNullValue(longBlock, count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertFalse(longBlock.isNull(i));
            }
        } else {
            VectorizedExpressionUtils.handleLongNullValue(longBlock, sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertFalse(longBlock.isNull(j));
            }
        }

        longBlock = (LongBlock) buildHalfNullBlock(true);
        if (!withSelection) {
            VectorizedExpressionUtils.handleLongNullValue(longBlock, count, null, false);
            for (int i = 0; i < count; i++) {
                Assert.assertEquals("Failed at position: " + i, i % 2 == 0, longBlock.isNull(i));
            }
        } else {
            VectorizedExpressionUtils.handleLongNullValue(longBlock, sel.length, sel, true);
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals(j % 2 == 0, longBlock.isNull(j));
            }
        }
    }

    @Test
    public void testSelectionIntersect() {
        int sel2Count = count / 2;
        int offset = sel2Count / 2;
        int[] sel2 = new int[sel2Count];
        for (int i = 0; i < sel2Count; i++) {
            if (i < offset) {
                sel2[i] = i * 2;
            } else {
                // fill the right half with matched select positions
                sel2[i] = i * 2 + 1;
            }
        }
        int[] intersection = new int[Math.min(count, sel2Count)];

        if (!withSelection) {
            try {
                VectorizedExpressionUtils.intersect(sel, count, sel2, sel2Count, intersection);
                Assert.fail("Expect failed with null selection");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalArgumentException);
            }
            try {
                VectorizedExpressionUtils.intersect(sel2, sel2Count, sel, count, intersection);
                Assert.fail("Expect failed with null selection");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalArgumentException);
            }
            return;
        }

        int intersectedSize = VectorizedExpressionUtils.intersect(sel, count, sel2, sel2Count, intersection);
        Assert.assertEquals(intersectedSize, sel2Count - offset);
        for (int i = 0; i < intersectedSize; i++) {
            Assert.assertEquals(sel2[i + offset], intersection[i]);
        }
    }

    @Test
    public void testConstantFinder() {
        if (withSelection) {
            testConstantFinderOverflow();
        } else {
            testConstantFinderNotOverflow();
        }
    }

    private void testConstantFinderOverflow() {
        final int MAX_LEVEL = 1 << 10;
        VectorizedExpression[] children = new VectorizedExpression[1];
        VectorizedExpression vectorizedExpression = new AndVectorizedExpression(0, children);
        for (int i = 0; i < MAX_LEVEL; i++) {
            VectorizedExpression[] subChildren = new VectorizedExpression[1];
            VectorizedExpression expr = new AndVectorizedExpression(0, subChildren);
            children[0] = expr;
            children = subChildren;
        }
        children[0] = null;
        boolean isConstant = VectorizedExpressionUtils.isConstantExpression(vectorizedExpression);
        Assert.assertFalse("Expect false when overflow max level", isConstant);
    }

    private void testConstantFinderNotOverflow() {
        final int level = 10;
        VectorizedExpression[] children = new VectorizedExpression[1];
        VectorizedExpression vectorizedExpression = new AndVectorizedExpression(0, children);
        for (int i = 0; i < level; i++) {
            VectorizedExpression[] subChildren = new VectorizedExpression[1];
            VectorizedExpression expr = new AndVectorizedExpression(0, subChildren);
            children[0] = expr;
            children = subChildren;
        }
        children[0] = new LiteralVectorizedExpression(DataTypes.LongType, 100, 0);
        boolean isConstant = VectorizedExpressionUtils.isConstantExpression(vectorizedExpression);
        Assert.assertTrue("Should be constant", isConstant);
    }

    private RandomAccessBlock newOutputBlock(int count) {
        return new LongBlock(DataTypes.LongType, count);
    }

    private RandomAccessBlock buildAllNullBlock() {
        BlockBuilder builder = new LongBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.appendNull();
        }
        return (RandomAccessBlock) builder.build();
    }

    private RandomAccessBlock buildAllValuesBlock() {
        BlockBuilder builder = new LongBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.writeLong(i);
        }
        return (RandomAccessBlock) builder.build();
    }

    private RandomAccessBlock buildHalfNullBlock(boolean firstPart) {
        BlockBuilder builder = new LongBlockBuilder(count);
        if (firstPart) {
            for (int i = 0; i < count / 2; i++) {
                builder.appendNull();
                builder.writeLong(i);
            }
        } else {
            for (int i = 0; i < count / 2; i++) {
                builder.writeLong(i);
                builder.appendNull();
            }
        }

        return (RandomAccessBlock) builder.build();
    }
}
