package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.LikeVarcharColCharConstVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class LikeVarcharColCharConstVectorizedExpressionTest {

    private final int count = 10;
    private final String[] strs = new String[] {
        "abc", "aabbcc", "acb", "bacdabc", "zxcvb",
        "aaabccccdd", "ccbbaacb", "a1b1c1", "a比c", null};
    private ExecutionContext context;

    public LikeVarcharColCharConstVectorizedExpressionTest(boolean ossCompatible) {
        this.context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_OSS_COMPATIBLE", Boolean.toString(ossCompatible));
    }

    @Parameterized.Parameters(name = "ossCompatible={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {true});
        list.add(new Object[] {false});
        return list;
    }

    @Before
    public void before() {
        Assert.assertEquals(count, strs.length);
    }

    @Test
    public void testSliceWithLatin1Match() {
        SliceBlockBuilder sliceBlockBuilder =
            new SliceBlockBuilder(new SliceType(CollationName.LATIN1_BIN), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc%", null);
    }

    @Test
    public void testSliceWithLatin1SubStrMatch() {
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc%", null);
    }

    @Test
    public void testSliceWithLatin1SubStrMatch2() {
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%aaa%", null);
    }

    @Test
    public void testSliceWithLatin1SubStrMatch3() {
        int[] sel = new int[] {0, 1, 2, 3, 4, 5, 8, 9};
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%aaa%", sel);
    }

    @Test
    public void testSliceWithSubStrMatch() {
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, null);

        doTest(inputChunk, expectBlock, "%a比c%", null);
    }

    @Test
    public void testSliceWitWildcardMatch() {
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 1L, 0L, 1L, null);

        doTest(inputChunk, expectBlock, "%a_c%", null);
    }

    @Test
    public void testSliceWithSuffixMatch() {
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc", null);
    }

    @Test
    public void testSliceWithSuffixMatchAndSelection() {
        int[] sel = new int[] {0, 1, 2, 3, 4, 5, 8, 9};
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(new SliceType(), count, context, true);
        for (String s : strs) {
            if (s == null) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(s);
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc", sel);
    }

    @Test
    public void testRef() {
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc%", null);
    }

    @Test
    public void testRefWithSel() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc%", sel);
    }

    @Test
    public void testRef2() {
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc", null);
    }

    @Test
    public void testRefWithSel2() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "%abc", sel);
    }

    @Test
    public void testNull() {
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, null);
    }

    @Test
    public void testNullWithSel() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (String s : strs) {
            if (s == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, String target, int[] sel) {
        LikeVarcharColCharConstVectorizedExpression condition =
            new LikeVarcharColCharConstVectorizedExpression(
                2,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.CharType, target, 1)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.VarcharType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();

        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);

        // Prepare selection array for evaluation.
        if (sel != null) {
            preAllocatedChunk.setBatchSize(sel.length);
            preAllocatedChunk.setSelection(sel);
            preAllocatedChunk.setSelectionInUse(true);
        } else {
            preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
            preAllocatedChunk.setSelection(null);
            preAllocatedChunk.setSelectionInUse(false);
        }

        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                if (((Block) expectBlock).isNull(j)) {
                    Assert.assertTrue("Failed at pos: " + j, ((Block) resultBlock).isNull(j));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                if (((Block) expectBlock).isNull(i)) {
                    Assert.assertTrue("Failed at pos: " + i, ((Block) resultBlock).isNull(i));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }
}
