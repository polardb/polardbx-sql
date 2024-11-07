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
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RunWith(Parameterized.class)
public class LikeVarcharColCharConstVectorizedExpressionTest {

    private final boolean ossCompatible;
    private final int count = 10;
    /**
     * two types of strings arrays with different cardinality
     */
    private final String[] strs = new String[] {
        "abc", "aabbcc", "acb", "bacdabc", "zxcvb",
        "aaabccccdd", "ccbbaacb", "a1b1c1", "a比c", null};

    private final String[] dictStrs = new String[] {
        "abc", "aabbcc", "abc", "aabbcc", "abc",
        "aabbcc", "ccbbaacb", "abc", "a比c", null};
    private final ExecutionContext context;
    private final LocalBlockDictionary dictionary1;
    private final int[] dictIds1;
    private final LocalBlockDictionary dictionary2;
    private final int[] dictIds2;
    private final boolean[] nulls;

    public LikeVarcharColCharConstVectorizedExpressionTest(boolean ossCompatible) {
        this.ossCompatible = ossCompatible;
        this.context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_OSS_COMPATIBLE", Boolean.toString(ossCompatible));

        this.dictIds1 = new int[count];
        this.dictIds2 = new int[count];
        this.nulls = new boolean[count];

        BiMap<String, Integer> map1 = getDictMap(strs);
        BiMap<String, Integer> map2 = getDictMap(dictStrs);
        List<Slice> slices1 = new ArrayList<>();
        List<Slice> slices2 = new ArrayList<>();
        for (int i = 0; i < map1.size(); i++) {
            slices1.add(Slices.utf8Slice(map1.inverse().get(i)));
        }
        for (int i = 0; i < map2.size(); i++) {
            slices2.add(Slices.utf8Slice(map2.inverse().get(i)));
        }
        for (int i = 0; i < count; i++) {
            if (strs[i] != null) {
                dictIds1[i] = map1.get(strs[i]);
                nulls[i] = false;
            } else {
                dictIds1[i] = -1;
                nulls[i] = true;
            }
            if (dictStrs[i] != null) {
                dictIds2[i] = map2.get(dictStrs[i]);
                nulls[i] = false;
            } else {
                dictIds2[i] = -1;
                nulls[i] = true;
            }
        }
        this.dictionary1 = new LocalBlockDictionary(slices1.toArray(new Slice[0]));
        this.dictionary2 = new LocalBlockDictionary(slices2.toArray(new Slice[0]));
    }

    private static BiMap<String, Integer> getDictMap(String[] strs) {
        BiMap<String, Integer> dictMap = HashBiMap.create();
        int id = 0;
        for (String str : strs) {
            if (str != null && !dictMap.containsKey(str)) {
                dictMap.put(str, id++);
            }
        }
        return dictMap;
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
    public void testDictWithLatin1Match() {
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(CollationName.LATIN1_BIN), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "%abc%", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(CollationName.LATIN1_BIN), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 0L, 1L, 0L, null);

        doTest(inputChunk2, expectBlock2, "%abc%", null);
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
    public void testDictWithLatin1SubStrMatch3() {
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(CollationName.LATIN1_BIN), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(0L, 1L, 0L, 0L, 0L, 1L, 1L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "%aa%", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(CollationName.LATIN1_BIN), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(0L, 1L, 0L, 1L, 0L, 1L, 1L, 0L, 0L, null);

        doTest(inputChunk2, expectBlock2, "%aa%", null);
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
    public void testSliceWithWildcardMatch() {
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
    public void testDictWithWildcardMatch() {
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 1L, 0L, 1L, 1L, 0L, 1L, null);

        doTest(inputChunk1, expectBlock1, "%a_c%", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 1L, 1L, 1L, null);

        doTest(inputChunk2, expectBlock2, "%a_c%", null);
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
    public void testDictWithSuffixMatch() {
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "%abc", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 0L, 1L, 0L, null);

        doTest(inputChunk2, expectBlock2, "%abc", null);
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
    public void testDictWithSuffixMatchAndSelection() {
        int[] sel = new int[] {0, 1, 2, 3, 4, 5, 8, 9};
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "%abc", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk2, expectBlock2, "%abc", sel);
    }

    @Test
    public void testSliceWithPrefixMatch() {
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

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "abc%", null);
    }

    @Test
    public void testDictWithPrefixMatch() {
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "abc%", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 0L, 1L, 0L, null);

        doTest(inputChunk2, expectBlock2, "abc%", null);
    }

    @Test
    public void testSliceWithPrefixMatchAndSelection() {
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

        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "abc%", sel);
    }

    @Test
    public void testDictWithPrefixMatchAndSelection() {
        int[] sel = new int[] {0, 1, 2, 3, 4, 5, 8, 9};
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds1, ossCompatible);
        Chunk inputChunk1 = new Chunk(sliceBlock1.getPositionCount(), sliceBlock1);

        LongBlock expectBlock1 = LongBlock.of(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk1, expectBlock1, "abc%", null);

        SliceBlock sliceBlock2 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary2, dictIds2, ossCompatible);
        Chunk inputChunk2 = new Chunk(sliceBlock2.getPositionCount(), sliceBlock2);

        LongBlock expectBlock2 = LongBlock.of(1L, 0L, 1L, 0L, 1L, 0L, 0L, 1L, 0L, null);

        doTest(inputChunk2, expectBlock2, "abc%", sel);
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
