package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/**
 * 有/无字典的SlickBlock 写入 有/无字典的SlickBlockBuilder
 */
@RunWith(Parameterized.class)
public class SliceBlockWriteBuilderTest extends SliceBlockTest {

    private static final int COUNT = 10;
    private static final int BLOCK_SIZE = 100;
    private final boolean blockWithDict;
    private final boolean builderWithDict;

    private final boolean notNull;

    public SliceBlockWriteBuilderTest(boolean blockWithDict, boolean builderWithDict, boolean notNull) {
        this.blockWithDict = blockWithDict;
        this.builderWithDict = builderWithDict;
        this.notNull = notNull;
    }

    @Parameterized.Parameters(name = "blockWithDict={0}, builderWithDict={1}, notNull={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        list.add(new Object[] {true, true, true});
        list.add(new Object[] {true, true, false});
        list.add(new Object[] {false, false, true});
        list.add(new Object[] {false, false, false});
        list.add(new Object[] {true, false, true});
        list.add(new Object[] {true, false, false});
        list.add(new Object[] {false, true, true});
        list.add(new Object[] {false, true, false});
        return list;
    }

    @Test
    public void testWriteTo() {
        List<SliceBlock> sliceBlockList = generateBlocks(COUNT);
        SliceBlockBuilder blockBuilder = generateBuilder();

        if (builderWithDict) {
            Assert.assertNotNull(blockBuilder.blockDictionary);
        } else {
            Assert.assertNull(blockBuilder.blockDictionary);
        }

        if (blockWithDict) {
            Assert.assertNotNull(sliceBlockList.get(0).getDictionary());
        } else {
            Assert.assertNull(sliceBlockList.get(0).getDictionary());
        }

        for (SliceBlock sliceBlock : sliceBlockList) {
            for (int pos = 0; pos < sliceBlock.getPositionCount(); pos++) {
                sliceBlock.writePositionTo(pos, blockBuilder);
            }
        }
        SliceBlock resultBlock = (SliceBlock) blockBuilder.build();
        Assert.assertEquals(COUNT * BLOCK_SIZE, resultBlock.getPositionCount());
        int resultPos = 0;
        for (SliceBlock sliceBlock : sliceBlockList) {
            for (int i = 0; i < sliceBlock.getPositionCount(); i++, resultPos++) {
                if (sliceBlock.isNull(i)) {
                    Assert.assertTrue(String.format("Failed at pos=%d", resultPos),
                        resultBlock.isNull(resultPos));
                    continue;
                }
                Assert.assertEquals(String.format("Failed at pos=%d", resultPos),
                    sliceBlock.getRegion(i), resultBlock.getRegion(resultPos));
            }
        }
    }

    @Test
    public void testWriteToWithSelection1() {
        int[] selection = generateFullSelection();
        testWithSelection(selection);
    }

    @Test
    public void testWriteToWithSelection2() {
        int[] selection = generateSelection();
        testWithSelection(selection);
    }

    private void testWithSelection(int[] selection) {
        Preconditions.checkNotNull(selection);

        List<SliceBlock> sliceBlockList = generateBlocks(COUNT);
        SliceBlockBuilder blockBuilder = generateBuilder();

        if (builderWithDict) {
            Assert.assertNotNull(blockBuilder.blockDictionary);
        } else {
            Assert.assertNull(blockBuilder.blockDictionary);
        }

        if (blockWithDict) {
            Assert.assertNotNull(sliceBlockList.get(0).getDictionary());
        } else {
            Assert.assertNull(sliceBlockList.get(0).getDictionary());
        }

        for (SliceBlock sliceBlock : sliceBlockList) {
            sliceBlock.writePositionTo(selection, 0, selection.length, blockBuilder);
        }
        SliceBlock resultBlock = (SliceBlock) blockBuilder.build();
        // 先检查 selection后数量是否一致
        Assert.assertEquals(COUNT * selection.length, resultBlock.getPositionCount());
        int resultPos = 0;
        for (SliceBlock sliceBlock : sliceBlockList) {
            for (int i = 0; i < selection.length; i++, resultPos++) {
                int position = selection[i];
                if (sliceBlock.isNull(position)) {
                    Assert.assertTrue(String.format("Failed at resultPos=%d", resultPos),
                        resultBlock.isNull(resultPos));
                    continue;
                }
                Assert.assertEquals(String.format("Failed at resultPos=%d", resultPos),
                    sliceBlock.getRegion(position), resultBlock.getRegion(resultPos));
            }
        }
    }

    private int[] generateFullSelection() {
        int[] selection = new int[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            selection[i] = i;
        }
        return selection;
    }

    private int[] generateSelection() {
        int positionCount = BLOCK_SIZE / 2;
        int[] selection = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            selection[i] = 2 * i + 1;
        }
        return selection;
    }

    private SliceBlockBuilder generateBuilder() {
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(new SliceType(), COUNT * BLOCK_SIZE, new ExecutionContext(), true);
        if (builderWithDict) {
            blockBuilder.setDictionary(mockDict1());
        }

        return blockBuilder;
    }

    private List<SliceBlock> generateBlocks(int count) {
        List<SliceBlock> sliceBlockList = new ArrayList<>(count);

        BlockDictionary[] dicts = new BlockDictionary[] {mockDict1(), mockDict2(), mockDict3()};

        sliceBlockList.add(generateBlock(dicts[0]));

        for (int i = 1; i < count; i++) {
            // 先添加两个一样的字典
            sliceBlockList.add(generateBlock(dicts[(i + 2) % 3]));
        }
        return sliceBlockList;
    }

    private SliceBlock generateBlock(BlockDictionary dict) {
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(new SliceType(), COUNT * BLOCK_SIZE, new ExecutionContext(), true);
        if (!blockWithDict) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                if (i % 2 == 0 && !notNull) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.writeSlice(Slices.utf8Slice(generate(2)));
                }
            }
            return (SliceBlock) blockBuilder.build();
        }

        int dictSize = dict.size();
        blockBuilder.setDictionary(dict);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            if (i % 2 == 0 && !notNull) {
                blockBuilder.appendNull();
            } else {
                int dictId = i % dictSize;
                Slice dictValue = dict.getValue(dictId);
                blockBuilder.writeSlice(dictValue);
                blockBuilder.values.add(dictId);
            }
        }

        return (SliceBlock) blockBuilder.build();
    }

    private BlockDictionary mockDict1() {
        final int size = 5;
        Slice[] slices = new Slice[size];
        for (int i = 0; i < size; i++) {
            slices[i] = Slices.utf8Slice(ORIGINAL_STRINGS.substring(i, i + 2));
        }
        return new LocalBlockDictionary(slices);
    }

    private BlockDictionary mockDict2() {
        final int size = 8;
        Slice[] slices = new Slice[size];
        for (int i = 0; i < size; i++) {
            slices[i] = Slices.utf8Slice(ORIGINAL_STRINGS.substring(i, i + 2));
        }
        return new LocalBlockDictionary(slices);
    }

    private BlockDictionary mockDict3() {
        final int size = 9;
        Slice[] slices = new Slice[size];
        for (int i = 0; i < size; i++) {
            slices[i] = Slices.utf8Slice(ORIGINAL_STRINGS.substring(i + 2, i + 4));
        }
        return new LocalBlockDictionary(slices);
    }
}
