package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockEncoding;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryBlockBuilder;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class DictBlockTest {
    // build dictionary
    static final int SIZE = 8;
    static final Slice[] DICT = new Slice[SIZE];

    static {
        DICT[0] = Slices.utf8Slice("White");
        DICT[1] = Slices.utf8Slice("Yellow");
        DICT[2] = Slices.utf8Slice("Orange");
        DICT[3] = Slices.utf8Slice("Red");
        DICT[4] = Slices.utf8Slice("Purple");
        DICT[5] = Slices.utf8Slice("Blue");
        DICT[6] = Slices.utf8Slice("Gray");
        DICT[7] = Slices.utf8Slice("Green");
    }

    @Test
    public void testDictionary() {
        BlockDictionary dictionary = new LocalBlockDictionary(DICT);

        // encoding
        SliceOutput sliceOutput = new DynamicSliceOutput(Long.BYTES);
        dictionary.encoding(sliceOutput);

        // decoding
        SliceInput sliceInput = sliceOutput.slice().getInput();
        BlockDictionary decoded = BlockDictionary.decoding(sliceInput);

        Assert.assertTrue(decoded.size() == SIZE);
        for (int id = 0; id < SIZE; id++) {
            Slice slice = decoded.getValue(id);
            Assert.assertTrue(slice.compareTo(DICT[id]) == 0);
        }
    }

    @Test
    public void testSliceBlock() {
        final int length = 2000;
        final int[] dictIds = generateDictIds(length);

        BlockDictionary dictionary = new LocalBlockDictionary(DICT);
        DictionaryBlockBuilder blockBuilder = new DictionaryBlockBuilder(false, new SliceType(), 8);

        blockBuilder.setDictionary(dictionary);

        for (int i = 0; i < dictIds.length; i++) {
            blockBuilder.writeInt(dictIds[i]);
            blockBuilder.appendNull();
        }
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();

        for (int i = 0; i < 2 * dictIds.length; i++) {
            if (i % 2 == 1) {
                Assert.assertTrue(sliceBlock.isNull(i));
            } else {
                Assert.assertTrue(
                    ((Slice) sliceBlock.getObject(i)).compareTo(
                        dictionary.getValue(dictIds[i / 2])
                    ) == 0
                );
            }
        }
    }

    @Test
    public void testSliceEncoding() {
        final int length = 2000;
        final int[] dictIds = generateDictIds(length);

        BlockDictionary dictionary = new LocalBlockDictionary(DICT);
        DictionaryBlockBuilder blockBuilder = new DictionaryBlockBuilder(false, new VarcharType(), 8);

        blockBuilder.setDictionary(dictionary);

        for (int i = 0; i < dictIds.length; i++) {
            blockBuilder.writeInt(dictIds[i]);
            blockBuilder.appendNull();
        }
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();

        // encoding
        SliceBlockEncoding blockEncoding = new SliceBlockEncoding(new VarcharType());
        SliceOutput sliceOutput = new DynamicSliceOutput(8);
        blockEncoding.writeBlock(sliceOutput, sliceBlock);

        // decoding
        SliceInput sliceInput = sliceOutput.slice().getInput();
        Block block = blockEncoding.readBlock(sliceInput);

        for (int i = 0; i < 2 * dictIds.length; i++) {
            if (i % 2 == 1) {
                Assert.assertTrue(block.isNull(i));
                System.out.println("null");
            } else {
                Assert.assertTrue(
                    ((Slice) block.getObject(i)).compareTo(
                        dictionary.getValue(dictIds[i / 2])
                    ) == 0
                );
                System.out.println(((Slice) block.getObject(i)).toStringUtf8());
            }
        }
    }

    @NotNull
    private int[] generateDictIds(int length) {
        Random random = new Random();
        final int[] dictIds = new int[length];
        for (int i = 0; i < dictIds.length; i++) {
            dictIds[i] = random.nextInt(SIZE);
        }
        return dictIds;
    }
}
