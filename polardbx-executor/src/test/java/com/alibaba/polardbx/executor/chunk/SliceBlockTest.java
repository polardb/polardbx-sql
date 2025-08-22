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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SliceBlockTest extends BaseBlockTest {
    protected static final String ORIGINAL_STRINGS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890中文字符串测试";
    protected static final Random RANDOM = new Random();
    protected static final Charset CHARSET = StandardCharsets.UTF_8;

    @Test
    public void test() {
        List<String> checkList = new ArrayList<>();

        SliceType sliceType = new SliceType(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(sliceType, 1024, new ExecutionContext(), true);

        // block builder writing
        for (int i = 0; i < CHUNK_SIZE; i++) {
            int length = RANDOM.nextInt(20);
            if (length > 10) {
                // append null
                sliceBlockBuilder.writeObject(null);
                checkList.add(null);
            } else if (length == 10) {
                // append empty string
                sliceBlockBuilder.writeObject(Slices.utf8Slice(""));
                checkList.add("");
            } else {
                // append string
                String s = generate(length);
                sliceBlockBuilder.writeObject(Slices.utf8Slice(s));
                checkList.add(s);
            }
        }

        MemoryCountable.checkDeviation(sliceBlockBuilder, .05d, true);

        // test read/write consistency
        SliceBlock block = (SliceBlock) sliceBlockBuilder.build();
        MemoryCountable.checkDeviation(block, .05d, true);
        for (int i = 0; i < CHUNK_SIZE; i++) {
            Slice slice = (Slice) block.getObject(i);
            String s = checkList.get(i);
            Assert.assertTrue(
                s == null ? slice == null : slice.toString(CHARSET).equals(s)
            );
        }

        // test serialization / deserialization
        SliceBlockEncoding sliceBlockEncoding =
            new SliceBlockEncoding(new VarcharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI));

        SliceOutput sliceOutput = new DynamicSliceOutput(CHUNK_SIZE);
        sliceBlockEncoding.writeBlock(sliceOutput, block);

        Slice serializedBlock = sliceOutput.slice();
        SliceBlock sliceBlock = (SliceBlock) sliceBlockEncoding.readBlock(serializedBlock.getInput());
        MemoryCountable.checkDeviation(sliceBlock, .05d, true);

        for (int i = 0; i < CHUNK_SIZE; i++) {
            Assert.assertTrue(sliceBlock.equals(i, block, i));
            Assert.assertEquals(sliceBlock.checksum(i), block.checksum(i));
            Assert.assertEquals(sliceBlock.hashCode(i), block.hashCode(i));
            Assert.assertEquals(sliceBlock.hashCodeUseXxhash(i), block.hashCodeUseXxhash(i));
        }

    }

    @Test
    public void testDict() {
        final int count = 10;
        boolean[] nulls = new boolean[count];
        Slice[] dict = new Slice[] {Slices.utf8Slice("a"), Slices.utf8Slice("b"), Slices.utf8Slice("c")};
        int[] dictIds = new int[] {0, 1, 2, 0, 1, 2, 0, 1, 2, 0};
        LocalBlockDictionary dictionary1 = new LocalBlockDictionary(dict);
        SliceBlock sliceBlock1 = new SliceBlock(new SliceType(), 0, count,
            nulls, dictionary1, dictIds, false);
        MemoryCountable.checkDeviation(sliceBlock1, .05d, true);

        for (int i = 0; i < dictIds.length; i++) {
            Assert.assertEquals(1, sliceBlock1.equals(i, dict[dictIds[i]]));
        }
    }

    @Test
    public void testCopyAllValues() {
        final boolean compatible = true;
        SliceBlock sourceBlock = getNonNullBlock();
        SliceBlock resultBlock = SliceBlock.from(sourceBlock, CHUNK_SIZE, null, compatible, false);
        MemoryCountable.checkDeviation(sourceBlock, .05d, true);
        MemoryCountable.checkDeviation(resultBlock, .05d, true);
        for (int i = 0; i < CHUNK_SIZE; i++) {
            Assert.assertTrue("Failed in copying without selection",
                resultBlock.equals(i, sourceBlock, i));
        }

        // test copy with selection, useSelection=false
        int[] selection = new int[CHUNK_SIZE / 2];
        for (int i = 0; i < selection.length; i++) {
            selection[i] = i * 2;
        }
        resultBlock = SliceBlock.from(sourceBlock, selection.length, selection, compatible, false);
        for (int i = 0; i < resultBlock.positionCount; i++) {
            Assert.assertTrue("Failed in copying with selection and useSelection=false",
                resultBlock.equals(i, sourceBlock, selection[i]));
        }

        // test copy with selection, useSelection=true
        resultBlock = SliceBlock.from(sourceBlock, selection.length, selection, compatible, true);
        for (int i = 0; i < resultBlock.positionCount; i++) {
            Assert.assertTrue("Failed in copying with selection and useSelection=false",
                resultBlock.equals(i, sourceBlock, selection[i]));
        }
    }

    @Test
    public void testCopyAllNulls() {
        final boolean compatible = true;
        SliceBlock sourceBlock = getAllNullBlock();
        SliceBlock resultBlock = SliceBlock.from(sourceBlock, CHUNK_SIZE, null, compatible, false);
        MemoryCountable.checkDeviation(sourceBlock, .05d, true);
        MemoryCountable.checkDeviation(resultBlock, .05d, true);
        for (int i = 0; i < CHUNK_SIZE; i++) {
            Assert.assertTrue("Failed in copying without selection",
                resultBlock.equals(i, sourceBlock, i));
        }

        // test copy with selection, useSelection=false
        int[] selection = new int[CHUNK_SIZE / 2];
        for (int i = 0; i < selection.length; i++) {
            selection[i] = i * 2;
        }
        resultBlock = SliceBlock.from(sourceBlock, selection.length, selection, compatible, false);
        for (int i = 0; i < resultBlock.positionCount; i++) {
            Assert.assertTrue("Failed in copying with selection and useSelection=false",
                resultBlock.equals(i, sourceBlock, selection[i]));
        }

        // test copy with selection, useSelection=true
        resultBlock = SliceBlock.from(sourceBlock, selection.length, selection, compatible, true);
        for (int i = 0; i < resultBlock.positionCount; i++) {
            Assert.assertTrue("Failed in copying with selection and useSelection=false",
                resultBlock.equals(i, sourceBlock, selection[i]));
        }
    }

    private SliceBlock getNonNullBlock() {
        SliceType sliceType = new SliceType(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        SliceBlockBuilder sliceBlockBuilder =
            new SliceBlockBuilder(sliceType, CHUNK_SIZE, new ExecutionContext(), true);

        // block builder writing
        for (int i = 0; i < CHUNK_SIZE; i++) {
            int length = RANDOM.nextInt(20);
            if (length > 10) {
                // append null
                sliceBlockBuilder.writeObject(null);
            } else if (length == 10) {
                // append empty string
                sliceBlockBuilder.writeObject(Slices.utf8Slice(""));
            } else {
                // append string
                String s = generate(length);
                sliceBlockBuilder.writeObject(Slices.utf8Slice(s));
            }
        }
        MemoryCountable.checkDeviation(sliceBlockBuilder, .05d, true);
        return (SliceBlock) sliceBlockBuilder.build();
    }

    private SliceBlock getAllNullBlock() {
        SliceType sliceType = new SliceType(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        SliceBlockBuilder sliceBlockBuilder =
            new SliceBlockBuilder(sliceType, CHUNK_SIZE, new ExecutionContext(), true);

        // block builder writing
        for (int i = 0; i < CHUNK_SIZE; i++) {
            sliceBlockBuilder.appendNull();
        }
        MemoryCountable.checkDeviation(sliceBlockBuilder, .05d, true);
        return (SliceBlock) sliceBlockBuilder.build();
    }

    protected String generate(int length) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length) { // length of the random string.
            int index = (int) (RANDOM.nextFloat() * ORIGINAL_STRINGS.length());
            String subStr = ORIGINAL_STRINGS.substring(index, index + 1);
            sb.append(subStr);
        }
        return sb.toString();
    }
}
