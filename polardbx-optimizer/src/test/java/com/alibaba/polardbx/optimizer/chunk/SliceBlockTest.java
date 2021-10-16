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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SliceBlockTest extends BaseBlockTest {
    private static final String ORIGINAL_STRINGS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890中文字符串测试";
    private static final Random RANDOM = new Random();
    private static final Charset CHARSET = Charset.forName("UTF-8");

    @Test
    public void test() {
        List<String> checkList = new ArrayList<>();

        SliceType sliceType = new SliceType(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(sliceType, 1024);

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

        // test read/write consistency
        SliceBlock block = (SliceBlock) sliceBlockBuilder.build();
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

        for (int i = 0; i < CHUNK_SIZE; i++) {
            Assert.assertTrue(sliceBlock.equals(i, block, i));
        }

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
