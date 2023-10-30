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

package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetConfigUtils;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.nio.ByteBuffer;

public class SimpleCollationHandler extends AbstractCollationHandler {
    private int[] sortOrder;
    private int spaceWeight;
    private CollationName collationName;

    public SimpleCollationHandler(CharsetHandler charsetHandler, CollationName collation) {
        super(charsetHandler);
        this.collationName = collation;
        this.sortOrder = CharsetConfigUtils.sortOrderOf(collation);
        this.spaceWeight = sortOrder[0x20];
    }

    @Override
    public CollationName getName() {
        return collationName;
    }

    @Override
    public int compare(Slice str1, Slice str2) {

        SliceInput sliceInput1 = str1.getInput();
        SliceInput sliceInput2 = str2.getInput();

        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            int b1 = codeOfSimpleFromUTF8(sliceInput1);
            int b2 = codeOfSimpleFromUTF8(sliceInput2);
            if (sortOrder[b1] != sortOrder[b2]) {
                return b1 - b2;
            }
        }

        return sliceInput1.available() > sliceInput2.available() ? 1
            : (sliceInput1.available() < sliceInput2.available() ? -1 : 0);
    }

    public static int codeOfSimpleFromUTF8(SliceInput buff) {
        byte c1 = buff.readByte();
        int code1 = Byte.toUnsignedInt(c1);

        if (code1 < 0x80) {
            return code1;
        } else if (code1 < 0xc2) {
            return INVALID_CODE;
        } else if (code1 < 0xe0) {
            if (buff.available() < 1) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            if (!isContinuationByte(c2)) {
                return INVALID_CODE;
            }
            return ((code1 & 0x1f) << 6) | (Byte.toUnsignedInt(c2) ^ 0x80);
        }
        return INVALID_CODE;
    }

    public static boolean isContinuationByte(byte c) {
        return (Byte.toUnsignedInt(c) ^ 0x80) < 0x40;
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {

        SliceInput sliceInput1 = str1.getInput();
        SliceInput sliceInput2 = str2.getInput();

        int minLen = Math.min(str1.length(), str2.length());
        int len1 = str1.length();
        int len2 = str2.length();

        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            int weight1 = sortOrder[codeOfSimpleFromUTF8(sliceInput1)];
            int weight2 = sortOrder[codeOfSimpleFromUTF8(sliceInput2)];
            if (weight1 != weight2) {
                return weight1 - weight2;
            }
        }
        int res = 0;
        if (len1 != len2) {
            int swap = 1;
            if (DIFF_IF_ONLY_END_SPACE_DIFFERENCE) {
                res = 1;
            }
            if (len1 < len2) {
                sliceInput1 = sliceInput2;
                len1 = len2;
                swap = -1;
                res = -res;
            }

            int rest = len1 - minLen;
            while (rest > 0) {
                int weight = sortOrder[codeOfSimpleFromUTF8(sliceInput1)];
                --rest;
                if (weight != spaceWeight) {
                    return weight < spaceWeight ? -swap : swap;
                }
            }

        }
        return res;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {

        ByteBuffer dst = ByteBuffer.allocate(maxLength);
        SliceInput sliceInput = str.getInput();

        while (sliceInput.isReadable() && dst.hasRemaining()) {
            int codepoint = codeOfSimpleFromUTF8(sliceInput);
            int weight = sortOrder[codepoint];
            dst.put((byte) (weight & 0xff));
        }
        int effectiveLen = dst.position();

        while (dst.hasRemaining()) {
            dst.put((byte) 0x20);
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int hashcode(Slice str) {

        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;

        int len = str.length();
        while (len >= 1 && str.getByte(len - 1) == 0x20) {
            len--;
        }

        SliceInput sliceInput = str.slice(0, len).getInput();
        while (sliceInput.isReadable()) {
            int codepoint = codeOfSimpleFromUTF8(sliceInput);
            int weight = sortOrder[codepoint];
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * weight) + (tmp1 << 8);
            tmp2 += 3;
        }

        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {

        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }
        long tmp1 = Integer.toUnsignedLong((int) numbers[0]);
        long tmp2 = Integer.toUnsignedLong((int) numbers[1]);

        for (int i = begin; i < end; i++) {
            int weight = sortOrder[Byte.toUnsignedInt(bytes[i])];
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * weight) + (tmp1 << 8);
            tmp2 += 3;
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    @Override
    public int instr(Slice source, Slice target) {

        return instrForSingleByte(source, target, b -> sortOrder[Byte.toUnsignedInt(b)]);
    }
}
