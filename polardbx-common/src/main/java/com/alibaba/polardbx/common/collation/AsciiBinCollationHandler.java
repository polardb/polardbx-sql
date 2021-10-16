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

package com.alibaba.polardbx.common.collation;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.nio.ByteBuffer;

public class AsciiBinCollationHandler extends AbstractCollationHandler {
    public AsciiBinCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.ASCII_BIN;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        SliceInput leftInput = str1.getInput();
        SliceInput rightInput = str2.getInput();
        while (leftInput.isReadable() && rightInput.isReadable()) {
            byte b1 = leftInput.readByte();
            byte b2 = rightInput.readByte();
            if (b1 != b2) {
                return Byte.toUnsignedInt(b1) - Byte.toUnsignedInt(b2);
            }
        }
        return leftInput.available() - rightInput.available();
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        SliceInput sliceInput1 = str1.getInput();
        SliceInput sliceInput2 = str2.getInput();

        int minLen = Math.min(str1.length(), str2.length());
        int len1 = str1.length();
        int len2 = str2.length();

        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            int b1 = Byte.toUnsignedInt(sliceInput1.readByte());
            int b2 = Byte.toUnsignedInt(sliceInput2.readByte());
            if (b1 != b2) {
                return b1 - b2;
            }
        }
        int res = 0;
        if (len1 != len2) {
            int swap = 1;
            if (DIFF_IF_ONLY_END_SPACE_DIFFERENCE) {
                res = 1;
            }
            if (len1 < len2) {
                len1 = len2;
                sliceInput1 = sliceInput2;
                swap = -1;
                res = -res;
            }

            int rest = len1 - minLen;
            while (rest > 0) {
                int b = Byte.toUnsignedInt(sliceInput1.readByte());
                rest--;
                if (b != 0x20) {
                    return b < 0x20 ? -swap : swap;
                }
            }

        }
        return res;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);
        SliceInput sliceInput = str.getInput();
        while (dst.hasRemaining() && sliceInput.isReadable()) {
            dst.put(sliceInput.readByte());
        }
        int effectiveLen = dst.position();

        while (dst.hasRemaining()) {
            dst.put((byte) 0x20);
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForSingleByte(source, target, b -> Byte.toUnsignedInt(b));
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
            byte b = sliceInput.readByte();
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * Byte.toUnsignedInt(b)) + (tmp1 << 8);
            tmp2 += 3;
        }
        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {

        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }
        long tmp1 = numbers[0] & 0xffffffffL;
        long tmp2 = numbers[1] & 0xffffffffL;

        for (int i = begin; i < end; i++) {
            byte b = bytes[i];
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * (((int) b) & 0xff)) + (tmp1 << 8);
            tmp2 += 3;
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }
}
