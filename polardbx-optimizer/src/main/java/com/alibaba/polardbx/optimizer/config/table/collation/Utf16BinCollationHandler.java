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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.nio.ByteBuffer;

public class Utf16BinCollationHandler extends AbstractCollationHandler {
    public Utf16BinCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF16_BIN;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.UTF16;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        SliceInput leftInput = str1.getInput();
        SliceInput rightInput = str2.getInput();
        while (leftInput.isReadable() && rightInput.isReadable()) {
            long pos1 = leftInput.position();
            long pos2 = rightInput.position();

            int codepoint1 = Utf16GeneralCiCollationHandler.codepointOfUTF16(leftInput);
            int codepoint2 = Utf16GeneralCiCollationHandler.codepointOfUTF16(rightInput);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                leftInput.setPosition(pos1);
                rightInput.setPosition(pos2);
                return binaryCompare(leftInput, rightInput);
            }

            if (codepoint1 != codepoint2) {
                return codepoint1 - codepoint2;
            }
        }
        return leftInput.available() - rightInput.available();
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        SliceInput sliceInput1 = str1.getInput();
        SliceInput sliceInput2 = str2.getInput();
        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            long pos1 = sliceInput1.position();
            long pos2 = sliceInput2.position();

            int codepoint1 = Utf16GeneralCiCollationHandler.codepointOfUTF16(sliceInput1);
            int codepoint2 = Utf16GeneralCiCollationHandler.codepointOfUTF16(sliceInput2);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                sliceInput1.setPosition(pos1);
                sliceInput2.setPosition(pos2);
                return binaryCompare(sliceInput1, sliceInput2);
            }

            if (codepoint1 != codepoint2) {
                return codepoint1 - codepoint2;
            }
        }

        int len1 = sliceInput1.available();
        int len2 = sliceInput2.available();
        int res = 0;

        if (len1 != len2) {
            int swap = 1;
            if (DIFF_IF_ONLY_END_SPACE_DIFFERENCE) {
                res = 1;
            }
            if (len1 < len2) {
                sliceInput1 = sliceInput2;
                swap = -1;
                res = -res;
            }
            while (sliceInput1.isReadable()) {
                int codepoint = Utf16GeneralCiCollationHandler.codepointOfUTF16(sliceInput1);
                if (codepoint == INVALID_CODE) {
                    return 0;
                }
                if (codepoint != ' ') {
                    return (codepoint < ' ') ? -swap : swap;
                }
            }
        }
        return res;
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

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        SliceInput sliceInput = str.getInput();
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        while (dst.hasRemaining() && sliceInput.isReadable()) {
            int codepoint = Utf16GeneralCiCollationHandler.codepointOfUTF16(sliceInput);
            if (codepoint == INVALID_CODE) {
                break;
            }

            dst.put((byte) (codepoint >> 16));
            if (dst.hasRemaining()) {
                dst.put((byte) ((codepoint >> 8) & 0xFF));
                if (dst.hasRemaining()) {
                    dst.put((byte) (codepoint & 0xFF));
                }
            }
        }
        int effectiveLen = dst.position();

        if (dst.hasRemaining()) {
            while (dst.remaining() >= 3) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }
            if (dst.hasRemaining()) {
                dst.put((byte) 0x00);
                if (dst.hasRemaining()) {
                    dst.put((byte) 0x00);
                    if (dst.hasRemaining()) {
                        dst.put((byte) 0x20);
                    }
                }
            }
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target,
            sliceInput -> Utf16GeneralCiCollationHandler.codepointOfUTF16(sliceInput));
    }
}
