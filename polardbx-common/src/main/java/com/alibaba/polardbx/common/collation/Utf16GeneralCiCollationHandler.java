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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.nio.ByteBuffer;

import static com.alibaba.polardbx.common.collation.AbstractCollationHandler.codepointOfUTF8;

public class Utf16GeneralCiCollationHandler extends AbstractCollationHandler {
    public Utf16GeneralCiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF16_GENERAL_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.UTF16;
    }

    @Override
    public int compare(Slice binaryStr1, Slice binaryStr2) {
        SliceInput sliceInput1 = binaryStr1.getInput();
        SliceInput sliceInput2 = binaryStr2.getInput();
        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            long pos1 = sliceInput1.position();
            long pos2 = sliceInput2.position();

            int codepoint1 = codepointOfUTF16(sliceInput1);
            int codepoint2 = codepointOfUTF16(sliceInput2);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                sliceInput1.setPosition(pos1);
                sliceInput2.setPosition(pos2);
                return binaryCompare(sliceInput1, sliceInput2);
            }

            int weight1 = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint1);
            int weight2 = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint2);
            if (weight1 != weight2) {
                return weight1 - weight2;
            }
        }
        return sliceInput1.available() - sliceInput2.available();
    }

    @Override
    public int compareSp(Slice binaryStr1, Slice binaryStr2) {
        SliceInput sliceInput1 = binaryStr1.getInput();
        SliceInput sliceInput2 = binaryStr2.getInput();
        while (sliceInput1.isReadable() && sliceInput2.isReadable()) {
            long pos1 = sliceInput1.position();
            long pos2 = sliceInput2.position();

            int codepoint1 = codepointOfUTF16(sliceInput1);
            int codepoint2 = codepointOfUTF16(sliceInput2);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                sliceInput1.setPosition(pos1);
                sliceInput2.setPosition(pos2);
                return binaryCompare(sliceInput1, sliceInput2);
            }

            int weight1 = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint1);
            int weight2 = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint2);
            if (weight1 != weight2) {
                return weight1 > weight2 ? 1 : -1;
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
                int codepoint = codepointOfUTF16(sliceInput1);
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
            int codepoint = codepointOfUTF16(sliceInput);
            if (codepoint == INVALID_CODE) {
                break;
            }

            int weight = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint);

            tmp1 ^= (((tmp1 & 63) + tmp2) * (weight & 0xFF)) + (tmp1 << 8);
            tmp2 += 3;

            tmp1 ^= (((tmp1 & 63) + tmp2) * ((weight >> 8) & 0xFF)) + (tmp1 << 8);
            tmp2 += 3;
        }

        return (int) tmp1;
    }

    public static int codepointOfUTF16(SliceInput buff) {
        if (buff.available() < 2) {

            return INVALID_CODE;
        }

        byte c1 = buff.readByte();
        byte c2 = buff.readByte();

        if (isUTF16HighHead(c1)) {
            if (buff.available() < 2) {

                return INVALID_CODE;
            }
            byte c3 = buff.readByte();
            byte c4 = buff.readByte();
            if (!isUTF16LowHead(c3)) {

                return INVALID_CODE;
            }
            return getUTF16Unicode4(c1, c2, c3, c4);
        }

        if (isUTF16LowHead(c1)) {

            return INVALID_CODE;
        }
        return getUTF16Unicode2(c1, c2);
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        SliceInput sliceInput = str.getInput();
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        while (dst.hasRemaining() && sliceInput.isReadable()) {
            int codepoint = codepointOfUTF16(sliceInput);
            if (codepoint == INVALID_CODE) {
                break;
            }

            int weight = Utf8mb4GeneralCiCollationHandler.getWeight(codepoint);
            dst.put((byte) (weight >> 8));
            if (dst.hasRemaining()) {
                dst.put((byte) (weight & 0xFF));
            }
        }
        int effectiveLen = dst.position();

        if (dst.hasRemaining()) {
            while (dst.remaining() >= 4) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }
            if (dst.remaining() >= 2) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }
            if (dst.hasRemaining()) {
                dst.put((byte) 0x00);
            }
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target,
            sliceInput -> codepointOfUTF16(sliceInput));
    }

    public static boolean isUTF16HighHead(byte b) {
        return (Byte.toUnsignedInt(b) & 0xFC) == 0xD8;
    }

    public static boolean isUTF16LowHead(byte b) {
        return (Byte.toUnsignedInt(b) & 0xFC) == 0xDC;
    }

    public static boolean isUTF16Surrogate(byte b) {
        return (Byte.toUnsignedInt(b) & 0xF800) == 0xD800;
    }

    public static int getUTF16Unicode2(byte a, byte b) {
        return (Byte.toUnsignedInt(a) << 8) + Byte.toUnsignedInt(b);
    }

    public static int getUTF16Unicode4(byte a, byte b, byte c, byte d) {
        return ((Byte.toUnsignedInt(a) & 3) << 18) + (Byte.toUnsignedInt(b) << 10) + ((Byte.toUnsignedInt(c) & 3) << 8)
            + Byte.toUnsignedInt(d) + 0x10000;
    }
}
