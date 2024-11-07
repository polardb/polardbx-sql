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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.JvmUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class Latin1BinCollationHandler extends AbstractCollationHandler {

    private static final Logger logger = LoggerFactory.getLogger(Latin1BinCollationHandler.class);
    private static boolean useNativeMethod = true;
    private static MethodHandle latin1IndexOfHandle = null;

    static {
        try {
            Class<?> clazz = Class.forName("java.lang.StringLatin1");
            MethodHandles.Lookup lookup = JvmUtils.trustedLookup(String.class);
            MethodType methodType = MethodType.methodType(int.class, byte[].class, int.class, byte[].class,
                int.class, int.class);
            latin1IndexOfHandle = lookup.findStatic(clazz, "indexOf", methodType);
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    public Latin1BinCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.LATIN1_BIN;
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
    public int instr(Slice source, Slice target) {
        return instrForSingleByte(source, target, b -> Byte.toUnsignedInt(b));
    }

    @Override
    public boolean wildCompare(Slice slice, Slice wildCard) {
        return doWildCompare(slice, 0, wildCard, 0) == 0;
    }

    public boolean contains(Slice slice, byte[] wildCard) {
        if (wildCard == null || wildCard.length == 0) {
            // like '%%' always match
            return true;
        }

        if (latin1IndexOfHandle != null && useNativeMethod) {
            Object base = slice.getBase();
            if (base instanceof byte[]) {
                try {
                    int offset = (int) (slice.getAddress() - ARRAY_BYTE_BASE_OFFSET);
                    // should follow the rules inside the intrinsic method
                    int result = (int) latin1IndexOfHandle.invokeExact((byte[]) base, slice.length() + offset,
                        wildCard, wildCard.length, offset);
                    return result >= 0;
                } catch (Throwable t) {
                    // unexpected exception occurs in invocation
                    // set useNativeMethod to false to avoid repeated errors
                    logger.error(t.getMessage(), t);
                    useNativeMethod = false;
                }
            }
        }

        byte first = wildCard[0];
        int max = slice.length() - wildCard.length;
        for (int i = 0; i <= max; i++) {
            if (slice.getByteUnchecked(i) != first) {
                while (++i <= max && slice.getByteUnchecked(i) != first) {
                    // move on until first byte matches
                }
            }
            if (i <= max) {
                int j = i + 1;
                int end = j + wildCard.length - 1;
                for (int k = 1; j < end && slice.getByteUnchecked(j) == wildCard[k]; j++, k++) {
                    // stop when not matching
                }
                if (j == end) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean startsWith(Slice slice, byte[] wildCard) {
        int len = slice.length();
        if (len < wildCard.length) {
            return false;
        }
        if (wildCard.length == 0) {
            return true;
        }
        Object base = slice.getBase();
        if (base instanceof byte[]) {
            byte[] bytes = (byte[]) base;
            int offset = (int) (slice.getAddress() - ARRAY_BYTE_BASE_OFFSET);
            for (int i = 0; i < wildCard.length; i++) {
                // be careful with this unchecked behavior
                if (bytes[i + offset] != wildCard[i]) {
                    return false;
                }
            }
        } else {
            for (int i = 0; i < wildCard.length; i++) {
                if (slice.getByteUnchecked(i) != wildCard[i]) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean endsWith(Slice slice, byte[] wildCard) {
        int len = slice.length();
        if (len < wildCard.length) {
            return false;
        }
        if (wildCard.length == 0) {
            return true;
        }
        Object base = slice.getBase();
        if (base instanceof byte[]) {
            byte[] bytes = (byte[]) base;
            int offset = (int) (slice.getAddress() - ARRAY_BYTE_BASE_OFFSET);
            for (int i = wildCard.length - 1, j = len - 1; i >= 0; i--, j--) {
                // be careful with this unchecked behavior
                if (bytes[j + offset] != wildCard[i]) {
                    return false;
                }
            }
        } else {
            for (int i = wildCard.length - 1, j = len - 1; i >= 0; i--, j--) {
                if (slice.getByteUnchecked(j) != wildCard[i]) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return -1代表不匹配，0代表匹配，1代表继续
     */
    private int doWildCompare(Slice str, int strIdx, Slice wildstr, int wildstrIdx) {
        int result = -1;
        final int strEnd = str.length();
        final int wildstrEnd = wildstr.length();
        byte wildByte;
        while (wildstrIdx != wildstrEnd) {
            wildByte = wildstr.getByteUnchecked(wildstrIdx);
            while (wildByte != WILD_MANY && wildByte != WILD_ONE) {
                if (wildByte == ESCAPE &&
                    (wildstrIdx + 1) != wildstrEnd) {
                    wildstrIdx++;
                }

                if (strIdx == strEnd || wildstr.getByte(wildstrIdx++) != str.getByte(strIdx++)) {
                    return 1;
                }
                if (wildstrIdx == wildstrEnd) {
                    // 匹配完成
                    return (strIdx != strEnd) ? 1 : 0;
                }
                result = 1;
                wildByte = wildstr.getByteUnchecked(wildstrIdx);
            }

            if (wildstr.getByte(wildstrIdx) == WILD_ONE) {
                do {
                    if (strIdx == strEnd) {
                        return result;
                    }
                    strIdx++;
                } while (++wildstrIdx < wildstrEnd && wildstr.getByte(wildstrIdx) == WILD_ONE);

                if (wildstrIdx == wildstrEnd) {
                    break;
                }
            }

            if (wildstr.getByte(wildstrIdx) == WILD_MANY) {
                byte cmp;
                wildstrIdx++;
                for (; wildstrIdx != wildstrEnd; wildstrIdx++) {
                    if (wildstr.getByte(wildstrIdx) == WILD_MANY) {
                        continue;
                    }
                    if (wildstr.getByte(wildstrIdx) == WILD_ONE) {
                        if (strIdx == strEnd) {
                            return -1;
                        }
                        strIdx++;
                        continue;
                    }
                    break;
                }
                if (wildstrIdx == wildstrEnd) {
                    return 0;
                }
                if (strIdx == strEnd) {
                    return -1;
                }
                if ((cmp = wildstr.getByte(wildstrIdx)) == ESCAPE && wildstrIdx + 1 != wildstrEnd) {
                    cmp = wildstr.getByte(++wildstrIdx);
                }
                wildstrIdx++;
                do {
                    while (strIdx != strEnd && str.getByte(strIdx) != cmp) {
                        strIdx++;
                    }
                    if (strIdx++ == strEnd) {
                        return -1;
                    }
                    int tmp = doWildCompare(str, strIdx, wildstr, wildstrIdx);
                    if (tmp <= 0) {
                        return tmp;
                    }
                } while (strIdx != strEnd);

                return -1;
            }
        }

        return (strIdx != strEnd) ? 1 : 0;
    }
}
