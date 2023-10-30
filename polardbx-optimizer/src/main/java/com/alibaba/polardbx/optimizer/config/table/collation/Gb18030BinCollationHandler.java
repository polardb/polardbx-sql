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
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;

import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.getGB18030ChsToCode;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.getMbLength;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven2;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven4;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbOdd;

public class Gb18030BinCollationHandler extends AbstractCollationHandler {
    public Gb18030BinCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_BIN;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030;
    }

    @Override
    public int compare(Slice binaryStr1, Slice binaryStr2) {
        int len1 = binaryStr1.length();
        int len2 = binaryStr2.length();
        int pos1 = 0, pos2 = 0;
        while (pos1 < len1 && pos2 < len2) {
            int mbLen1 = getMbLength(binaryStr1.slice(pos1, len1 - pos1));
            int mbLen2 = getMbLength(binaryStr2.slice(pos2, len2 - pos2));

            if (mbLen1 > 0 && mbLen2 > 0) {
                int code1 = getGB18030ChsToCode(binaryStr1.slice(pos1, mbLen1));
                int code2 = getGB18030ChsToCode(binaryStr2.slice(pos2, mbLen2));

                if (code1 != code2) {
                    return code1 > code2 ? 1 : -1;
                }

                pos1 += mbLen1;
                pos2 += mbLen2;
            } else if (mbLen1 == 0 && mbLen2 == 0) {
                int code1 = Byte.toUnsignedInt(binaryStr1.getByte(pos1++));
                int code2 = Byte.toUnsignedInt(binaryStr2.getByte(pos2++));

                if (code1 != code2) {
                    return code1 - code2;
                }
            } else {
                return mbLen1 == 0 ? -1 : 1;
            }
        }

        return len1 - len2;
    }

    @Override
    public int compareSp(Slice binaryStr1, Slice binaryStr2) {
        int len1 = binaryStr1.length();
        int len2 = binaryStr2.length();
        int pos1 = 0, pos2 = 0;

        while (pos1 < len1 && pos2 < len2) {
            int mbLen1 = getMbLength(binaryStr1.slice(pos1, len1 - pos1));
            int mbLen2 = getMbLength(binaryStr2.slice(pos2, len2 - pos2));

            if (mbLen1 > 0 && mbLen2 > 0) {
                int code1 = getGB18030ChsToCode(binaryStr1.slice(pos1, mbLen1));
                int code2 = getGB18030ChsToCode(binaryStr2.slice(pos2, mbLen2));

                if (code1 != code2) {
                    return code1 > code2 ? 1 : -1;
                }

                pos1 += mbLen1;
                pos2 += mbLen2;
            } else if (mbLen1 == 0 && mbLen2 == 0) {
                int code1 = Byte.toUnsignedInt(binaryStr1.getByte(pos1++));
                int code2 = Byte.toUnsignedInt(binaryStr2.getByte(pos2++));

                if (code1 != code2) {
                    return code1 - code2;
                }
            } else {
                return mbLen1 == 0 ? -1 : 1;
            }
        }

        if (pos1 < len1 || pos2 < len2) {
            int swap = 1;
            if (len1 < len2) {
                binaryStr1 = binaryStr2;
                pos1 = pos2;
                len1 = len2;
                swap = -1;
            }
            for (; pos1 < len1; pos1++) {
                if (Byte.toUnsignedInt(binaryStr1.getByte(pos1)) != 0x20) {
                    return Byte.toUnsignedInt(binaryStr1.getByte(pos1)) < 0x20 ? -swap : swap;
                }
            }
        }

        return 0;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        int len = str.length();
        int pos = 0;
        while (pos < len && dst.hasRemaining()) {
            int mbLen = getMbLength(str.slice(pos, len - pos));

            if (mbLen > 0) {
                for (int i = 0; i < mbLen; i++) {
                    if (dst.hasRemaining()) {
                        dst.put(str.getByte(pos + i));
                    }
                }
                pos += mbLen;
            } else {
                dst.put(str.getByte(pos++));
            }
        }

        int effectiveLen = dst.position();

        while (dst.hasRemaining()) {
            dst.put((byte) 0x20);
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target,
            Gb18030BinCollationHandler::codeOfGB18030);
    }

    @Override
    public int hashcode(Slice str) {
        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;
        // skip trailing space
        int len = str.length();
        while (len >= 1 && str.getByte(len - 1) == 0x20) {
            len--;
        }

        int pos = 0;
        while (pos < len) {
            int mbLen = getMbLength(str.slice(pos, len - pos));
            int code;

            if (mbLen > 0) {
                code = getGB18030ChsToCode(str.slice(pos, mbLen));

                pos += mbLen;
            } else {
                code = Byte.toUnsignedInt(str.getByte(pos++));
            }

            for (int i = 0; i < 4; i++) {
                tmp1 ^= (((tmp1 & 63) + tmp2) * ((code >> (i * 8)) & 0xFF)) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {
        long tmp1 = Integer.toUnsignedLong((int) numbers[0]);
        long tmp2 = Integer.toUnsignedLong((int) numbers[1]);

        // skip trailing space
        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }

        for (int pos = begin; pos < end; ) {
            int mbLen = getMbLength(Slices.wrappedBuffer(bytes, pos, end - pos));
            int code;

            if (mbLen > 0) {
                code = getGB18030ChsToCode(Slices.wrappedBuffer(bytes, pos, mbLen));

                pos += mbLen;
            } else {
                code = Byte.toUnsignedInt(bytes[pos++]);
            }

            for (int i = 0; i < 4; i++) {
                tmp1 ^= (((tmp1 & 63) + tmp2) * ((code >> (i * 8)) & 0xFF)) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    public static int codeOfGB18030(SliceInput sliceInput) {
        if (sliceInput.available() <= 0) {
            return INVALID_CODE;
        }

        byte b1 = sliceInput.readByte();
        if (sliceInput.isReadable() && isMbOdd(b1)) {
            byte b2 = sliceInput.readByte();
            if (isMbEven2(b2)) {
                // 2 bytes code.
                return getGB18030ChsToCode(Slices.wrappedBuffer(b1, b2));
            } else {
                if (sliceInput.available() >= 2) {
                    byte b3 = sliceInput.readByte();
                    byte b4 = sliceInput.readByte();
                    if (isMbEven4(b2) && isMbOdd(b3) && isMbEven4(b4)) {
                        // 4 bytes code.
                        return getGB18030ChsToCode(Slices.wrappedBuffer(b1, b2, b3, b4));
                    } else {
                        sliceInput.setPosition(sliceInput.position() - 3);
                    }
                } else {
                    sliceInput.setPosition(sliceInput.position() - 1);
                }
            }
        }

        // 1 byte code.
        return Byte.toUnsignedInt(b1);
    }

}
