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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.yaml.snakeyaml.Yaml;

import java.nio.ByteBuffer;

public class GbkChineseCiCollationHandler extends AbstractCollationHandler {
    private static final int[] GBK_ORDER;

    static {
        Yaml yaml = new Yaml();
        GBK_ORDER = yaml.loadAs(GbkChineseCiCollationHandler.class.getResourceAsStream("gbk_order.yml"), int[].class);
    }

    public static int[] SORT_ORDER_GBK =
        {
            '\000', '\001', '\002', '\003', '\004', '\005', '\006', '\007',
            '\010', '\011', '\012', '\013', '\014', '\015', '\016', '\017',
            '\020', '\021', '\022', '\023', '\024', '\025', '\026', '\027',
            '\030', '\031', '\032', '\033', '\034', '\035', '\036', '\037',
            ' ', '!', '"', '#', '$', '%', '&', '\'',
            '(', ')', '*', '+', ',', '-', '.', '/',
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', ':', ';', '<', '=', '>', '?',
            '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
            'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
            'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
            'X', 'Y', 'Z', '\\', ']', '[', '^', '_',
            '`', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
            'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
            'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
            'X', 'Y', 'Z', '{', '|', '}', 'Y', '\177',
            '\200', '\201', '\202', '\203', '\204', '\205', '\206', '\207',
            '\210', '\211', '\212', '\213', '\214', '\215', '\216', '\217',
            '\220', '\221', '\222', '\223', '\224', '\225', '\226', '\227',
            '\230', '\231', '\232', '\233', '\234', '\235', '\236', '\237',
            '\240', '\241', '\242', '\243', '\244', '\245', '\246', '\247',
            '\250', '\251', '\252', '\253', '\254', '\255', '\256', '\257',
            '\260', '\261', '\262', '\263', '\264', '\265', '\266', '\267',
            '\270', '\271', '\272', '\273', '\274', '\275', '\276', '\277',
            '\300', '\301', '\302', '\303', '\304', '\305', '\306', '\307',
            '\310', '\311', '\312', '\313', '\314', '\315', '\316', '\317',
            '\320', '\321', '\322', '\323', '\324', '\325', '\326', '\327',
            '\330', '\331', '\332', '\333', '\334', '\335', '\336', '\337',
            '\340', '\341', '\342', '\343', '\344', '\345', '\346', '\347',
            '\350', '\351', '\352', '\353', '\354', '\355', '\356', '\357',
            '\360', '\361', '\362', '\363', '\364', '\365', '\366', '\367',
            '\370', '\371', '\372', '\373', '\374', '\375', '\376', '\377',
        };

    public GbkChineseCiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.GBK_CHINESE_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GBK;
    }

    @Override
    public int compareSp(Slice binaryStr1, Slice binaryStr2) {
        SliceInput sliceInput1 = binaryStr1.getInput();
        SliceInput sliceInput2 = binaryStr2.getInput();
        int len1 = binaryStr1.length();
        int len2 = binaryStr2.length();
        int minLen = Math.min(len1, len2);

        int sortOrder1, sortOrder2;
        int pos;

        for (pos = 0; pos < minLen; pos++) {
            byte l1 = binaryStr1.getByte(pos);
            byte l2 = binaryStr2.getByte(pos);
            byte h1, h2;
            if (pos + 1 < minLen
                && isGBKCode(l1, h1 = binaryStr1.getByte(pos + 1))
                && isGBKCode(l2, h2 = binaryStr2.getByte(pos + 1))) {

                int code1 = getGBKCode(l1, h1);
                int code2 = getGBKCode(l2, h2);
                if (code1 != code2) {
                    return getGBKWeight(code1) - getGBKWeight(code2);
                }

                pos++;
            } else if ((sortOrder1 = SORT_ORDER_GBK[Byte.toUnsignedInt(l1)])
                != (sortOrder2 = SORT_ORDER_GBK[Byte.toUnsignedInt(l2)])) {

                return sortOrder1 - sortOrder2;
            }
        }

        sliceInput1.setPosition(pos);
        sliceInput2.setPosition(pos);

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
                rest--;
                byte b = sliceInput1.readByte();
                if (Byte.toUnsignedInt(b) != ' ') {
                    return Byte.toUnsignedInt(b) < ' ' ? -swap : swap;
                }
            }
        }
        return res;
    }

    @Override
    public int compare(Slice binaryStr1, Slice binaryStr2) {
        int len1 = binaryStr1.length();
        int len2 = binaryStr2.length();
        int minLen = Math.min(len1, len2);

        int sortOrder1, sortOrder2;
        int pos;

        for (pos = 0; pos < minLen; pos++) {
            byte l1 = binaryStr1.getByte(pos);
            byte l2 = binaryStr2.getByte(pos);
            byte h1, h2;
            if (pos + 1 < minLen
                && isGBKCode(l1, h1 = binaryStr1.getByte(pos + 1))
                && isGBKCode(l2, h2 = binaryStr2.getByte(pos + 1))) {

                int code1 = getGBKCode(l1, h1);
                int code2 = getGBKCode(l2, h2);
                if (code1 != code2) {
                    return getGBKWeight(code1) - getGBKWeight(code2);
                }

                pos++;
            } else if ((sortOrder1 = SORT_ORDER_GBK[Byte.toUnsignedInt(l1)])
                != (sortOrder2 = SORT_ORDER_GBK[Byte.toUnsignedInt(l2)])) {

                return sortOrder1 - sortOrder2;
            }
        }

        return len1 - len2;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        int len = str.length();
        int pos;

        for (pos = 0; pos < len && dst.hasRemaining(); pos++) {
            byte l = str.getByte(pos);
            byte h;
            if (pos + 1 < len
                && isGBKCode(l, h = str.getByte(pos + 1))) {
                int code = getGBKCode(l, h);
                int gbkWeight = getGBKWeight(code);
                dst.put((byte) (gbkWeight >> 8));
                if (dst.hasRemaining()) {
                    dst.put((byte) (gbkWeight & 0xFF));
                }

                pos++;
            } else {
                dst.put((byte) (SORT_ORDER_GBK[Byte.toUnsignedInt(l)] & 0xFF));
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
        return instrForMultiBytes(source, target, sliceInput -> codeOfGBK(sliceInput));
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
            int code = Byte.toUnsignedInt(sliceInput.readByte());
            int weight = SORT_ORDER_GBK[code];
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
            int code = Byte.toUnsignedInt(bytes[i]);
            int weight = SORT_ORDER_GBK[code];
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * weight) + (tmp1 << 8);
            tmp2 += 3;
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    public static int getGBKWeight(int code) {
        int index = code & 0xFF;
        if (index > 0x7F) {
            index -= 0x41;
        } else {
            index -= 0x40;
        }
        index += ((code >> 8) - 0x81) * 0xBE;
        Preconditions.checkArgument(index >= 0 && index < GBK_ORDER.length);
        return 0x8100 + GBK_ORDER[index];
    }

    public static int getGBKHead(int code) {
        return code >> 8;
    }

    public static int getGBKTail(int code) {
        return code & 0xFF;
    }

    public static int getGBKCode(byte a, byte b) {
        return (Byte.toUnsignedInt(a) << 8) | Byte.toUnsignedInt(b);
    }

    public static int codeOfGBK(SliceInput sliceInput) {
        if (sliceInput.available() <= 0) {
            return INVALID_CODE;
        }
        byte l = sliceInput.readByte();
        if (sliceInput.isReadable()) {
            byte h = sliceInput.readByte();
            if (isGBKCode(l, h)) {

                return getGBKCode(l, h);
            } else {
                sliceInput.setPosition(sliceInput.position() - 1);
            }
        }

        return Byte.toUnsignedInt(l);
    }

    public static boolean isGBKCode(byte a, byte b) {
        return isGBKHead(a) && isGBKTail(b);
    }

    public static boolean isGBKHead(byte b) {
        return 0x81 <= Byte.toUnsignedInt(b) && Byte.toUnsignedInt(b) <= 0xfe;
    }

    public static boolean isGBKTail(byte b) {
        return (0x40 <= Byte.toUnsignedInt(b) && Byte.toUnsignedInt(b) <= 0x7e)
            || (0x80 <= Byte.toUnsignedInt(b) && Byte.toUnsignedInt(b) <= 0xfe);
    }
}
