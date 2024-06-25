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
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractCollationHandler implements CollationHandler {
    /* wm_wc and wc_mb return codes */
    /* clang-format off */
    static final int MY_CS_ILSEQ = 0;        /* Wrong by sequence: wb_wc                   */
    static final int MY_CS_ILUNI = 0;        /* Cannot encode Unicode to charset: wc_mb    */
    static final int MY_CS_TOOSMALL = -101; /* Need at least one byte:    wc_mb and mb_wc */
    static final int MY_CS_TOOSMALL2 = -102; /* Need at least two bytes:   wc_mb and mb_wc */
    static final int MY_CS_TOOSMALL3 = -103; /* Need at least three bytes: wc_mb and mb_wc */

    /* These following three are currently not really used */
    static final int MY_CS_TOOSMALL4 = -104; /* Need at least 4 bytes: wc_mb and mb_wc */
    static final int MY_CS_TOOSMALL5 = -105; /* Need at least 5 bytes: wc_mb and mb_wc */
    static final int MY_CS_TOOSMALL6 = -106; /* Need at least 6 bytes: wc_mb and mb_wc */

    protected final CharsetHandler charsetHandler;

    AbstractCollationHandler(CharsetHandler charsetHandler) {
        this.charsetHandler = charsetHandler;
    }

    /**
     * 0x0 - 0x7f | 0xxxxxxx
     * 0x80 - 0x7ff | 110xxxxx 10xxxxxx
     * 0x800 - 0xffff | 1110xxxx 10xxxxxx 10xxxxxx
     * 0x10000 - 0x10ffff | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
     */
    public static int codepointOfUTF8(int[] parsedWildCharacter, Slice str, int s, int e,
                                      boolean rangeCheck, boolean supportMB4) {
        if (rangeCheck && s >= e) {
            return MY_CS_TOOSMALL;
        }

        byte c = str.getByte(s);
        if (Byte.toUnsignedInt(c) < 0x80) {
            parsedWildCharacter[0] = c;
            return 1;
        }

        if (Byte.toUnsignedInt(c) < 0xe0) {
            // Resulting code point would be less than 0x80.
            if (Byte.toUnsignedInt(c) < 0xc2) {
                return MY_CS_ILSEQ;
            }

            if (rangeCheck && s + 2 > e) {
                return MY_CS_TOOSMALL2;
            }

            // Next byte must be a continuation byte.
            if ((str.getByte(s + 1) & 0xc0) != 0x80) {
                return MY_CS_ILSEQ;
            }

            parsedWildCharacter[0] = ((c & 0x1f) << 6) + (str.getByte(s + 1) & 0x3f);
            return 2;
        }

        if (Byte.toUnsignedInt(c) < 0xf0) {
            if (rangeCheck && s + 3 > e) {
                return MY_CS_TOOSMALL3;
            }

            // Next two bytes must be continuation bytes.
            int two_bytes = ((str.getByte(s + 1) & 0xFF) << 8) | (str.getByte(s + 2) & 0xFF);

            // Endianness does not matter.
            if ((two_bytes & 0xc0c0) != 0x8080) {
                return MY_CS_ILSEQ;
            }

            parsedWildCharacter[0] = ((c & 0x0f) << 12)
                + ((str.getByte(s + 1) & 0x3f) << 6)
                + (str.getByte(s + 2) & 0x3f);

            if (parsedWildCharacter[0] < 0x800) {
                return MY_CS_ILSEQ;
            }

            // According to RFC 3629, UTF-8 should prohibit characters between
            // U+D800 and U+DFFF, which are reserved for surrogate pairs and do
            // not directly represent characters.
            if (parsedWildCharacter[0] >= 0xd800 && parsedWildCharacter[0] <= 0xdfff) {
                return MY_CS_ILSEQ;
            }
            return 3;
        }

        if (supportMB4) {
            // We need 4 characters
            if (rangeCheck && s + 4 > e) {
                return MY_CS_TOOSMALL4;
            }

            // This byte must be of the form 11110xxx, and the next three bytes
            // must be continuation bytes.
            long four_bytes = ((0xFF & str.getByte(s)) << 24) | ((0xFF & str.getByte(s + 1)) << 16) |
                ((0xFF & str.getByte(s + 2)) << 8) | (0xFF & str.getByte(s + 3));

            if ((four_bytes & 0xf8c0c0c0) != 0xf0808080) {
                return MY_CS_ILSEQ;
            }

            parsedWildCharacter[0] = ((c & 0x07) << 18) + ((str.getByte(s + 1) & 0x3f) << 12) +
                ((str.getByte(s + 2) & 0x3f) << 6) + (str.getByte(s + 3) & 0x3f);
            if (parsedWildCharacter[0] < 0x10000 || parsedWildCharacter[0] > 0x10ffff) {
                return MY_CS_ILSEQ;
            }
            return 4;
        }

        return MY_CS_ILSEQ;
    }

    /**
     * 0x0 - 0x7f | 0xxxxxxx
     * 0x80 - 0x7ff | 110xxxxx 10xxxxxx
     * 0x800 - 0xffff | 1110xxxx 10xxxxxx 10xxxxxx
     * 0x10000 - 0x10ffff | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
     */
    public static int codepointOfUTF8(SliceInput buff) {
        if (!buff.isReadable()) {
            return INVALID_CODE;
        }
        byte c1 = buff.readByte();

        if (Byte.toUnsignedInt(c1) < 0x80) {
            return Byte.toUnsignedInt(c1);
        } else if (Byte.toUnsignedInt(c1) < 0xc2) {
            return INVALID_CODE;
        } else if (Byte.toUnsignedInt(c1) < 0xe0) {
            if (buff.available() < 1) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            if (!Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2)) {
                return INVALID_CODE;
            }
            return ((Byte.toUnsignedInt(c1) & 0x1f) << 6) | (Byte.toUnsignedInt(c2) ^ 0x80);
        } else if (Byte.toUnsignedInt(c1) < 0xf0) {
            if (buff.available() < 2) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            byte c3 = buff.readByte();
            if (!(Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2) && Utf8mb4GeneralCiCollationHandler
                .isContinuationByte(c3) && (Byte.toUnsignedInt(c1) >= 0xe1
                || Byte.toUnsignedInt(c2) >= 0xa0))) {
                return INVALID_CODE;
            }
            return ((Byte.toUnsignedInt(c1) & 0x0f) << 12) | ((Byte.toUnsignedInt(c2) ^ 0x80) << 6) | (
                Byte.toUnsignedInt(c3) ^ 0x80);
        } else if (Byte.toUnsignedInt(c1) < 0xf5) {
            if (buff.available() < 3) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            byte c3 = buff.readByte();
            byte c4 = buff.readByte();
            if (!(Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2) && Utf8mb4GeneralCiCollationHandler
                .isContinuationByte(c3) && Utf8mb4GeneralCiCollationHandler.isContinuationByte(c4) && (
                Byte.toUnsignedInt(c1) >= 0xf1
                    || Byte.toUnsignedInt(c2) >= 0x90) && (Byte.toUnsignedInt(c1) <= 0xf3
                || Byte.toUnsignedInt(c2) <= 0x8F))) {
                return INVALID_CODE;
            }
            return ((Byte.toUnsignedInt(c1) & 0x07) << 18) | ((Byte.toUnsignedInt(c2) ^ 0x80) << 12) | (
                (Byte.toUnsignedInt(c3) ^ 0x80) << 6) | (Byte.toUnsignedInt(c4) ^ 0x80);
        }
        return INVALID_CODE;
    }

    public static int codepointOfUTF8(SliceInput buff, long[] positions) {
        // mark begin position
        positions[0] = buff.position();

        int codePoint = codepointOfUTF8(buff);

        // mark end position, and reset position to begin position
        positions[1] = buff.position();
        buff.setPosition(positions[0]);

        return codePoint;
    }

    /**
     * Get the next codepoint and move index from UTF-8 byte array.
     *
     * @param buff utf-8 bytes
     * @param begin begin index
     * @param end end index
     * @param results results[0] = codepoint, results[1] = current index
     */
    public static void codepointOfUTF8(byte[] buff, int begin, int end, int[] results) {
        byte c1 = buff[begin++];

        if (Byte.toUnsignedInt(c1) < 0x80) {
            results[0] = Byte.toUnsignedInt(c1);
            results[1] = begin;
            return;
        } else if (Byte.toUnsignedInt(c1) < 0xc2) {
            results[0] = INVALID_CODE;
            results[1] = begin;
            return;
        } else if (Byte.toUnsignedInt(c1) < 0xe0) {
            if (end - begin < 1) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            byte c2 = buff[begin++];
            if (!Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2)) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            results[0] = ((Byte.toUnsignedInt(c1) & 0x1f) << 6) | (Byte.toUnsignedInt(c2) ^ 0x80);
            results[1] = begin;
            return;
        } else if (Byte.toUnsignedInt(c1) < 0xf0) {
            if (end - begin < 2) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            byte c2 = buff[begin++];
            byte c3 = buff[begin++];
            if (!(Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2) && Utf8mb4GeneralCiCollationHandler
                .isContinuationByte(c3) && (Byte.toUnsignedInt(c1) >= 0xe1
                || Byte.toUnsignedInt(c2) >= 0xa0))) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            results[0] = ((Byte.toUnsignedInt(c1) & 0x0f) << 12) | ((Byte.toUnsignedInt(c2) ^ 0x80) << 6) | (
                Byte.toUnsignedInt(c3) ^ 0x80);
            results[1] = begin;
            return;
        } else if (Byte.toUnsignedInt(c1) < 0xf5) {
            if (end - begin < 3) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            byte c2 = buff[begin++];
            byte c3 = buff[begin++];
            byte c4 = buff[begin++];
            if (!(Utf8mb4GeneralCiCollationHandler.isContinuationByte(c2) && Utf8mb4GeneralCiCollationHandler
                .isContinuationByte(c3) && Utf8mb4GeneralCiCollationHandler.isContinuationByte(c4) && (
                Byte.toUnsignedInt(c1) >= 0xf1
                    || Byte.toUnsignedInt(c2) >= 0x90) && (Byte.toUnsignedInt(c1) <= 0xf3
                || Byte.toUnsignedInt(c2) <= 0x8F))) {
                results[0] = INVALID_CODE;
                results[1] = begin;
                return;
            }
            results[0] = ((Byte.toUnsignedInt(c1) & 0x07) << 18) | ((Byte.toUnsignedInt(c2) ^ 0x80) << 12) | (
                (Byte.toUnsignedInt(c3) ^ 0x80) << 6) | (Byte.toUnsignedInt(c4) ^ 0x80);
            results[1] = begin;
            return;
        }
        results[0] = INVALID_CODE;
        results[1] = begin;
        return;
    }

    @Override
    public CharsetName getCharsetName() {
        return charsetHandler.getName();
    }

    @Override
    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    @Override
    public int compare(String utf16Str1, String utf16Str2) {
        if (utf16Str1 == null) {
            return -1;
        } else if (utf16Str2 == null) {
            return 1;
        }
        return isCaseSensitive() ? utf16Str1.compareTo(utf16Str2) : utf16Str1.compareToIgnoreCase(utf16Str2);
    }

    @Override
    public int compare(char[] utf16Char1, char[] utf16Char2) {
        return compare(new String(utf16Char1), new String(utf16Char2));
    }

    @Override
    public int compare(Slice binaryStr1, Slice binaryStr2) {
        String utf16Str1 = charsetHandler.decode(binaryStr1);
        String utf16Str2 = charsetHandler.decode(binaryStr2);
        return compare(utf16Str1, utf16Str2);
    }

    @Override
    public int compareSp(Slice binaryStr1, Slice binaryStr2) {
        return compare(binaryStr1, binaryStr2);
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        byte[] keys = str.getBytes();
        return new SortKey(getCharsetName(), getName(), str.getInput(), keys);
    }

    @Override
    public int instr(Slice source, Slice target) {
        String str = charsetHandler.decode(source);
        String subStr = charsetHandler.decode(target);
        return TStringUtil.indexOf(str, subStr) + 1;
    }

    @Override
    public int hashcode(Slice str) {
        return Optional.ofNullable(str)
            .map(charsetHandler::decode)
            .map(String::hashCode)
            .orElse(0);
    }

    protected int binaryCompare(SliceInput str1, SliceInput str2) {
        Preconditions.checkNotNull(str1);
        Preconditions.checkNotNull(str2);
        int len1 = str1.available();
        int len2 = str2.available();
        while (str1.isReadable() && str2.isReadable()) {
            byte b1 = str1.readByte();
            byte b2 = str2.readByte();
            if (b1 != b2) {
                return Byte.toUnsignedInt(b1) - Byte.toUnsignedInt(b2);
            }
        }
        return len1 - len2;
    }

    protected int instrForMultiBytes(Slice source, Slice target, Function<SliceInput, Integer> codePointOf) {
        int sourceLen = source.length();
        int targetLen = target.length();
        int characters = 1;
        if (sourceLen >= targetLen) {
            if (targetLen == 0) {

                return 1;
            }

            for (int pos = 0; pos < sourceLen - targetLen + 1; ) {
                Slice beSearched = source.slice(pos, targetLen);
                if (compareSp(beSearched, target) == 0) {
                    return characters;
                }

                SliceInput sliceInput = source.getInput();
                sliceInput.setPosition(pos);
                int code = codePointOf.apply(sliceInput);

                pos = (code == INVALID_CODE)
                    ? pos + 1
                    : (int) sliceInput.position();
                characters++;
            }
        }
        return 0;
    }

    protected int instrForSingleByte(Slice source, Slice target, Function<Byte, Integer> sortOrder) {
        int sourceLen = source.length();
        int targetLen = target.length();
        if (sourceLen >= targetLen) {
            if (targetLen == 0) {

                return 1;
            }

            MatchScanner scanner = new MatchScanner(source, target, sortOrder);
            int nextAlignedPosition;
            while ((nextAlignedPosition = scanner.nextAlignedPosition()) > 0) {
                if (scanner.matchFrom(nextAlignedPosition)) {
                    return nextAlignedPosition + 1;
                }
            }
        }
        return 0;
    }

    static class MatchScanner {
        final int size;
        final Slice source;
        final Slice target;
        final int limit;
        final Function<Byte, Integer> sortOrder;
        int lastAlignedPosition;
        final int weight0;

        MatchScanner(Slice source, Slice target, Function<Byte, Integer> sortOrder) {
            this.size = target.length();
            this.source = source;
            this.target = target;
            this.limit = source.length() - target.length() + 1;
            this.sortOrder = sortOrder;
            this.lastAlignedPosition = -1;
            this.weight0 = sortOrder.apply(target.getByte(0));
        }

        boolean matchFrom(int position) {
            Slice beSearched = source.slice(position, size);
            for (int i = 0; i < size; i++) {
                byte b1 = beSearched.getByte(i);
                byte b2 = target.getByte(i);
                if (!sortOrder.apply(b1).equals(sortOrder.apply(b2))) {
                    return false;
                }
            }
            return true;
        }

        int nextAlignedPosition() {

            int i = lastAlignedPosition + 1;
            for (; i < limit; i++) {
                if (sortOrder.apply(source.getByte(i)) == weight0) {
                    return lastAlignedPosition = i;
                }
            }
            return lastAlignedPosition = -1;
        }
    }
}
