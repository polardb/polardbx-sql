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

package com.alibaba.polardbx.common.charset;

import com.alibaba.polardbx.common.collation.CollationHandler;
import com.google.common.primitives.UnsignedLongs;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Optional;

public abstract class AbstractCharsetHandler implements CharsetHandler {
    private static final long SIGNED_MAX_LONG = 0x7fffffffffffffffL;

    protected Charset charset;
    protected CharsetEncoder encoder;
    protected CollationHandler collationHandler;
    protected CollationName collationName;

    AbstractCharsetHandler(Charset charset, CollationName collationName) {
        this.charset = charset;
        this.encoder = charset.newEncoder();
        this.collationName = collationName;
        this.collationHandler = null;
    }

    @Override
    public Charset getCharset() {
        return charset;
    }

    @Override
    public CollationHandler getCollationHandler() {
        return collationHandler;
    }

    @Override
    public byte[] getBytes(String str) {
        return Optional
            .ofNullable(str)
            .map(s -> s.getBytes(charset))
            .orElse(new byte[] {});
    }

    @Override
    public Slice encodeWithReplace(String str) {
        return Optional
            .ofNullable(str)
            .map(s -> s.getBytes(charset))
            .map(Slices::wrappedBuffer)
            .orElse(null);
    }

    @Override
    public Slice encode(String unicodeChars) throws CharacterCodingException {
        if (unicodeChars == null) {
            return null;
        } else if (unicodeChars.isEmpty()) {
            return Slices.EMPTY_SLICE;
        }

        encoder.reset();
        CharBuffer charBuffer = CharBuffer.wrap(unicodeChars);

        ByteBuffer buffer = encoder.encode(charBuffer);
        if (!buffer.hasRemaining()) {
            return Slices.EMPTY_SLICE;
        }

        return Slices.wrappedBuffer(buffer);
    }

    @Override
    public Slice encodeFromUtf8(Slice utf8str) {
        return Optional.ofNullable(utf8str)
            .map(Slice::toByteBuffer)
            .map(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK::decode)
            .map(CharBuffer::toString)
            .map(this::encodeWithReplace)
            .orElse(null);
    }

    @Override
    public String decode(Slice slice) {
        return decode(slice, 0, slice.length());
    }

    @Override
    public String decode(byte[] bytes) {
        return Optional
            .ofNullable(bytes)
            .map(s -> new String(bytes, charset))
            .orElse(null);
    }

    @Override
    public String decode(byte[] bytes, int index, int len) {
        return Optional
            .ofNullable(bytes)
            .map(s -> new String(bytes, index, len, charset))
            .orElse(null);
    }

    @Override
    public String decode(Slice slice, int index, int len) {
        return Optional
            .ofNullable(slice)
            .map(s -> charset.decode(s.toByteBuffer(0, len)).toString())
            .orElse(null);
    }

    @Override
    public void parseToLongWithRound(byte[] numericAsBytes, int begin, int end, long[] results, boolean isUnsigned) {
        StringNumericParser.parseStringWithRound(numericAsBytes, begin, end, isUnsigned, results);
    }

    @Override
    public int parseFromLong(long toParse, boolean isUnsigned, byte[] result, int len) {
        byte[] buff = new byte[65];
        int pos, end;

        int dstPos = 0;
        long longVal;
        long unsignedVal = toParse;
        boolean isNeg = false;
        if (isUnsigned) {
            if (toParse < 0) {

                unsignedVal = -unsignedVal;
                result[dstPos++] = '-';
                len--;
                isNeg = true;
            }
        }

        pos = end = buff.length - 1;
        buff[pos] = 0;
        if (unsignedVal == 0) {
            buff[--pos] = '0';
            len = 1;
            System.arraycopy(buff, pos, result, dstPos, len);
            return len + (isNeg ? 1 : 0);
        }

        while (UnsignedLongs.compare(unsignedVal, SIGNED_MAX_LONG) > 0) {
            long quot = UnsignedLongs.divide(unsignedVal, 10);
            int rem = (int) (unsignedVal - quot * 10);
            buff[--pos] = (byte) (('0' + rem) & 0xFF);
            unsignedVal = quot;
        }

        longVal = unsignedVal;
        while (longVal != 0) {
            long quot = longVal / 10;
            buff[--pos] = (byte) (('0' + (longVal - quot * 10)) & 0xFF);
            longVal = quot;
        }

        len = Math.min(len, end - pos);
        System.arraycopy(buff, pos, result, dstPos, len);
        return len + (isNeg ? 1 : 0);
    }

    protected int nextCharUtf8(SliceInput buff) {
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
            if (!isContinuationByte(c2)) {
                return INVALID_CODE;
            }
            return ((Byte.toUnsignedInt(c1) & 0x1f) << 6) | (Byte.toUnsignedInt(c2) ^ 0x80);
        } else if (Byte.toUnsignedInt(c1) < 0xf0) {
            if (buff.available() < 2) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            byte c3 = buff.readByte();
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && (Byte.toUnsignedInt(c1) >= 0xe1
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
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && isContinuationByte(c4) && (
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

    protected int nextCharLenUtf8(SliceInput buff) {
        byte c1 = buff.readByte();

        if (Byte.toUnsignedInt(c1) < 0x80) {
            return 1;
        } else if (Byte.toUnsignedInt(c1) < 0xc2) {
            return INVALID_CODE;
        } else if (Byte.toUnsignedInt(c1) < 0xe0) {
            if (buff.available() < 1) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            if (!isContinuationByte(c2)) {
                return INVALID_CODE;
            }
            return 2;
        } else if (Byte.toUnsignedInt(c1) < 0xf0) {
            if (buff.available() < 2) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            byte c3 = buff.readByte();
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && (Byte.toUnsignedInt(c1) >= 0xe1
                || Byte.toUnsignedInt(c2) >= 0xa0))) {
                return INVALID_CODE;
            }
            return 3;
        } else if (Byte.toUnsignedInt(c1) < 0xf5) {
            if (buff.available() < 3) {
                return INVALID_CODE;
            }
            byte c2 = buff.readByte();
            byte c3 = buff.readByte();
            byte c4 = buff.readByte();
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && isContinuationByte(c4) && (
                Byte.toUnsignedInt(c1) >= 0xf1
                    || Byte.toUnsignedInt(c2) >= 0x90) && (Byte.toUnsignedInt(c1) <= 0xf3
                || Byte.toUnsignedInt(c2) <= 0x8F))) {
                return INVALID_CODE;
            }
            return 4;
        }
        return INVALID_CODE;
    }

    protected int nextCharLenUtf8(byte[] bytes, int offset, int length) {
        int pos = offset;
        final int end = offset + length;
        byte c1 = bytes[pos++];

        if (Byte.toUnsignedInt(c1) < 0x80) {
            return 1;
        } else if (Byte.toUnsignedInt(c1) < 0xc2) {
            return INVALID_CODE;
        } else if (Byte.toUnsignedInt(c1) < 0xe0) {
            if (end - pos < 1) {
                return INVALID_CODE;
            }
            byte c2 = bytes[pos++];
            if (!isContinuationByte(c2)) {
                return INVALID_CODE;
            }
            return 2;
        } else if (Byte.toUnsignedInt(c1) < 0xf0) {
            if (end - pos < 2) {
                return INVALID_CODE;
            }
            byte c2 = bytes[pos++];
            byte c3 = bytes[pos++];
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && (Byte.toUnsignedInt(c1) >= 0xe1
                || Byte.toUnsignedInt(c2) >= 0xa0))) {
                return INVALID_CODE;
            }
            return 3;
        } else if (Byte.toUnsignedInt(c1) < 0xf5) {
            if (end - pos < 3) {
                return INVALID_CODE;
            }
            byte c2 = bytes[pos++];
            byte c3 = bytes[pos++];
            byte c4 = bytes[pos++];
            if (!(isContinuationByte(c2) && isContinuationByte(c3) && isContinuationByte(c4) && (
                Byte.toUnsignedInt(c1) >= 0xf1
                    || Byte.toUnsignedInt(c2) >= 0x90) && (Byte.toUnsignedInt(c1) <= 0xf3
                || Byte.toUnsignedInt(c2) <= 0x8F))) {
                return INVALID_CODE;
            }
            return 4;
        }
        return INVALID_CODE;
    }

    public static boolean isContinuationByte(byte c) {
        return (Byte.toUnsignedInt(c) ^ 0x80) < 0x40;
    }
}
