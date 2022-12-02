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

package com.alibaba.polardbx.optimizer.core.datatype;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.EMPTY_BYTE_ARRAY;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NULL_BYTE_HIGH;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NULL_BYTE_LOW;

/**
 * A very low-level class that decodes key components encoded by methods of
 * {@link DataEncoder}.
 *
 * @author Brian S O'Neill
 * @see KeyDecoder
 */
public class DataDecoder {

    /**
     * Decodes a signed integer from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed integer value
     */
    public static int decodeInt(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int value = (src[srcOffset] << 24) | ((src[srcOffset + 1] & 0xff) << 16)
                | ((src[srcOffset + 2] & 0xff) << 8) | (src[srcOffset + 3] & 0xff);
            return value ^ 0x80000000;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Integer object from exactly 1 or 5 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Integer object or null
     */
    public static Integer decodeIntegerObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeInt(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed long from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed long value
     */
    public static long decodeLong(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (((long) (((src[srcOffset]) << 24) | ((src[srcOffset + 1] & 0xff) << 16)
                | ((src[srcOffset + 2] & 0xff) << 8) | ((src[srcOffset + 3] & 0xff))) ^ 0x80000000) << 32)
                | (((((src[srcOffset + 4]) << 24) | ((src[srcOffset + 5] & 0xff) << 16)
                | ((src[srcOffset + 6] & 0xff) << 8) | ((src[srcOffset + 7] & 0xff))) & 0xffffffffL));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Long object from exactly 1 or 9 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Long object or null
     */
    public static Long decodeLongObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeLong(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed byte from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed byte value
     */
    public static byte decodeByte(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (byte) (src[srcOffset] ^ 0x80);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Byte object from exactly 1 or 2 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Byte object or null
     */
    public static Byte decodeByteObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeByte(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed short from exactly 2 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed short value
     */
    public static short decodeShort(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (short) (((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff)) ^ 0x8000);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Short object from exactly 1 or 3 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Short object or null
     */
    public static Short decodeShortObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeShort(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a char from exactly 2 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return char value
     */
    public static char decodeChar(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (char) ((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Character object from exactly 1 or 3 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Character object or null
     */
    public static Character decodeCharacterObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeChar(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a boolean from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return boolean value
     */
    public static boolean decodeBoolean(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return src[srcOffset] == (byte) 128;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Boolean object from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Boolean object or null
     */
    public static Boolean decodeBooleanObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            switch (src[srcOffset]) {
            case NULL_BYTE_LOW:
            case NULL_BYTE_HIGH:
                return null;
            case (byte) 128:
                return Boolean.TRUE;
            default:
                return Boolean.FALSE;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a float from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return float value
     */
    public static float decodeFloat(byte[] src, int srcOffset) throws CorruptEncodingException {
        int bits = decodeFloatBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x80000000 : 0xffffffff;
        return Float.intBitsToFloat(bits);
    }

    /**
     * Decodes a Float object from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Float object or null
     */
    public static Float decodeFloatObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        int bits = decodeFloatBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x80000000 : 0xffffffff;
        return bits == 0x7fffffff ? null : Float.intBitsToFloat(bits);
    }

    protected static int decodeFloatBits(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (src[srcOffset] << 24) | ((src[srcOffset + 1] & 0xff) << 16) | ((src[srcOffset + 2] & 0xff) << 8)
                | (src[srcOffset + 3] & 0xff);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a double from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return double value
     */
    public static double decodeDouble(byte[] src, int srcOffset) throws CorruptEncodingException {
        long bits = decodeDoubleBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x8000000000000000L : 0xffffffffffffffffL;
        return Double.longBitsToDouble(bits);
    }

    /**
     * Decodes a Double object from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Double object or null
     */
    public static Double decodeDoubleObj(byte[] src, int srcOffset) throws CorruptEncodingException {
        long bits = decodeDoubleBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x8000000000000000L : 0xffffffffffffffffL;
        return bits == 0x7fffffffffffffffL ? null : Double.longBitsToDouble(bits);
    }

    protected static long decodeDoubleBits(byte[] src, int srcOffset) throws CorruptEncodingException {
        try {
            return (((long) (((src[srcOffset]) << 24) | ((src[srcOffset + 1] & 0xff) << 16)
                | ((src[srcOffset + 2] & 0xff) << 8) | ((src[srcOffset + 3] & 0xff)))) << 32)
                | (((((src[srcOffset + 4]) << 24) | ((src[srcOffset + 5] & 0xff) << 16)
                | ((src[srcOffset + 6] & 0xff) << 8) | ((src[srcOffset + 7] & 0xff))) & 0xffffffffL));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a BigInteger.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded BigInteger is stored in element 0, which may be
     * null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     * @since 1.2
     */
    public static int decode(byte[] src, int srcOffset, BigInteger[] valueRef) throws CorruptEncodingException {
        byte[][] bytesRef = new byte[1][];
        int amt = decode(src, srcOffset, bytesRef);
        valueRef[0] = (bytesRef[0] == null || bytesRef[0].length == 0) ? null : new BigInteger(bytesRef[0]);
        return amt;
    }

    /**
     * Decodes a BigDecimal.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded BigDecimal is stored in element 0, which may be
     * null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     * @since 1.2
     */
    public static int decode(byte[] src, int srcOffset, BigDecimal[] valueRef) throws CorruptEncodingException {
        try {
            final int originalOffset = srcOffset;

            int b = src[srcOffset++] & 0xff;
            if (b >= 0xf8) {
                valueRef[0] = null;
                return 1;
            }

            int scale;
            if (b <= 0x7f) {
                scale = b;
            } else if (b <= 0xbf) {
                scale = ((b & 0x3f) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xdf) {
                scale = ((b & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xef) {
                scale = ((b & 0x0f) << 24) | ((src[srcOffset++] & 0xff) << 16) | ((src[srcOffset++] & 0xff) << 8)
                    | (src[srcOffset++] & 0xff);
            } else {
                scale = ((src[srcOffset++] & 0xff) << 24) | ((src[srcOffset++] & 0xff) << 16)
                    | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            }

            if ((scale & 1) != 0) {
                scale = (~(scale >> 1)) | (1 << 31);
            } else {
                scale >>>= 1;
            }

            BigInteger[] unscaledRef = new BigInteger[1];
            int amt = decode(src, srcOffset, unscaledRef);

            if (unscaledRef[0] == null) {
                valueRef[0] = null;
            } else {
                valueRef[0] = new BigDecimal(unscaledRef[0], scale);
            }
            return (srcOffset + amt) - originalOffset;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded byte array is stored in element 0, which may be
     * null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decode(byte[] src, int srcOffset, byte[][] valueRef) throws CorruptEncodingException {
        try {
            final int originalOffset = srcOffset;

            int b = src[srcOffset++] & 0xff;
            if (b >= 0xf8) {
                valueRef[0] = null;
                return 1;
            }

            int valueLength;
            if (b <= 0x7f) {
                valueLength = b;
            } else if (b <= 0xbf) {
                valueLength = ((b & 0x3f) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xdf) {
                valueLength = ((b & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xef) {
                valueLength = ((b & 0x0f) << 24) | ((src[srcOffset++] & 0xff) << 16) | ((src[srcOffset++] & 0xff) << 8)
                    | (src[srcOffset++] & 0xff);
            } else {
                valueLength = ((src[srcOffset++] & 0xff) << 24) | ((src[srcOffset++] & 0xff) << 16)
                    | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            }

            if (valueLength == 0) {
                valueRef[0] = EMPTY_BYTE_ARRAY;
            } else {
                byte[] value = new byte[valueLength];
                System.arraycopy(src, srcOffset, value, 0, valueLength);
                valueRef[0] = value;
            }

            return srcOffset - originalOffset + valueLength;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    public static int decodeStringObj(byte[] src, int srcOffset, String[] valueRef) throws CorruptEncodingException {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                valueRef[0] = null;
                return 1;
            }
            return decodeString(src, srcOffset + 1, valueRef) + 1;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes an encoded string from the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded string is stored in element 0, which may be null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decodeString(byte[] src, int srcOffset, String[] valueRef) throws CorruptEncodingException {
        try {
            final int originalOffset = srcOffset;

            int b = src[srcOffset++] & 0xff;
            if (b >= 0xf8) {
                valueRef[0] = null;
                return 1;
            }

            int valueLength;
            if (b <= 0x7f) {
                valueLength = b;
            } else if (b <= 0xbf) {
                valueLength = ((b & 0x3f) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xdf) {
                valueLength = ((b & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xef) {
                valueLength = ((b & 0x0f) << 24) | ((src[srcOffset++] & 0xff) << 16) | ((src[srcOffset++] & 0xff) << 8)
                    | (src[srcOffset++] & 0xff);
            } else {
                valueLength = ((src[srcOffset++] & 0xff) << 24) | ((src[srcOffset++] & 0xff) << 16)
                    | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            }

            if (valueLength == 0) {
                valueRef[0] = "";
                return srcOffset - originalOffset;
            }

            char[] value;
            try {
                value = new char[valueLength];
            } catch (NegativeArraySizeException e) {
                throw new CorruptEncodingException("Corrupt encoded string length (negative size): " + valueLength);
            } catch (OutOfMemoryError e) {
                throw new CorruptEncodingException("Corrupt encoded string length (too large): " + valueLength, e);
            }

            int valueOffset = 0;

            while (valueOffset < valueLength) {
                int c = src[srcOffset++] & 0xff;
                switch (c >> 5) {
                case 0:
                case 1:
                case 2:
                case 3:
                    // 0xxxxxxx
                    value[valueOffset++] = (char) c;
                    break;
                case 4:
                case 5:
                    // 10xxxxxx xxxxxxxx
                    value[valueOffset++] = (char) (((c & 0x3f) << 8) | (src[srcOffset++] & 0xff));
                    break;
                case 6:
                    // 110xxxxx xxxxxxxx xxxxxxxx
                    c = ((c & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
                    if (c >= 0x10000) {
                        // Split into surrogate pair.
                        c -= 0x10000;
                        value[valueOffset++] = (char) (0xd800 | ((c >> 10) & 0x3ff));
                        value[valueOffset++] = (char) (0xdc00 | (c & 0x3ff));
                    } else {
                        value[valueOffset++] = (char) c;
                    }
                    break;
                default:
                    // 111xxxxx
                    // Illegal.
                    throw new CorruptEncodingException("Corrupt encoded string data (source offset = "
                        + (srcOffset - 1) + ')');
                }
            }

            valueRef[0] = new String(value);

            return srcOffset - originalOffset;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a length value which was encoded by
     * {@link DataEncoder#writeLength}.
     *
     * @return length value
     * @since 1.2
     */
    public static int readLength(InputStream in) throws IOException, EOFException {
        int b0 = in.read();
        if (b0 < 0) {
            throw new EOFException();
        }
        if (b0 <= 0x7f) {
            return b0;
        }
        int b1 = in.read();
        if (b1 < 0) {
            throw new EOFException();
        }
        if (b0 <= 0xbf) {
            return ((b0 & 0x3f) << 8) | b1;
        }
        int b2 = in.read();
        if (b2 < 0) {
            throw new EOFException();
        }
        if (b0 <= 0xdf) {
            return ((b0 & 0x1f) << 16) | (b1 << 8) | b2;
        }
        int b3 = in.read();
        if (b3 < 0) {
            throw new EOFException();
        }
        if (b0 <= 0xef) {
            return ((b0 & 0x0f) << 24) | (b1 << 16) | (b2 << 8) | b3;
        }
        int b4 = in.read();
        if (b4 < 0) {
            throw new EOFException();
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    /**
     * Reads as many bytes from the stream as is necessary to fill the given
     * byte array. An EOFException is thrown if the stream end is encountered.
     *
     * @since 1.2
     */
    public static void readFully(InputStream in, byte[] b) throws IOException, EOFException {
        final int length = b.length;
        int total = 0;
        while (total < length) {
            int amt = in.read(b, total, length - total);
            if (amt < 0) {
                throw new EOFException();
            }
            total += amt;
        }
    }

    /**
     * Decodes the given byte array which was encoded by
     * {@link DataEncoder#encodeSingle}. Always returns a new byte array
     * instance.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded
     * byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte
     * array
     */
    public static byte[] decodeSingle(byte[] src, int prefixPadding, int suffixPadding)
        throws CorruptEncodingException {
        try {
            int length = src.length - suffixPadding - prefixPadding;
            if (length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            if (prefixPadding <= 0 && suffixPadding <= 0) {
                // Always return a new byte array instance
                return src.clone();
            }
            byte[] dst = new byte[length];
            System.arraycopy(src, prefixPadding, dst, 0, length);
            return dst;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array which was encoded by
     * {@link DataEncoder#encodeSingleNullable}. Always returns a new byte array
     * instance.
     */
    public static byte[] decodeSingleNullable(byte[] src) throws CorruptEncodingException {
        return decodeSingleNullable(src, 0, 0);
    }

    /**
     * Decodes the given byte array which was encoded by
     * {@link DataEncoder#encodeSingleNullable}. Always returns a new byte array
     * instance.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded
     * byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte
     * array
     */
    public static byte[] decodeSingleNullable(byte[] src, int prefixPadding, int suffixPadding)
        throws CorruptEncodingException {
        try {
            byte b = src[prefixPadding];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            int length = src.length - suffixPadding - 1 - prefixPadding;
            if (length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            byte[] value = new byte[length];
            System.arraycopy(src, 1 + prefixPadding, value, 0, length);
            return value;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }
}
