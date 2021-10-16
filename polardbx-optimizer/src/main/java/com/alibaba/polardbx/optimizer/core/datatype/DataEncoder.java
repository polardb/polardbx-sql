/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.datatype;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NOT_NULL_BYTE_HIGH;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NULL_BYTE_LOW;

/**
 * A very low-level class that supports encoding of primitive data. For encoding
 * data into keys, see {@link KeyEncoder}.
 *
 * @author Brian S O'Neill
 * @see DataDecoder
 */
public class DataEncoder {

    // Note: Most of these methods are also used by KeyEncoder, which is why
    // they are encoded for supporting proper ordering.

    /**
     * Encodes the given signed integer into exactly 4 bytes.
     *
     * @param value signed integer value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(int value, byte[] dst, int dstOffset) {
        value ^= 0x80000000;
        dst[dstOffset] = (byte) (value >> 24);
        dst[dstOffset + 1] = (byte) (value >> 16);
        dst[dstOffset + 2] = (byte) (value >> 8);
        dst[dstOffset + 3] = (byte) value;
    }

    /**
     * Encodes the given signed Integer object into exactly 1 or 5 bytes. If the
     * Integer object is never expected to be null, consider encoding as an int
     * primitive.
     *
     * @param value optional signed Integer value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(Integer value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {

            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            encode(value.intValue(), dst, dstOffset + 1);
            return 5;

        }
    }

    /**
     * Encodes the given signed long into exactly 8 bytes.
     *
     * @param value signed long value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(long value, byte[] dst, int dstOffset) {
        int w = ((int) (value >> 32)) ^ 0x80000000;
        dst[dstOffset] = (byte) (w >> 24);
        dst[dstOffset + 1] = (byte) (w >> 16);
        dst[dstOffset + 2] = (byte) (w >> 8);
        dst[dstOffset + 3] = (byte) w;
        w = (int) value;
        dst[dstOffset + 4] = (byte) (w >> 24);
        dst[dstOffset + 5] = (byte) (w >> 16);
        dst[dstOffset + 6] = (byte) (w >> 8);
        dst[dstOffset + 7] = (byte) w;
    }

    /**
     * Encodes the given signed Long object into exactly 1 or 9 bytes. If the
     * Long object is never expected to be null, consider encoding as a long
     * primitive.
     *
     * @param value optional signed Long value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(Long value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            encode(value.longValue(), dst, dstOffset + 1);
            return 9;
        }
    }

    /**
     * Encodes the given signed byte into exactly 1 byte.
     *
     * @param value signed byte value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(byte value, byte[] dst, int dstOffset) {
        dst[dstOffset] = (byte) (value ^ 0x80);
    }

    /**
     * Encodes the given signed Byte object into exactly 1 or 2 bytes. If the
     * Byte object is never expected to be null, consider encoding as a byte
     * primitive.
     *
     * @param value optional signed Byte value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(Byte value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            dst[dstOffset + 1] = (byte) (value ^ 0x80);
            return 2;
        }
    }

    /**
     * Encodes the given signed short into exactly 2 bytes.
     *
     * @param value signed short value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(short value, byte[] dst, int dstOffset) {
        value ^= 0x8000;
        dst[dstOffset] = (byte) (value >> 8);
        dst[dstOffset + 1] = (byte) value;
    }

    /**
     * Encodes the given signed Short object into exactly 1 or 3 bytes. If the
     * Short object is never expected to be null, consider encoding as a short
     * primitive.
     *
     * @param value optional signed Short value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(Short value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            encode(value.shortValue(), dst, dstOffset + 1);
            return 3;
        }
    }

    /**
     * Encodes the given character into exactly 2 bytes.
     *
     * @param value character value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(char value, byte[] dst, int dstOffset) {
        dst[dstOffset] = (byte) (value >> 8);
        dst[dstOffset + 1] = (byte) value;
    }

    /**
     * Encodes the given Character object into exactly 1 or 3 bytes. If the
     * Character object is never expected to be null, consider encoding as a
     * char primitive.
     *
     * @param value optional Character value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(Character value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            encode(value.charValue(), dst, dstOffset + 1);
            return 3;
        }
    }

    /**
     * Encodes the given boolean into exactly 1 byte.
     *
     * @param value boolean value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(boolean value, byte[] dst, int dstOffset) {
        dst[dstOffset] = value ? (byte) 128 : (byte) 127;
    }

    /**
     * Encodes the given Boolean object into exactly 1 byte.
     *
     * @param value optional Boolean value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(Boolean value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
        } else {
            dst[dstOffset] = value.booleanValue() ? (byte) 128 : (byte) 127;
        }
    }

    /**
     * Encodes the given float into exactly 4 bytes.
     *
     * @param value float value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(float value, byte[] dst, int dstOffset) {
        int bits = Float.floatToIntBits(value);
        bits ^= (bits < 0) ? 0xffffffff : 0x80000000;
        dst[dstOffset] = (byte) (bits >> 24);
        dst[dstOffset + 1] = (byte) (bits >> 16);
        dst[dstOffset + 2] = (byte) (bits >> 8);
        dst[dstOffset + 3] = (byte) bits;
    }

    /**
     * Encodes the given Float object into exactly 4 bytes. A non-canonical NaN
     * value is used to represent null.
     *
     * @param value optional Float value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(Float value, byte[] dst, int dstOffset) {
        if (value == null) {
            encode(0x7fffffff, dst, dstOffset);
        } else {
            encode(value.floatValue(), dst, dstOffset);
        }
    }

    /**
     * Encodes the given double into exactly 8 bytes.
     *
     * @param value double value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(double value, byte[] dst, int dstOffset) {
        long bits = Double.doubleToLongBits(value);
        bits ^= (bits < 0) ? 0xffffffffffffffffL : 0x8000000000000000L;
        int w = (int) (bits >> 32);
        dst[dstOffset] = (byte) (w >> 24);
        dst[dstOffset + 1] = (byte) (w >> 16);
        dst[dstOffset + 2] = (byte) (w >> 8);
        dst[dstOffset + 3] = (byte) w;
        w = (int) bits;
        dst[dstOffset + 4] = (byte) (w >> 24);
        dst[dstOffset + 5] = (byte) (w >> 16);
        dst[dstOffset + 6] = (byte) (w >> 8);
        dst[dstOffset + 7] = (byte) w;
    }

    /**
     * Encodes the given Double object into exactly 8 bytes. A non-canonical NaN
     * value is used to represent null.
     *
     * @param value optional Double value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encode(Double value, byte[] dst, int dstOffset) {
        if (value == null) {
            encode(0x7fffffffffffffffL, dst, dstOffset);
        } else {
            encode(value.doubleValue(), dst, dstOffset);
        }
    }

    /**
     * Encodes the given optional BigInteger into a variable amount of bytes. If
     * the BigInteger is null, exactly 1 byte is written. Otherwise, the amount
     * written can be determined by calling calculateEncodedLength.
     *
     * @param value BigInteger value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     * @since 1.2
     */
    public static int encode(BigInteger value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }

        byte[] bytes = value.toByteArray();

        // Write the byte array length first, in a variable amount of bytes.
        int amt = encodeUnsignedVarInt(bytes.length, dst, dstOffset);

        // Now write the byte array.
        System.arraycopy(bytes, 0, dst, dstOffset + amt, bytes.length);

        return amt + bytes.length;
    }

    /**
     * Returns the amount of bytes required to encode the given BigInteger.
     *
     * @param value BigInteger value to encode, may be null
     * @return amount of bytes needed to encode
     * @since 1.2
     */
    public static int calculateEncodedLength(BigInteger value) {
        if (value == null) {
            return 1;
        }
        int byteCount = (value.bitLength() >> 3) + 1;
        return unsignedVarIntLength(byteCount) + byteCount;
    }

    /**
     * Encodes the given optional BigDecimal into a variable amount of bytes. If
     * the BigDecimal is null, exactly 1 byte is written. Otherwise, the amount
     * written can be determined by calling calculateEncodedLength.
     *
     * @param value BigDecimal value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     * @since 1.2
     */
    public static int encode(BigDecimal value, byte[] dst, int dstOffset) {
        int amt = 0;
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            amt += 1;
            amt += encode((BigInteger) null, dst, dstOffset + amt);
            return amt;
        }
        amt = encodeSignedVarInt(value.scale(), dst, dstOffset);
        return amt + encode(value.unscaledValue(), dst, dstOffset + amt);
    }

    /**
     * Returns the amount of bytes required to encode the given BigDecimal.
     *
     * @param value BigDecimal value to encode, may be null
     * @return amount of bytes needed to encode
     * @since 1.2
     */
    public static int calculateEncodedLength(BigDecimal value) {
        if (value == null) {
            return 1;
        }
        return signedVarIntLength(value.scale()) + calculateEncodedLength(value.unscaledValue());
    }

    /**
     * Encodes the given optional byte array into a variable amount of bytes. If
     * the byte array is null, exactly 1 byte is written. Otherwise, the amount
     * written can be determined by calling calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(byte[] value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }
        return encode(value, 0, value.length, dst, dstOffset);
    }

    /**
     * Encodes the given optional byte array into a variable amount of bytes. If
     * the byte array is null, exactly 1 byte is written. Otherwise, the amount
     * written can be determined by calling calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param valueOffset offset into byte array
     * @param valueLength length of data in byte array
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(byte[] value, int valueOffset, int valueLength, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }

        // Write the value length first, in a variable amount of bytes.
        int amt = encodeUnsignedVarInt(valueLength, dst, dstOffset);

        // Now write the value.
        System.arraycopy(value, valueOffset, dst, dstOffset + amt, valueLength);

        return amt + valueLength;
    }

    /**
     * Returns the amount of bytes required to encode the given byte array.
     *
     * @param value byte array value to encode, may be null
     * @return amount of bytes needed to encode
     */
    public static int calculateEncodedLength(byte[] value) {
        return value == null ? 1 : calculateEncodedLength(value, 0, value.length);
    }

    /**
     * Returns the amount of bytes required to encode the given byte array.
     *
     * @param value byte array value to encode, may be null
     * @param valueOffset offset into byte array
     * @param valueLength length of data in byte array
     * @return amount of bytes needed to encode
     */
    public static int calculateEncodedLength(byte[] value, int valueOffset, int valueLength) {
        return value == null ? 1 : (unsignedVarIntLength(valueLength) + valueLength);
    }

    public static int encode(String value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {

            dst[dstOffset] = NOT_NULL_BYTE_HIGH;
            return encodeString(value, dst, dstOffset + 1) + 1;

        }
    }

    /**
     * Encodes the given optional String into a variable amount of bytes. The
     * amount written can be determined by calling calculateEncodedStringLength.
     * <p>
     * Strings are encoded in a fashion similar to UTF-8, in that ASCII
     * characters are written in one byte. This encoding is more efficient than
     * UTF-8, but it isn't compatible with UTF-8.
     *
     * @param value String value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeString(String value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }
        final int originalOffset = dstOffset;

        int valueLength = value.length();

        // Write the value length first, in a variable amount of bytes.
        dstOffset += encodeUnsignedVarInt(valueLength, dst, dstOffset);

        for (int i = 0; i < valueLength; i++) {
            int c = value.charAt(i);
            if (c <= 0x7f) {
                dst[dstOffset++] = (byte) c;
            } else if (c <= 0x3fff) {
                dst[dstOffset++] = (byte) (0x80 | (c >> 8));
                dst[dstOffset++] = (byte) (c & 0xff);
            } else {
                if (c >= 0xd800 && c <= 0xdbff) {
                    // Found a high surrogate. Verify that surrogate pair is
                    // well-formed. Low surrogate must follow high surrogate.
                    if (i + 1 < valueLength) {
                        int c2 = value.charAt(i + 1);
                        if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                            c = 0x10000 + (((c & 0x3ff) << 10) | (c2 & 0x3ff));
                            i++;
                        }
                    }
                }
                dst[dstOffset++] = (byte) (0xc0 | (c >> 16));
                dst[dstOffset++] = (byte) ((c >> 8) & 0xff);
                dst[dstOffset++] = (byte) (c & 0xff);
            }
        }

        return dstOffset - originalOffset;
    }

    /**
     * Returns the amount of bytes required to encode the given String.
     *
     * @param value String to encode, may be null
     */
    public static int calculateEncodedStringLength(String value) {
        if (value == null) {
            return 1;
        }

        int valueLength = value.length();
        int encodedLen = unsignedVarIntLength(valueLength);

        for (int i = 0; i < valueLength; i++) {
            int c = value.charAt(i);
            if (c <= 0x7f) {
                encodedLen++;
            } else if (c <= 0x3fff) {
                encodedLen += 2;
            } else {
                if (c >= 0xd800 && c <= 0xdbff) {
                    // Found a high surrogate. Verify that surrogate pair is
                    // well-formed. Low surrogate must follow high surrogate.
                    if (i + 1 < valueLength) {
                        int c2 = value.charAt(i + 1);
                        if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                            i++;
                        }
                    }
                }
                encodedLen += 3;
            }
        }
        encodedLen++;
        return encodedLen;
    }

    private static int encodeUnsignedVarInt(int value, byte[] dst, int dstOffset) {
        if (value < 128) {
            dst[dstOffset] = (byte) value;
            return 1;
        } else if (value < 16384) {
            dst[dstOffset++] = (byte) ((value >> 8) | 0x80);
            dst[dstOffset] = (byte) value;
            return 2;
        } else if (value < 2097152) {
            dst[dstOffset++] = (byte) ((value >> 16) | 0xc0);
            dst[dstOffset++] = (byte) (value >> 8);
            dst[dstOffset] = (byte) value;
            return 3;
        } else if (value < 268435456) {
            dst[dstOffset++] = (byte) ((value >> 24) | 0xe0);
            dst[dstOffset++] = (byte) (value >> 16);
            dst[dstOffset++] = (byte) (value >> 8);
            dst[dstOffset] = (byte) value;
            return 4;
        } else {
            dst[dstOffset++] = (byte) 0xf0;
            dst[dstOffset++] = (byte) (value >> 24);
            dst[dstOffset++] = (byte) (value >> 16);
            dst[dstOffset++] = (byte) (value >> 8);
            dst[dstOffset] = (byte) value;
            return 5;
        }
    }

    private static int unsignedVarIntLength(int value) {
        if (value < 128) {
            return 1;
        } else if (value < 16384) {
            return 2;
        } else if (value < 2097152) {
            return 3;
        } else if (value < 268435456) {
            return 4;
        } else {
            return 5;
        }
    }

    private static int encodeSignedVarInt(int value, byte[] dst, int dstOffset) {
        value = (value < 0 ? (((~value) << 1) | 1) : (value << 1));
        if (value < 0) {
            dst[dstOffset++] = (byte) 0xf0;
            dst[dstOffset++] = (byte) (value >> 24);
            dst[dstOffset++] = (byte) (value >> 16);
            dst[dstOffset++] = (byte) (value >> 8);
            dst[dstOffset] = (byte) value;
            return 5;
        } else {
            return encodeUnsignedVarInt(value, dst, dstOffset);
        }
    }

    private static int signedVarIntLength(int value) {
        value = (value < 0 ? ~value : value) << 1;
        return value < 0 ? 5 : unsignedVarIntLength(value);
    }

    /**
     * Writes a positive length value in up to five bytes.
     *
     * @return number of bytes written
     * @since 1.2
     */
    public static int writeLength(int valueLength, OutputStream out) throws IOException {
        if (valueLength < 128) {
            out.write(valueLength);
            return 1;
        } else if (valueLength < 16384) {
            out.write((valueLength >> 8) | 0x80);
            out.write(valueLength);
            return 2;
        } else if (valueLength < 2097152) {
            out.write((valueLength >> 16) | 0xc0);
            out.write(valueLength >> 8);
            out.write(valueLength);
            return 3;
        } else if (valueLength < 268435456) {
            out.write((valueLength >> 24) | 0xe0);
            out.write(valueLength >> 16);
            out.write(valueLength >> 8);
            out.write(valueLength);
            return 4;
        } else {
            out.write(0xf0);
            out.write(valueLength >> 24);
            out.write(valueLength >> 16);
            out.write(valueLength >> 8);
            out.write(valueLength);
            return 5;
        }
    }

    /**
     * Encodes the given byte array for use when there is only a single
     * property, whose type is a byte array. The original byte array is returned
     * if the padding lengths are zero.
     *
     * @param prefixPadding amount of extra bytes to allocate at start of
     * encoded byte array
     * @param suffixPadding amount of extra bytes to allocate at end of encoded
     * byte array
     */
    public static byte[] encodeSingle(byte[] value, int prefixPadding, int suffixPadding) {
        if (prefixPadding <= 0 && suffixPadding <= 0) {
            return value;
        }
        int length = value.length;
        byte[] dst = new byte[prefixPadding + length + suffixPadding];
        System.arraycopy(value, 0, dst, prefixPadding, length);
        return dst;
    }

    /**
     * Encodes the given byte array for use when there is only a single nullable
     * property, whose type is a byte array.
     */
    public static byte[] encodeSingleNullable(byte[] value) {
        return encodeSingleNullable(value, 0, 0);
    }

    /**
     * Encodes the given byte array for use when there is only a single nullable
     * property, whose type is a byte array.
     *
     * @param prefixPadding amount of extra bytes to allocate at start of
     * encoded byte array
     * @param suffixPadding amount of extra bytes to allocate at end of encoded
     * byte array
     */
    public static byte[] encodeSingleNullable(byte[] value, int prefixPadding, int suffixPadding) {
        if (prefixPadding <= 0 && suffixPadding <= 0) {
            if (value == null) {
                return new byte[] {NULL_BYTE_LOW};
            }

            int length = value.length;
            if (length == 0) {
                return new byte[] {NOT_NULL_BYTE_HIGH};
            }

            byte[] dst = new byte[1 + length];
            dst[0] = NOT_NULL_BYTE_HIGH;
            System.arraycopy(value, 0, dst, 1, length);
            return dst;
        }

        if (value == null) {
            byte[] dst = new byte[prefixPadding + 1 + suffixPadding];
            dst[prefixPadding] = NULL_BYTE_LOW;
            return dst;
        }

        int length = value.length;
        byte[] dst = new byte[prefixPadding + 1 + length + suffixPadding];
        dst[prefixPadding] = NOT_NULL_BYTE_HIGH;
        System.arraycopy(value, 0, dst, prefixPadding + 1, length);
        return dst;
    }
}
