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

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NOT_NULL_BYTE_LOW;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NULL_BYTE_HIGH;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.NULL_BYTE_LOW;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.ONE_HUNDRED;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.ONE_THOUSAND;
import static com.alibaba.polardbx.optimizer.core.datatype.EncodingConstants.TERMINATOR;

/**
 * A very low-level class that supports encoding of primitive data into unique,
 * sortable byte array keys. If the data to encode is of a variable size, then
 * it is written in base-32768, using only byte values 32..223. This allows
 * special values such as nulls and terminators to be unambiguously encoded.
 * Terminators for variable data can be encoded using 1 for ascending order and
 * 254 for descending order. Nulls can be encoded as 255 for high ordering and 0
 * for low ordering.
 *
 * @author Brian S O'Neill
 * @see KeyDecoder
 * @see DataEncoder
 */
public class KeyEncoder {

    /**
     * Encodes the given signed integer into exactly 4 bytes for descending
     * order.
     *
     * @param value signed integer value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(int value, byte[] dst, int dstOffset) {
        DataEncoder.encode(~value, dst, dstOffset);
    }

    /**
     * Encodes the given signed Integer object into exactly 1 or 5 bytes for
     * descending order. If the Integer object is never expected to be null,
     * consider encoding as an int primitive.
     *
     * @param value optional signed Integer value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(Integer value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_LOW;
            DataEncoder.encode(~value.intValue(), dst, dstOffset + 1);
            return 5;
        }
    }

    /**
     * Encodes the given signed long into exactly 8 bytes for descending order.
     *
     * @param value signed long value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(long value, byte[] dst, int dstOffset) {
        DataEncoder.encode(~value, dst, dstOffset);
    }

    /**
     * Encodes the given signed Long object into exactly 1 or 9 bytes for
     * descending order. If the Long object is never expected to be null,
     * consider encoding as a long primitive.
     *
     * @param value optional signed Long value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(Long value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_LOW;
            DataEncoder.encode(~value.longValue(), dst, dstOffset + 1);
            return 9;
        }
    }

    /**
     * Encodes the given signed byte into exactly 1 byte for descending order.
     *
     * @param value signed byte value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(byte value, byte[] dst, int dstOffset) {
        dst[dstOffset] = (byte) (value ^ 0x7f);
    }

    /**
     * Encodes the given signed Byte object into exactly 1 or 2 bytes for
     * descending order. If the Byte object is never expected to be null,
     * consider encoding as a byte primitive.
     *
     * @param value optional signed Byte value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(Byte value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_LOW;
            dst[dstOffset + 1] = (byte) (value ^ 0x7f);
            return 2;
        }
    }

    /**
     * Encodes the given signed short into exactly 2 bytes for descending order.
     *
     * @param value signed short value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(short value, byte[] dst, int dstOffset) {
        DataEncoder.encode((short) ~value, dst, dstOffset);
    }

    /**
     * Encodes the given signed Short object into exactly 1 or 3 bytes for
     * descending order. If the Short object is never expected to be null,
     * consider encoding as a short primitive.
     *
     * @param value optional signed Short value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(Short value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_LOW;
            DataEncoder.encode((short) ~value.shortValue(), dst, dstOffset + 1);
            return 3;
        }
    }

    /**
     * Encodes the given character into exactly 2 bytes for descending order.
     *
     * @param value character value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(char value, byte[] dst, int dstOffset) {
        DataEncoder.encode((char) ~value, dst, dstOffset);
    }

    /**
     * Encodes the given Character object into exactly 1 or 3 bytes for
     * descending order. If the Character object is never expected to be null,
     * consider encoding as a char primitive.
     *
     * @param value optional Character value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(Character value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        } else {
            dst[dstOffset] = NOT_NULL_BYTE_LOW;
            DataEncoder.encode((char) ~value.charValue(), dst, dstOffset + 1);
            return 3;
        }
    }

    /**
     * Encodes the given boolean into exactly 1 byte for descending order.
     *
     * @param value boolean value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(boolean value, byte[] dst, int dstOffset) {
        dst[dstOffset] = value ? (byte) 127 : (byte) 128;
    }

    /**
     * Encodes the given Boolean object into exactly 1 byte for descending
     * order.
     *
     * @param value optional Boolean value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(Boolean value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
        } else {
            dst[dstOffset] = value.booleanValue() ? (byte) 127 : (byte) 128;
        }
    }

    /**
     * Encodes the given float into exactly 4 bytes for descending order.
     *
     * @param value float value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(float value, byte[] dst, int dstOffset) {
        int bits = Float.floatToIntBits(value);
        if (bits >= 0) {
            bits ^= 0x7fffffff;
        }
        dst[dstOffset] = (byte) (bits >> 24);
        dst[dstOffset + 1] = (byte) (bits >> 16);
        dst[dstOffset + 2] = (byte) (bits >> 8);
        dst[dstOffset + 3] = (byte) bits;
    }

    /**
     * Encodes the given Float object into exactly 4 bytes for descending order.
     * A non-canonical NaN value is used to represent null.
     *
     * @param value optional Float value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(Float value, byte[] dst, int dstOffset) {
        if (value == null) {
            DataEncoder.encode(~0x7fffffff, dst, dstOffset);
        } else {
            encodeDesc(value.floatValue(), dst, dstOffset);
        }
    }

    /**
     * Encodes the given double into exactly 8 bytes for descending order.
     *
     * @param value double value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(double value, byte[] dst, int dstOffset) {
        long bits = Double.doubleToLongBits(value);
        if (bits >= 0) {
            bits ^= 0x7fffffffffffffffL;
        }
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
     * Encodes the given Double object into exactly 8 bytes for descending
     * order. A non-canonical NaN value is used to represent null.
     *
     * @param value optional Double value to encode
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     */
    public static void encodeDesc(Double value, byte[] dst, int dstOffset) {
        if (value == null) {
            DataEncoder.encode(~0x7fffffffffffffffL, dst, dstOffset);
        } else {
            encodeDesc(value.doubleValue(), dst, dstOffset);
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
        /*
         * Encoding of first byte: 0x00: null low (unused) 0x01: negative
         * signum; four bytes follow for value length 0x02..0x7f: negative
         * signum; value length 7e range, 1..126 0x80..0xfd: positive signum;
         * value length 7e range, 1..126 0xfe: positive signum; four bytes
         * follow for value length 0xff: null high
         */

        if (value == null) {
            dst[dstOffset] = NULL_BYTE_HIGH;
            return 1;
        }

        byte[] bytes = value.toByteArray();
        // Always at least one.
        int bytesLength = bytes.length;

        int headerSize;
        if (bytesLength < 0x7f) {
            if (value.signum() < 0) {
                dst[dstOffset] = (byte) (0x80 - bytesLength);
            } else {
                dst[dstOffset] = (byte) (bytesLength + 0x7f);
            }
            headerSize = 1;
        } else {
            dst[dstOffset] = (byte) (value.signum() < 0 ? 1 : 0xfe);
            int encodedLen = value.signum() < 0 ? -bytesLength : bytesLength;
            DataEncoder.encode(encodedLen, dst, dstOffset + 1);
            headerSize = 5;
        }

        System.arraycopy(bytes, 0, dst, headerSize + dstOffset, bytesLength);

        return headerSize + bytesLength;
    }

    /**
     * Encodes the given optional BigInteger into a variable amount of bytes for
     * descending order. If the BigInteger is null, exactly 1 byte is written.
     * Otherwise, the amount written can be determined by calling
     * calculateEncodedLength.
     *
     * @param value BigInteger value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     * @since 1.2
     */
    public static int encodeDesc(BigInteger value, byte[] dst, int dstOffset) {
        /*
         * Encoding of first byte: 0x00: null high (unused) 0x01: positive
         * signum; four bytes follow for value length 0x02..0x7f: positive
         * signum; value length 7e range, 1..126 0x80..0xfd: negative signum;
         * value length 7e range, 1..126 0xfe: negative signum; four bytes
         * follow for value length 0xff: null low
         */

        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }

        byte[] bytes = value.toByteArray();
        // Always at least one.
        int bytesLength = bytes.length;

        int headerSize;
        if (bytesLength < 0x7f) {
            if (value.signum() < 0) {
                dst[dstOffset] = (byte) (bytesLength + 0x7f);
            } else {
                dst[dstOffset] = (byte) (0x80 - bytesLength);
            }
            headerSize = 1;
        } else {
            dst[dstOffset] = (byte) (value.signum() < 0 ? 0xfe : 1);
            int encodedLen = value.signum() < 0 ? bytesLength : -bytesLength;
            DataEncoder.encode(encodedLen, dst, dstOffset + 1);
            headerSize = 5;
        }

        dstOffset += headerSize;
        for (int i = 0; i < bytesLength; i++) {
            dst[dstOffset + i] = (byte) ~bytes[i];
        }

        return headerSize + bytesLength;
    }

    /**
     * Returns the amount of bytes required to encode a BigInteger.
     *
     * @param value BigInteger value to encode, may be null
     * @return amount of bytes needed to encode
     * @since 1.2
     */
    public static int calculateEncodedLength(BigInteger value) {
        if (value == null) {
            return 1;
        }
        int bytesLength = (value.bitLength() >> 3) + 1;
        return bytesLength < 0x7f ? (1 + bytesLength) : (5 + bytesLength);
    }

    /**
     * Encodes the given optional BigDecimal into a variable amount of bytes. If
     * the BigDecimal is null, exactly 1 byte is written. Otherwise, the amount
     * written can be determined by calling calculateEncodedLength.
     * <p>
     * <i>Note:</i> It is recommended that value be normalized by stripping
     * trailing zeros. This makes searching by value much simpler.
     *
     * @param value BigDecimal value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     * @since 1.2
     */
    public static int encode(BigDecimal value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_HIGH;
            return 1;
        }

        if (value.signum() == 0) {
            dst[dstOffset] = (byte) 0x80;
            return 1;
        }

        return encode(value).copyTo(dst, dstOffset);
    }

    /**
     * Encodes the given optional BigDecimal into a variable amount of bytes for
     * descending order. If the BigDecimal is null, exactly 1 byte is written.
     * Otherwise, the amount written can be determined by calling
     * calculateEncodedLength.
     * <p>
     * <i>Note:</i> It is recommended that value be normalized by stripping
     * trailing zeros. This makes searching by value much simpler.
     *
     * @param value BigDecimal value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     * @since 1.2
     */
    public static int encodeDesc(BigDecimal value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }

        if (value.signum() == 0) {
            dst[dstOffset] = (byte) 0x7f;
            return 1;
        }

        return encode(value).copyDescTo(dst, dstOffset);
    }

    /**
     * Returns the amount of bytes required to encode a BigDecimal.
     * <p>
     * <i>Note:</i> It is recommended that value be normalized by stripping
     * trailing zeros. This makes searching by value much simpler.
     *
     * @param value BigDecimal value to encode, may be null
     * @return amount of bytes needed to encode
     * @since 1.2
     */
    public static int calculateEncodedLength(BigDecimal value) {
        if (value == null || value.signum() == 0) {
            return 1;
        }

        return encode(value).mLength;
    }

    private static class CachedBigDecimal {

        static final ThreadLocal<CachedBigDecimal> cLocal = new ThreadLocal<CachedBigDecimal>();

        final BigDecimal mValue;
        final byte[] mEncoded;
        final int mLength;

        CachedBigDecimal(BigDecimal value, byte[] encoded, int length) {
            mValue = value;
            mEncoded = encoded;
            mLength = length;
        }

        int copyTo(byte[] dst, int dstOffset) {
            int length = mLength;
            System.arraycopy(mEncoded, 0, dst, dstOffset, length);
            return length;
        }

        int copyDescTo(byte[] dst, int dstOffset) {
            byte[] encoded = mEncoded;
            int length = mLength;
            for (int i = 0; i < length; i++) {
                dst[dstOffset++] = (byte) ~encoded[i];
            }
            return length;
        }
    }

    /**
     * @param value cannot be null or zero
     * @return non-null cached encoding
     */
    private static CachedBigDecimal encode(BigDecimal value) {
        CachedBigDecimal cached = CachedBigDecimal.cLocal.get();
        if (cached != null && cached.mValue.equals(value)) {
            return cached;
        }

        // Exactly predicting encoding length is hard. Instead, overestimate
        // and compare with actual encoded result. Result is cached to avoid
        // recomputation.

        // 5: maximum header encoding length
        // 1: extra byte for last digit
        // 10: bits for rare extra digit
        // 10: bits for terminator digit
        int length = (5 + 1) + (((value.unscaledValue().bitLength() + (10 + 10)) + 7) >> 3);

        byte[] encoded = new byte[length];
        length = encodeUncached(value, encoded);

        cached = new CachedBigDecimal(value, encoded, length);
        CachedBigDecimal.cLocal.set(cached);

        return cached;
    }

    /**
     * @param value cannot be null or zero
     */
    private static int encodeUncached(BigDecimal value, byte[] dst) {
        /*
         * Encoding of header: 0x00: null low (unused) 0x01: negative signum;
         * four bytes follow for positive exponent 0x02..0x3f: negative signum;
         * positive exponent; 3e range, 61..0 0x40..0x7d: negative signum;
         * negative exponent; 3e range, -1..-62 0x7e: negative signum; four
         * bytes follow for negative exponent 0x7f: negative zero (unused) 0x80:
         * zero 0x81: positive signum; four bytes follow for negative exponent
         * 0x82..0xbf: positive signum; negative exponent; 3e range, -62..-1
         * 0xc0..0xfd: positive signum; positive exponent; 3e range, 0..61 0xfe:
         * positive signum; four bytes follow for positive exponent 0xff: null
         * high
         */

        int dstOffset = 0;
        int precision = value.precision();
        int exponent = precision - value.scale();

        if (value.signum() < 0) {
            if (exponent >= -0x3e && exponent < 0x3e) {
                dst[dstOffset++] = (byte) (0x3f - exponent);
            } else {
                if (exponent < 0) {
                    dst[dstOffset] = (byte) 0x7e;
                } else {
                    dst[dstOffset] = (byte) 1;
                }
                DataEncoder.encode(~exponent, dst, dstOffset + 1);
                dstOffset += 5;
            }
        } else {
            if (exponent >= -0x3e && exponent < 0x3e) {
                dst[dstOffset++] = (byte) (exponent + 0xc0);
            } else {
                if (exponent < 0) {
                    dst[dstOffset] = (byte) 0x81;
                } else {
                    dst[dstOffset] = (byte) 0xfe;
                }
                DataEncoder.encode(exponent, dst, dstOffset + 1);
                dstOffset += 5;
            }
        }

        // Significand must be decimal encoded to maintain proper sort order.
        // Base 1000 is more efficient than base 10 and still maintains proper
        // sort order. A minimum of two bytes must be generated, however.

        BigInteger unscaledValue = value.unscaledValue();

        // Ensure a non-fractional amount of base 1000 digits.
        int terminator;
        switch (precision % 3) {
        case 0:
        default:
            terminator = 2;
            break;
        case 1:
            terminator = 0;
            unscaledValue = unscaledValue.multiply(ONE_HUNDRED);
            break;
        case 2:
            terminator = 1;
            unscaledValue = unscaledValue.multiply(BigInteger.TEN);
            break;
        }

        // 10 bits per digit and 1 extra terminator digit. Digit values 0..999
        // are encoded as 12..1011. Digit values 0..11 and 1012..1023 are used
        // for terminators.

        int digitAdjust;
        if (unscaledValue.signum() >= 0) {
            digitAdjust = 12;
        } else {
            digitAdjust = 999 + 12;
            terminator = 1023 - terminator;
        }

        int pos = ((unscaledValue.bitLength() + 9) / 10) + 1;
        int[] digits = new int[pos];
        digits[--pos] = terminator;

        while (unscaledValue.signum() != 0) {
            BigInteger[] divrem = unscaledValue.divideAndRemainder(ONE_THOUSAND);

            if (--pos < 0) {
                // Handle rare case when an extra digit is required.
                int[] newDigits = new int[digits.length + 1];
                System.arraycopy(digits, 0, newDigits, 1, digits.length);
                digits = newDigits;
                pos = 0;
            }

            digits[pos] = divrem[1].intValue() + digitAdjust;

            unscaledValue = divrem[0];
        }

        // Now encode digits in proper order, 10 bits per digit. 1024 possible
        // values per 10 bits, and so base 1000 is quite efficient.

        int accum = 0;
        int bits = 0;
        for (int i = 0; i < digits.length; i++) {
            accum = (accum << 10) | digits[i];
            bits += 10;
            do {
                dst[dstOffset++] = (byte) (accum >> (bits -= 8));
            } while (bits >= 8);
        }

        if (bits != 0) {
            dst[dstOffset++] = (byte) (accum << (8 - bits));
        }

        return dstOffset;
    }

    /**
     * Encodes the given optional unsigned byte array into a variable amount of
     * bytes. If the byte array is null, exactly 1 byte is written. Otherwise,
     * the amount written can be determined by calling calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(byte[] value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_HIGH;
            return 1;
        }
        return encode(value, 0, value.length, dst, dstOffset, 0);
    }

    /**
     * Encodes the given optional unsigned byte array into a variable amount of
     * bytes. If the byte array is null, exactly 1 byte is written. Otherwise,
     * the amount written can be determined by calling calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param valueOffset offset into byte array
     * @param valueLength length of data in byte array
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(byte[] value, int valueOffset, int valueLength, byte[] dst, int dstOffset) {
        return encode(value, valueOffset, valueLength, dst, dstOffset, 0);
    }

    /**
     * Encodes the given optional unsigned byte array into a variable amount of
     * bytes for descending order. If the byte array is null, exactly 1 byte is
     * written. Otherwise, the amount written is determined by calling
     * calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(byte[] value, byte[] dst, int dstOffset) {
        if (value == null) {
            dst[dstOffset] = NULL_BYTE_LOW;
            return 1;
        }
        return encode(value, 0, value.length, dst, dstOffset, -1);
    }

    /**
     * Encodes the given optional unsigned byte array into a variable amount of
     * bytes for descending order. If the byte array is null, exactly 1 byte is
     * written. Otherwise, the amount written is determined by calling
     * calculateEncodedLength.
     *
     * @param value byte array value to encode, may be null
     * @param valueOffset offset into byte array
     * @param valueLength length of data in byte array
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(byte[] value, int valueOffset, int valueLength, byte[] dst, int dstOffset) {
        return encode(value, valueOffset, valueLength, dst, dstOffset, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static int encode(byte[] value, int valueOffset, int valueLength, byte[] dst, int dstOffset, int xorMask) {
        if (value == null) {
            dst[dstOffset] = (byte) (NULL_BYTE_HIGH ^ xorMask);
            return 1;
        }

        final int originalOffset = dstOffset;

        // Value is encoded in base-32768.

        int accumBits = 0;
        int accum = 0;

        final int end = valueOffset + valueLength;
        for (int i = valueOffset; i < end; i++) {
            if (accumBits <= 7) {
                accumBits += 8;
                accum = (accum << 8) | (value[i] & 0xff);
                if (accumBits == 15) {
                    emitDigit(accum, dst, dstOffset, xorMask);
                    dstOffset += 2;
                    accum = 0;
                    accumBits = 0;
                }
            } else {
                int supply = 15 - accumBits;
                accum = (accum << supply) | ((value[i] & 0xff) >> (8 - supply));
                emitDigit(accum, dst, dstOffset, xorMask);
                dstOffset += 2;
                accumBits = 8 - supply;
                accum = value[i] & ((1 << accumBits) - 1);
            }
        }

        if (accumBits > 0) {
            // Pad with zeros.
            accum <<= (15 - accumBits);
            if (accumBits <= 7) {
                // Since amount of significant bits is small, emit only the
                // upper half of the digit. The following code is modified from
                // emitDigit.

                int a = (accum * 21845) >> 22;
                if (accum - ((a << 7) + (a << 6)) == 192) {
                    a++;
                }
                dst[dstOffset++] = (byte) ((a + 32) ^ xorMask);
            } else {
                emitDigit(accum, dst, dstOffset, xorMask);
                dstOffset += 2;
            }
        }

        // Append terminator.
        dst[dstOffset++] = (byte) (TERMINATOR ^ xorMask);

        return dstOffset - originalOffset;
    }

    /**
     * Emits a base-32768 digit using exactly two bytes. The first byte is in
     * the range 32..202 and the second byte is in the range 32..223.
     *
     * @param value digit value in the range 0..32767
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static void emitDigit(int value, byte[] dst, int dstOffset, int xorMask) {
        // The first byte is computed as ((value / 192) + 32) and the second
        // byte is computed as ((value % 192) + 32). To speed things up a bit,
        // the integer division and remainder operations are replaced with a
        // scaled multiplication.

        // approximate value / 192
        int a = (value * 21845) >> 22;

        // approximate value % 192
        // Note: the value 192 was chosen as a divisor because a multiply by
        // 192 can be replaced with two summed shifts.
        int b = value - ((a << 7) + (a << 6));
        if (b == 192) {
            // Fix error.
            a++;
            b = 0;
        }

        dst[dstOffset++] = (byte) ((a + 32) ^ xorMask);
        dst[dstOffset] = (byte) ((b + 32) ^ xorMask);
    }

    /**
     * Returns the amount of bytes required to encode a byte array of the given
     * length.
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
        // The add of 119 is used to force ceiling rounding.
        return value == null ? 1 : (((valueLength << 7) + 119) / 120 + 1);
    }

    /**
     * Encodes the given optional String into a variable amount of bytes. The
     * amount written can be determined by calling calculateEncodedStringLength.
     * <p>
     * Strings are encoded in a fashion similar to UTF-8, in that ASCII
     * characters are usually written in one byte. This encoding is more
     * efficient than UTF-8, but it isn't compatible with UTF-8.
     *
     * @param value String value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encode(String value, byte[] dst, int dstOffset) {
        return encode(value, dst, dstOffset, 0);
    }

    /**
     * Encodes the given optional String into a variable amount of bytes for
     * descending order. The amount written can be determined by calling
     * calculateEncodedStringLength.
     * <p>
     * Strings are encoded in a fashion similar to UTF-8, in that ASCII
     * characters are usually written in one byte. This encoding is more
     * efficient than UTF-8, but it isn't compatible with UTF-8.
     *
     * @param value String value to encode, may be null
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @return amount of bytes written
     */
    public static int encodeDesc(String value, byte[] dst, int dstOffset) {
        return encode(value, dst, dstOffset, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static int encode(String value, byte[] dst, int dstOffset, int xorMask) {
        if (value == null) {
            dst[dstOffset] = (byte) (NULL_BYTE_HIGH ^ xorMask);
            return 1;
        }

        final int originalOffset = dstOffset;

        // All characters have an offset of 2 added, in order to reserve bytes
        // 0 and 1 for encoding nulls and terminators. This means the ASCII
        // string "HelloWorld" is actually encoded as "JgnnqYqtnf". This also
        // means that the ASCII '~' and del characters are encoded in two bytes.

        int length = value.length();
        for (int i = 0; i < length; i++) {
            int c = value.charAt(i) + 2;
            if (c <= 0x7f) {
                // 0xxxxxxx
                dst[dstOffset++] = (byte) (c ^ xorMask);
            } else if (c <= 12415) {
                // 10xxxxxx xxxxxxxx

                // Second byte cannot have the values 0, 1, 254, or 255 because
                // they clash with null and terminator bytes. Divide by 192 and
                // store in first 6 bits. The remainder, with 32 added, goes
                // into the second byte. Note that (192 * 63 + 191) + 128 ==
                // 12415.
                // 63 is the maximum value that can be represented in 6 bits.

                c -= 128; // c will always be at least 128, so normalize.

                // approximate value / 192
                int a = (c * 21845) >> 22;

                // approximate value % 192
                // Note: the value 192 was chosen as a divisor because a
                // multiply by
                // 192 can be replaced with two summed shifts.
                c = c - ((a << 7) + (a << 6));
                if (c == 192) {
                    // Fix error.
                    a++;
                    c = 0;
                }

                dst[dstOffset++] = (byte) ((0x80 | a) ^ xorMask);
                dst[dstOffset++] = (byte) ((c + 32) ^ xorMask);
            } else {
                // 110xxxxx xxxxxxxx xxxxxxxx

                if ((c - 2) >= 0xd800 && (c - 2) <= 0xdbff) {
                    // Found a high surrogate. Verify that surrogate pair is
                    // well-formed. Low surrogate must follow high surrogate.
                    if (i + 1 < length) {
                        int c2 = value.charAt(i + 1);
                        if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                            c = ((((c - 2) & 0x3ff) << 10) | (c2 & 0x3ff)) + 0x10002;
                            i++;
                        }
                    }
                }

                // Second and third bytes cannot have the values 0, 1, 254, or
                // 255 because they clash with null and terminator
                // bytes. Divide by 192 twice, storing the first and second
                // remainders in the third and second bytes, respectively.
                // Note that largest unicode value supported is 2^20 + 65535 ==
                // 1114111. When divided by 192 twice, the value is 30, which
                // just barely fits in the 5 available bits of the first byte.

                c -= 12416; // c will always be at least 12416, so normalize.

                int a = (int) ((c * 21845L) >> 22);
                c = c - ((a << 7) + (a << 6));
                if (c == 192) {
                    a++;
                    c = 0;
                }

                dst[dstOffset + 2] = (byte) ((c + 32) ^ xorMask);

                c = (a * 21845) >> 22;
                a = a - ((c << 7) + (c << 6));
                if (a == 192) {
                    c++;
                    a = 0;
                }

                dst[dstOffset++] = (byte) ((0xc0 | c) ^ xorMask);
                dst[dstOffset++] = (byte) ((a + 32) ^ xorMask);
                dstOffset++;
            }
        }

        // Append terminator.
        dst[dstOffset++] = (byte) (TERMINATOR ^ xorMask);

        return dstOffset - originalOffset;
    }

    /**
     * Returns the amount of bytes required to encode the given String.
     *
     * @param value String to encode, may be null
     */
    public static int calculateEncodedStringLength(String value) {
        int encodedLen = 1;
        if (value != null) {
            int valueLength = value.length();
            for (int i = 0; i < valueLength; i++) {
                int c = value.charAt(i);
                if (c <= (0x7f - 2)) {
                    encodedLen++;
                } else if (c <= (12415 - 2)) {
                    encodedLen += 2;
                } else {
                    if (c >= 0xd800 && c <= 0xdbff) {
                        // Found a high surrogate. Verify that surrogate pair is
                        // well-formed. Low surrogate must follow high
                        // surrogate.
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
        }
        return encodedLen;
    }

    /**
     * Encodes the given byte array for use when there is only a single required
     * property, descending order, whose type is a byte array. The original byte
     * array is returned if the length is zero.
     */
    public static byte[] encodeSingleDesc(byte[] value) {
        return encodeSingleDesc(value, 0, 0);
    }

    /**
     * Encodes the given byte array for use when there is only a single required
     * property, descending order, whose type is a byte array. The original byte
     * array is returned if the length and padding lengths are zero.
     *
     * @param prefixPadding amount of extra bytes to allocate at start of
     * encoded byte array
     * @param suffixPadding amount of extra bytes to allocate at end of encoded
     * byte array
     */
    public static byte[] encodeSingleDesc(byte[] value, int prefixPadding, int suffixPadding) {
        int length = value.length;
        if (prefixPadding <= 0 && suffixPadding <= 0 && length == 0) {
            return value;
        }
        byte[] dst = new byte[prefixPadding + length + suffixPadding];
        while (--length >= 0) {
            dst[prefixPadding + length] = (byte) (~value[length]);
        }
        return dst;
    }

    /**
     * Encodes the given byte array for use when there is only a single nullable
     * property, descending order, whose type is a byte array.
     */
    public static byte[] encodeSingleNullableDesc(byte[] value) {
        return encodeSingleNullableDesc(value, 0, 0);
    }

    /**
     * Encodes the given byte array for use when there is only a single nullable
     * property, descending order, whose type is a byte array.
     *
     * @param prefixPadding amount of extra bytes to allocate at start of
     * encoded byte array
     * @param suffixPadding amount of extra bytes to allocate at end of encoded
     * byte array
     */
    public static byte[] encodeSingleNullableDesc(byte[] value, int prefixPadding, int suffixPadding) {
        if (prefixPadding <= 0 && suffixPadding <= 0) {
            if (value == null) {
                return new byte[] {NULL_BYTE_LOW};
            }

            int length = value.length;
            if (length == 0) {
                return new byte[] {NOT_NULL_BYTE_LOW};
            }

            byte[] dst = new byte[1 + length];
            dst[0] = NOT_NULL_BYTE_LOW;
            while (--length >= 0) {
                dst[1 + length] = (byte) (~value[length]);
            }
            return dst;
        }

        if (value == null) {
            byte[] dst = new byte[prefixPadding + 1 + suffixPadding];
            dst[prefixPadding] = NULL_BYTE_LOW;
            return dst;
        }

        int length = value.length;
        byte[] dst = new byte[prefixPadding + 1 + length + suffixPadding];
        dst[prefixPadding] = NOT_NULL_BYTE_LOW;
        while (--length >= 0) {
            dst[prefixPadding + 1 + length] = (byte) (~value[length]);
        }
        return dst;
    }
}
