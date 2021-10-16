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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.math.BigInteger;

public abstract class AbstractNumericField extends AbstractStorageField {
    public static final long INT_64_MAX = 0x7FFFFFFFFFFFFFFFL;
    public static final long INT_64_MIN = -0x7FFFFFFFFFFFFFFFL - 1;
    public static final long UNSIGNED_INT_64_MAX = 0xFFFFFFFFFFFFFFFFL;
    public static final long UNSIGNED_INT_64_MIN = 0L;

    public static final int INT_32_MAX = 0x7FFFFFFF;
    public static final int INT_32_MIN = ~0x7FFFFFFF;
    public static final int UNSIGNED_INT_32_MAX = 0xFFFFFFFF;
    public static final int UNSIGNED_INT_32_MIN = 0;

    public static final int INT_24_MAX = 0x007FFFFF;
    public static final int INT_24_MIN = ~0x007FFFFF;
    public static final int UNSIGNED_INT_24_MAX = 0x00FFFFFF;
    public static final int UNSIGNED_INT_24_MIN = 0;

    public static final int INT_16_MAX = 0x7FFF;
    public static final int INT_16_MIN = ~0x7FFF;
    public static final int UNSIGNED_INT_16_MAX = 0xFFFF;
    public static final int UNSIGNED_INT_16_MIN = 0;

    public static final int INT_8_MAX = 0x7F;
    public static final int INT_8_MIN = ~0x7F;
    public static final int UNSIGNED_INT_8_MAX = 0xFF;
    public static final int UNSIGNED_INT_8_MIN = 0;

    public static final long LONG_MASK = 0xFFFFFFFFL;

    public static final BigInteger MAX_UNSIGNED_INT64 = new BigInteger(Long.toUnsignedString(0xffffffffffffffffL));

    public static final BigInteger MIN_SIGNED_INT64 = new BigInteger(Long.toString(-0x7fffffffffffffffL - 1));

    protected AbstractNumericField(DataType<?> fieldType) {
        super(fieldType);
    }

    protected TypeConversionStatus checkInt(byte[] bytes, long parseError, int numberEnd) {
        TypeConversionStatus conversionStatus;
        if (0 == numberEnd || parseError == StringNumericParser.MY_ERRNO_EDOM) {
            conversionStatus = TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        } else if (numberEnd < bytes.length) {
            // Test if we have garbage(space character) at the end of the given string.
            boolean findNonSpace = false;
            for (int i = numberEnd; i < bytes.length; i++) {
                if (bytes[numberEnd] != ' ') {
                    findNonSpace = true;
                    break;
                }
            }
            if (findNonSpace) {
                // has important data
                conversionStatus = TypeConversionStatus.TYPE_WARN_TRUNCATED;
            } else {
                conversionStatus = TypeConversionStatus.TYPE_OK;
            }
        } else {
            conversionStatus = TypeConversionStatus.TYPE_OK;
        }
        return conversionStatus;
    }

    /**
     * Copies an integer value to a format comparable with memcmp(). The
     * format is characterized by the following:
     * - The sign bit goes first and is unset for negative values.
     * - The representation is big endian.
     * The function template can be instantiated to copy from little or
     * big endian values.
     */
    protected void copyInteger(byte[] to, int toLen, byte[] from, int fromLen, boolean isUnsigned,
                               boolean isBigEndian) {
        if (isBigEndian) {
            if (isUnsigned) {
                to[0] = from[0];
            } else {
                // Reverse the sign bit.
                to[0] = (byte) (from[0] ^ 128);
            }
            System.arraycopy(from, 1, to, 1, toLen - 1);
        } else {
            byte sign = from[fromLen - 1];
            if (isUnsigned) {
                to[0] = sign;
            } else {
                // Reverse the sign bit.
                to[0] = (byte) (sign ^ 128);
            }
            for (int i = 1, j = fromLen - 2; i < toLen; ++i, --j) {
                to[i] = from[j];
            }
        }
    }
}
