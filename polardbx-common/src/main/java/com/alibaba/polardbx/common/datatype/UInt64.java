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

package com.alibaba.polardbx.common.datatype;

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.google.common.primitives.Longs;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.alibaba.polardbx.common.datatype.UInt64Utils.UNSIGNED_MASK;
import static com.alibaba.polardbx.common.utils.time.parser.StringNumericParser.ERROR_INDEX;


public class UInt64 extends Number implements Comparable<UInt64>, Serializable {

    public static final UInt64 MAX_UINT64 = new UInt64(0xFFFFFFFFFFFFFFFFL);
    public static final UInt64 UINT64_ZERO = new UInt64(0L);

    private final long value;

    public UInt64(long value) {
        this.value = value;
    }

    public static UInt64 fromLong(long l) {
        return new UInt64(l);
    }

    public static UInt64 fromBigInteger(BigInteger bd) {
        if (bd == null) {
            return null;
        }
        return UInt64.fromString(bd.toString());
    }

    public static UInt64 fromString(String value) {
        if (value == null) {
            return null;
        }
        return fromBytes(value.getBytes());
    }

    public static UInt64 fromBytes(byte[] value) {
        if (value == null) {
            return null;
        }
        return fromBytes(value, 0, value.length);
    }

    public static UInt64 fromBytes(byte[] value, int offset, int length) {
        if (value == null) {
            return null;
        }
        long[] results = StringNumericParser.parseString(value, offset, offset + length);
        return new UInt64(results[StringNumericParser.NUMERIC_INDEX]);
    }

    public UInt64 add(UInt64 u) {
        return new UInt64(this.value + u.value);
    }

    public UInt64 sub(UInt64 u) {
        return new UInt64(this.value - u.value);
    }

    public UInt64 multi(UInt64 u) {
        return new UInt64(this.value * u.value);
    }

    public UInt64 divide(UInt64 u) {
        return new UInt64(UInt64Utils.divide(this.value, u.value));
    }

    public UInt64 mod(UInt64 u) {
        return new UInt64(UInt64Utils.remainder(this.value, u.value));
    }

    /**
     * Mathematics method. BitAnd unsigned value.
     */
    public UInt64 bitAnd(UInt64 u) {
        return new UInt64(this.value & u.value);
    }

    /**
     * Mathematics method. BitOr unsigned value.
     */
    public UInt64 bitOr(UInt64 u) {
        return new UInt64(this.value | u.value);
    }

    /**
     * Mathematics method. BitOr unsigned value.
     */
    public UInt64 bitXor(UInt64 u) {
        return new UInt64(this.value ^ u.value);
    }

    @Override
    public int compareTo(UInt64 u) {
        return UInt64Utils.compareUnsigned(this.value, u.value);
    }

    @Override
    public int intValue() {
        return (int) this.value;
    }

    @Override
    public long longValue() {
        return this.value;
    }

    @Override
    public float floatValue() {
        float fValue = (float) (value & UNSIGNED_MASK);
        if (value < 0) {

            fValue += 0x1.0p63f;
        }
        return fValue;
    }

    @Override
    public double doubleValue() {
        double dValue = (double) (value & UNSIGNED_MASK);
        if (value < 0) {

            dValue += 0x1.0p63;
        }
        return dValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UInt64) {
            return value == ((UInt64) obj).value;
        }
        return false;
    }

    @Override
    public String toString() {
        return Long.toUnsignedString(value);
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(value);
    }

    public BigInteger toBigInteger() {
        return new BigInteger(toString());
    }

    public BigDecimal toBigDecimal() {
        return new BigDecimal(toString());
    }

    public Decimal toDecimal() {
        return Decimal.fromString(toString());
    }
}