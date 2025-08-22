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

import io.airlift.slice.Slice;

import java.math.BigDecimal;

/**
 * Wrap a decimal structure.
 */
public class Decimal extends Number implements Comparable<Decimal> {

    private static final long serialVersionUID = 5036570408857619384L;

    public static final Decimal ZERO = Decimal.fromLong(0L);

    public static final Decimal ONE = Decimal.fromLong(1L);

    public static final Decimal MAX_INT32 = Decimal.fromLong(0x7FFFFFFF);

    public static final Decimal MIN_INT32 = Decimal.fromLong(~0x7FFFFFFF);

    public static final Decimal MAX_UNSIGNED_INT32 = Decimal.fromLong(0xFFFFFFFFL);

    public static final Decimal MAX_SIGNED = Decimal.fromLong(0x7FFFFFFFFFFFFFFFL);

    public static final Decimal MIN_SIGNED = Decimal.fromLong(-0x7FFFFFFFFFFFFFFFL - 1);

    public static final Decimal MAX_UNSIGNED = Decimal.fromUnsigned(UInt64.fromLong(-1L));

    public static final int MAX_64_BIT_PRECISION = 18;

    public static final int MAX_128_BIT_PRECISION = 38;

    private final DecimalStructure decimalStructure;

    public Decimal() {
        this(new DecimalStructure());
    }

    public Decimal(Slice memorySegment) {
        this(new DecimalStructure(memorySegment));
    }

    public Decimal(DecimalStructure decimalStructure) {
        this.decimalStructure = decimalStructure;
    }

    public Decimal(long longVal, int scale) {
        this(new DecimalStructure());
        this.decimalStructure.setLongWithScale(longVal, scale);
    }

    public static Decimal fromBigDecimal(BigDecimal bd) {
        if (bd == null) {
            return null;
        }
        String decimalStr = bd.toPlainString();
        Decimal d = new Decimal(new DecimalStructure());
        DecimalConverter.parseString(decimalStr.getBytes(), d.decimalStructure, false);
        return d;
    }

    public static Decimal fromString(String str) {
        Decimal d = new Decimal(new DecimalStructure());
        DecimalConverter.parseString(str.getBytes(), d.decimalStructure, false);
        return d;
    }

    public static Decimal fromLong(long value) {
        Decimal d = new Decimal(new DecimalStructure());
        DecimalConverter.longToDecimal(value, d.decimalStructure);
        return d;
    }

    public static Decimal fromUnsigned(UInt64 value) {
        Decimal d = new Decimal(new DecimalStructure());
        long unsigned = value.longValue();
        DecimalConverter.unsignedlongToDecimal(unsigned, d.decimalStructure);
        return d;
    }

    public BigDecimal toBigDecimal() {
        String decimalStr = decimalStructure.toString();
        return new BigDecimal(decimalStr);
    }

    @Override
    public int compareTo(Decimal that) {
        return FastDecimalUtils.compare(this.decimalStructure, that.decimalStructure);
    }

    @Override
    public int hashCode() {
        return decimalStructure.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof Decimal) {
            return FastDecimalUtils.compare(this.decimalStructure, ((Decimal) o).decimalStructure) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return decimalStructure.toString();
    }

    public byte[] toBytes() {
        return decimalStructure.toBytes();
    }

    public void add(Decimal d, Decimal to) {
        FastDecimalUtils.add(this.decimalStructure, d.decimalStructure, to.decimalStructure, true);
    }

    public Decimal add(Decimal d) {
        DecimalStructure to = new DecimalStructure();
        FastDecimalUtils.add(this.decimalStructure, d.decimalStructure, to, true);
        return new Decimal(to);
    }

    public Decimal subtract(Decimal d) {
        DecimalStructure to = new DecimalStructure();
        FastDecimalUtils.sub(this.decimalStructure, d.decimalStructure, to, true);
        return new Decimal(to);
    }

    public Decimal multiply(Decimal d) {
        DecimalStructure to = new DecimalStructure();
        FastDecimalUtils.mul(this.decimalStructure, d.decimalStructure, to, true);
        return new Decimal(to);
    }

    public Decimal divide(Decimal d) {
        DecimalStructure to = new DecimalStructure();
        FastDecimalUtils
            .div(this.decimalStructure, d.decimalStructure, to, DecimalStructure.getDefaultDivPrecisionIncrement(), true);
        return new Decimal(to);
    }

    public Decimal remainder(Decimal d) {
        DecimalStructure to = new DecimalStructure();
        FastDecimalUtils.mod(this.decimalStructure, d.decimalStructure, to, true);
        return new Decimal(to);
    }

    @Override
    public long longValue() {
        // It should be compatible for BigDecimal / BigInteger longValue()
        if (this.compareTo(MAX_UNSIGNED) > 0 || this.compareTo(MIN_SIGNED) < 0) {
            // inflated value
            return Long.MIN_VALUE;
        } else {
            boolean isUnsigned = this.compareTo(MAX_SIGNED) > 0;
            return DecimalConverter.decimal2Long(this.getDecimalStructure(), isUnsigned)[0];
        }
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public double doubleValue() {
        return DecimalConverter.decimalToDouble(decimalStructure);
    }

    public int precision() {
        return decimalStructure.getIntegers() + decimalStructure.getFractions();
    }

    public int scale() {
        return decimalStructure.getFractions();
    }

    public Slice getMemorySegment() {
        return decimalStructure.getDecimalMemorySegment();
    }

    public DecimalStructure getDecimalStructure() {
        return decimalStructure;
    }

    public Decimal copy() {
        return new Decimal(decimalStructure.copy());
    }

    /**
     * shift by newScale and get the decimal64 value
     * Warning1: change the internal data of current decimal value!
     * Warning2: the result long value is inaccurate
     */
    public long unscaleInternal(int newScale) {
        if (newScale != 0) {
            FastDecimalUtils.shift(this.getDecimalStructure(), this.getDecimalStructure(),
                newScale);
        }
        return this.longValue();
    }

    /**
     * get the unscaled long value of current decimal
     */
    public long unscale() {
        return unscale(new DecimalStructure());
    }

    public long unscale(DecimalStructure bufferStructure) {
        Decimal decimal = this;
        if (scale() != 0) {
            decimal = new Decimal(bufferStructure);
            FastDecimalUtils.shift(this.getDecimalStructure(), bufferStructure,
                this.scale());
        }
        return decimal.longValue();
    }
}