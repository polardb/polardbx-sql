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

package io.airlift.slice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Preconditions.checkArgument;
import static java.lang.Math.floor;
import static java.util.Objects.requireNonNull;

public class DataSize
    implements Comparable<DataSize>
{
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

    // We iterate over the DATASIZE_UNITS constant in convertToMostSuccinctDataSize()
    // instead of Unit.values() as the latter results in non-trivial amount of memory
    // allocation when that method is called in a tight loop. The reason is that the values()
    // call allocates a new array at each call.
    private static final Unit[] DATASIZE_UNITS = Unit.values();

    public static DataSize succinctBytes(long bytes)
    {
        return succinctDataSize(bytes, Unit.BYTE);
    }

    public static DataSize succinctDataSize(double size, Unit unit)
    {
        return new DataSize(size, unit).convertToMostSuccinctDataSize();
    }

    private final double value;
    private final Unit unit;

    public DataSize(double size, Unit unit)
    {
        checkArgument(!Double.isInfinite(size), "size is infinite");
        checkArgument(!Double.isNaN(size), "size is not a number");
        checkArgument(size >= 0, "size is negative");
        requireNonNull(unit, "unit is null");

        this.value = size;
        this.unit = unit;
    }

    public long toBytes()
    {
        double bytes = getValue(Unit.BYTE);
        checkState(bytes <= Long.MAX_VALUE, "size is too large to be represented in bytes as a long");
        return (long) bytes;
    }

    public double getValue()
    {
        return value;
    }

    public Unit getUnit()
    {
        return unit;
    }

    public double getValue(Unit unit)
    {
        return value * (this.unit.getFactor() * 1.0 / unit.getFactor());
    }

    public long roundTo(Unit unit)
    {
        double rounded = Math.floor(getValue(unit) + 0.5d);
        checkArgument(rounded <= Long.MAX_VALUE,
            "size is too large to be represented in requested unit as a long");
        return (long) rounded;
    }

    public DataSize convertTo(Unit unit)
    {
        requireNonNull(unit, "unit is null");
        return new DataSize(getValue(unit), unit);
    }

    public DataSize convertToMostSuccinctDataSize()
    {
        Unit unitToUse = Unit.BYTE;
        for (Unit unitToTest : DATASIZE_UNITS) {
            if (getValue(unitToTest) >= 1.0) {
                unitToUse = unitToTest;
            }
            else {
                break;
            }
        }
        return convertTo(unitToUse);
    }

    @JsonValue
    @Override
    public String toString()
    {
        //noinspection FloatingPointEquality
        if (floor(value) == value) {
            return (long) (floor(value)) + unit.getUnitString();
        }

        return String.format(Locale.ENGLISH, "%.2f%s", value, unit.getUnitString());
    }

    @JsonCreator
    public static DataSize valueOf(String size)
        throws IllegalArgumentException
    {
        requireNonNull(size, "size is null");
        checkArgument(!size.isEmpty(), "size is empty");

        Matcher matcher = PATTERN.matcher(size);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("size is not a valid data size string: " + size);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unitString = matcher.group(2);

        for (Unit unit : Unit.values()) {
            if (unit.getUnitString().equals(unitString)) {
                return new DataSize(value, unit);
            }
        }

        throw new IllegalArgumentException("Unknown unit: " + unitString);
    }

    @Override
    public int compareTo(DataSize o)
    {
        return Double.compare(getValue(Unit.BYTE), o.getValue(Unit.BYTE));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataSize dataSize = (DataSize) o;

        return compareTo(dataSize) == 0;
    }

    @Override
    public int hashCode()
    {
        double value = getValue(Unit.BYTE);
        return Double.hashCode(value);
    }

    public enum Unit
    {
        //This order is important, it should be in increasing magnitude.
        BYTE(1L, "B"),
        KILOBYTE(1L << 10, "kB"),
        MEGABYTE(1L << 20, "MB"),
        GIGABYTE(1L << 30, "GB"),
        TERABYTE(1L << 40, "TB"),
        PETABYTE(1L << 50, "PB");

        private final long factor;
        private final String unitString;

        Unit(long factor, String unitString)
        {
            this.factor = factor;
            this.unitString = unitString;
        }

        long getFactor()
        {
            return factor;
        }

        public String getUnitString()
        {
            return unitString;
        }
    }
}