
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

/*
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

package io.airlift.slice;

import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static io.airlift.slice.JvmUtils.newByteBuffer;
import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.StringDecoder.decodeString;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public final class Slice
    implements Comparable<Slice> {
    private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(Slice.class).instanceSize();


    @Deprecated
    public static Slice toUnsafeSlice(ByteBuffer byteBuffer) {
        return Slices.wrappedBuffer(byteBuffer);
    }


    private Object base;


    private long address;

    private int size;

    private int retainedSize;

    private Object reference;

    private int hash;

    public Slice() {
        this.base = null;
        this.address = 0;
        this.size = 0;
        this.retainedSize = INSTANCE_SIZE;
        this.reference = null;
    }

    Slice(byte[] base) {
        requireNonNull(base, "base is null");
        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET;
        this.size = base.length;
        this.retainedSize = INSTANCE_SIZE + base.length;
        this.reference = null;
    }

    Slice(byte[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET + offset;
        this.size = length;
        this.retainedSize = INSTANCE_SIZE + base.length;
        this.reference = null;
    }

    Slice(boolean[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_BOOLEAN_BASE_OFFSET + offset * ARRAY_BOOLEAN_INDEX_SCALE;
        this.size = length * ARRAY_BOOLEAN_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_BOOLEAN_INDEX_SCALE;
        this.reference = null;
    }

    Slice(short[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_SHORT_BASE_OFFSET + offset * ARRAY_SHORT_INDEX_SCALE;
        this.size = length * ARRAY_SHORT_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_SHORT_INDEX_SCALE;
        this.reference = null;
    }

    Slice(int[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_INT_BASE_OFFSET + offset * ARRAY_INT_INDEX_SCALE;
        this.size = length * ARRAY_INT_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_INT_INDEX_SCALE;
        this.reference = null;
    }

    Slice(long[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_LONG_BASE_OFFSET + offset * ARRAY_LONG_INDEX_SCALE;
        this.size = length * ARRAY_LONG_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_LONG_INDEX_SCALE;
        this.reference = null;
    }

    Slice(float[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_FLOAT_BASE_OFFSET + offset * ARRAY_FLOAT_INDEX_SCALE;
        this.size = length * ARRAY_FLOAT_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_FLOAT_INDEX_SCALE;
        this.reference = null;
    }

    Slice(double[] base, int offset, int length) {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_DOUBLE_BASE_OFFSET + offset * ARRAY_DOUBLE_INDEX_SCALE;
        this.size = length * ARRAY_DOUBLE_INDEX_SCALE;
        this.retainedSize = INSTANCE_SIZE + base.length * ARRAY_DOUBLE_INDEX_SCALE;
        this.reference = null;
    }

    Slice(@Nullable Object base, long address, int size, int retainedSize, @Nullable Object reference) {
        if (address <= 0) {
            throw new IllegalArgumentException(format("Invalid address: %s", address));
        }
        if (size <= 0) {
            throw new IllegalArgumentException(format("Invalid size: %s", size));
        }
        checkArgument((address + size) >= size, "Address + size is greater than 64 bits");

        this.reference = reference;
        this.base = base;
        this.address = address;
        this.size = size;

        this.retainedSize = retainedSize;
    }

    void resetSlice(byte[] base, int offset, int length) {
        checkArgument(this != Slices.EMPTY_SLICE, "EmptySlice shouldn't resetSlice");
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET + offset;
        this.size = length;
        this.retainedSize = INSTANCE_SIZE + base.length;
        this.reference = null;
    }

    void resetSlice(byte[] base) {
        checkArgument(this != Slices.EMPTY_SLICE, "EmptySlice shouldn't resetSlice");
        requireNonNull(base, "base is null");
        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET;
        this.size = base.length;
        this.retainedSize = INSTANCE_SIZE + base.length;
        this.reference = null;
    }

    void resetSlice(@Nullable Object base, long address, int size, int retainedSize, @Nullable Object reference) {
        checkArgument(this != Slices.EMPTY_SLICE, "EmptySlice shouldn't resetSlice");
        if (address <= 0) {
            throw new IllegalArgumentException(format("Invalid address: %s", address));
        }
        if (size <= 0) {
            throw new IllegalArgumentException(format("Invalid size: %s", size));
        }
        checkArgument((address + size) >= size, "Address + size is greater than 64 bits");

        this.reference = reference;
        this.base = base;
        this.address = address;
        this.size = size;

        this.retainedSize = retainedSize;
    }

    public Object getBase() {
        return base;
    }

    public long getAddress() {
        return address;
    }

    public int length() {
        return size;
    }

    public int getRetainedSize() {
        return retainedSize;
    }

    public void fill(byte value) {
        int offset = 0;
        int length = size;
        long longValue = fillLong(value);
        while (length >= SIZE_OF_LONG) {
            unsafe.putLong(base, address + offset, longValue);
            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            unsafe.putByte(base, address + offset, value);
            offset++;
            length--;
        }
    }

    public void clear() {
        clear(0, size);
    }

    public void clear(int offset, int length) {
        while (length >= SIZE_OF_LONG) {
            unsafe.putLong(base, address + offset, 0);
            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            unsafe.putByte(base, address + offset, (byte) 0);
            offset++;
            length--;
        }
    }

    public byte getByte(int index) {
        checkIndexLength(index, SIZE_OF_BYTE);
        return getByteUnchecked(index);
    }

    public byte getByteUnchecked(int index) {
        return unsafe.getByte(base, address + index);
    }

    public short getUnsignedByte(int index) {
        return (short) (getByte(index) & 0xFF);
    }

    public short getShort(int index) {
        checkIndexLength(index, SIZE_OF_SHORT);
        return getShortUnchecked(index);
    }

    short getShortUnchecked(int index) {
        return unsafe.getShort(base, address + index);
    }

    public int getUnsignedShort(int index) {
        return getShort(index) & 0xFFFF;
    }

    public int getInt(int index) {
        checkIndexLength(index, SIZE_OF_INT);
        return getIntUnchecked(index);
    }

    public int getIntUnchecked(int index) {
        return unsafe.getInt(base, address + index);
    }

    public long getUnsignedInt(int index) {
        return getInt(index) & 0xFFFFFFFFL;
    }

    public long getLong(int index) {
        checkIndexLength(index, SIZE_OF_LONG);
        return getLongUnchecked(index);
    }

    public long getLongUnchecked(int index) {
        return unsafe.getLong(base, address + index);
    }

    public float getFloat(int index) {
        checkIndexLength(index, SIZE_OF_FLOAT);
        return unsafe.getFloat(base, address + index);
    }

    public double getDouble(int index) {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(base, address + index);
    }

    public double getDoubleUnchecked(int index) {
        return unsafe.getDouble(base, address + index);
    }

    public void getBytes(int index, Slice destination) {
        getBytes(index, destination, 0, destination.length());
    }

    public void getBytes(int index, Slice destination, int destinationIndex, int length) {
        destination.setBytes(destinationIndex, this, index, length);
    }

    public void getBytes(int index, byte[] destination) {
        getBytes(index, destination, 0, destination.length);
    }

    public void getBytes(int index, byte[] destination, int destinationIndex, int length) {
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);

        copyMemory(base, address + index, destination, (long) ARRAY_BYTE_BASE_OFFSET + destinationIndex, length);
    }

    public void getBytesUnchecked(int index, byte[] destination, int destinationIndex, int length) {
        copyMemory(base, address + index, destination, (long) ARRAY_BYTE_BASE_OFFSET + destinationIndex, length);
    }

    public byte[] getBytes() {
        return getBytes(0, length());
    }

    public byte[] getBytes(int index, int length) {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return bytes;
    }

    public void getBytes(int index, OutputStream out, int length)
        throws IOException {
        checkIndexLength(index, length);

        if (base instanceof byte[]) {
            out.write((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
            return;
        }

        byte[] buffer = new byte[4096];
        while (length > 0) {
            int size = min(buffer.length, length);
            getBytes(index, buffer, 0, size);
            out.write(buffer, 0, size);
            length -= size;
            index += size;
        }
    }

    public void setByte(int index, int value) {
        checkIndexLength(index, SIZE_OF_BYTE);
        setByteUnchecked(index, value);
    }

    public void setByteUnchecked(int index, int value) {
        unsafe.putByte(base, address + index, (byte) (value & 0xFF));
    }

    public void setShort(int index, int value) {
        checkIndexLength(index, SIZE_OF_SHORT);
        setShortUnchecked(index, value);
    }

    void setShortUnchecked(int index, int value) {
        unsafe.putShort(base, address + index, (short) (value & 0xFFFF));
    }

    public void setInt(int index, int value) {
        checkIndexLength(index, SIZE_OF_INT);
        setIntUnchecked(index, value);
    }

    public void setIntUnchecked(int index, int value) {
        unsafe.putInt(base, address + index, value);
    }

    public void setLong(int index, long value) {
        checkIndexLength(index, SIZE_OF_LONG);
        setLongUnchecked(index, value);
    }

    public void setLongUnchecked(int index, long value) {
        unsafe.putLong(base, address + index, value);
    }

    public void setFloat(int index, float value) {
        checkIndexLength(index, SIZE_OF_FLOAT);
        unsafe.putFloat(base, address + index, value);
    }

    public void setDouble(int index, double value) {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        unsafe.putDouble(base, address + index, value);
    }

    public void setDoubleUnchecked(int index, double value) {
        unsafe.putDouble(base, address + index, value);
    }

    public void setBytes(int index, Slice source) {
        setBytes(index, source, 0, source.length());
    }

    public void setBytes(int index, Slice source, int sourceIndex, int length) {
        checkIndexLength(index, length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length());

        copyMemory(source.base, source.address + sourceIndex, base, address + index, length);
    }

    public void setBytes(int index, byte[] source) {
        setBytes(index, source, 0, source.length);
    }

    public void setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        checkIndexLength(index, length);
        copyMemory(source, (long) ARRAY_BYTE_BASE_OFFSET + sourceIndex, base, address + index, length);
    }

    public void setBytesUnchecked(int index, byte[] source, int sourceIndex, int length) {
        copyMemory(source, (long) ARRAY_BYTE_BASE_OFFSET + sourceIndex, base, address + index, length);
    }

    public void setIntArray(int index, int[] source, int sourceIndex, int length) {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length * Integer.BYTES);
        copyMemory(source, (long) ARRAY_INT_BASE_OFFSET + sourceIndex, base, address + index, length * Integer.BYTES);
    }

    public void setIntArrayUnchecked(int index, int[] source, int sourceIndex, int length) {
        copyMemory(source, (long) ARRAY_INT_BASE_OFFSET + sourceIndex, base, address + index, length * Integer.BYTES);
    }

    public void setBytes(int index, InputStream in, int length)
        throws IOException {
        checkIndexLength(index, length);
        if (base instanceof byte[]) {
            byte[] bytes = (byte[]) base;
            int offset = (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index);
            while (length > 0) {
                int bytesRead = in.read(bytes, offset, length);
                if (bytesRead < 0) {
                    throw new IndexOutOfBoundsException("End of stream");
                }
                length -= bytesRead;
                offset += bytesRead;
            }
            return;
        }

        byte[] bytes = new byte[4096];

        while (length > 0) {
            int bytesRead = in.read(bytes, 0, min(bytes.length, length));
            if (bytesRead < 0) {
                throw new IndexOutOfBoundsException("End of stream");
            }
            copyMemory(bytes, ARRAY_BYTE_BASE_OFFSET, base, address + index, bytesRead);
            length -= bytesRead;
            index += bytesRead;
        }
    }

    public Slice slice(int index, int length, Slice output) {
        if ((index == 0) && (length == length())) {
            if (length == 0) {

                return Slices.EMPTY_SLICE;
            }

            if (output != null) {
                output.resetSlice(base, address, length, retainedSize, reference);
                return output;
            }
            return this;
        }
        checkIndexLength(index, length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        if (output != null) {
            output.resetSlice(base, address + index, length, retainedSize, reference);
            return output;
        }
        return new Slice(base, address + index, length, retainedSize, reference);
    }

    public Slice slice(int index, int length) {
        return slice(index, length, null);
    }

    public int indexOfByte(int b) {
        b = b & 0xFF;
        for (int i = 0; i < size; i++) {
            if (getByteUnchecked(i) == b) {
                return i;
            }
        }
        return -1;
    }

    public int indexOf(Slice slice) {
        return indexOf(slice, 0);
    }

    public int indexOf(Slice pattern, int offset) {
        if (size == 0 || offset >= size) {
            return -1;
        }

        if (pattern.length() == 0) {
            return offset;
        }

        if (pattern.length() < SIZE_OF_INT || size < SIZE_OF_LONG) {
            return indexOfBruteForce(pattern, offset);
        }

        int head = pattern.getIntUnchecked(0);

        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        int lastValidIndex = size - pattern.length();
        int index = offset;
        while (index <= lastValidIndex) {

            int value = getIntUnchecked(index);

            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;

            if (hasZeroBytes == 0) {
                index += SIZE_OF_INT;
                continue;
            }

            if (value == head && equalsUnchecked(index, pattern, 0, pattern.length())) {
                return index;
            }

            index++;
        }

        return -1;
    }

    int indexOfBruteForce(Slice pattern, int offset) {
        if (size == 0 || offset >= size) {
            return -1;
        }

        if (pattern.length() == 0) {
            return offset;
        }

        byte firstByte = pattern.getByteUnchecked(0);
        int lastValidIndex = size - pattern.length();
        int index = offset;
        while (true) {

            while (index < lastValidIndex && getByteUnchecked(index) != firstByte) {
                index++;
            }
            if (index > lastValidIndex) {
                break;
            }

            if (equalsUnchecked(index, pattern, 0, pattern.length())) {
                return index;
            }

            index++;
        }

        return -1;
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public int compareTo(Slice that) {
        if (this == that) {
            return 0;
        }
        return compareTo(0, size, that, 0, that.size);
    }

    @SuppressWarnings("ObjectEquality")
    public int compareTo(int offset, int length, Slice that, int otherOffset, int otherLength) {
        if ((this == that) && (offset == otherOffset) && (length == otherLength)) {
            return 0;
        }

        checkIndexLength(offset, length);
        that.checkIndexLength(otherOffset, otherLength);

        long thisAddress = address + offset;
        long thatAddress = that.address + otherOffset;

        int compareLength = min(length, otherLength);
        while (compareLength >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, thisAddress);
            long thatLong = unsafe.getLong(that.base, thatAddress);

            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            compareLength -= SIZE_OF_LONG;
        }

        while (compareLength > 0) {
            byte thisByte = unsafe.getByte(base, thisAddress);
            byte thatByte = unsafe.getByte(that.base, thatAddress);

            int v = compareUnsignedBytes(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            thisAddress++;
            thatAddress++;
            compareLength--;
        }

        return Integer.compare(length, otherLength);
    }

    @SuppressWarnings("ObjectEquality")
    public int compareTo(int offset, int length, byte[] that, int otherOffset, int otherLength) {
        checkIndexLength(offset, length);
        checkPositionIndexes(otherOffset, otherOffset + otherLength, that.length);

        long thisAddress = address + offset;
        long thatAddress = ARRAY_BYTE_BASE_OFFSET + otherOffset;
        final Object thisBase = base;

        int compareLength = min(length, otherLength);
        while (compareLength >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(thisBase, thisAddress);
            long thatLong = unsafe.getLong(that, thatAddress);

            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            compareLength -= SIZE_OF_LONG;
        }

        while (compareLength > 0) {
            byte thisByte = unsafe.getByte(thisBase, thisAddress);
            byte thatByte = unsafe.getByte(that, thatAddress);

            int v = compareUnsignedBytes(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            thisAddress++;
            thatAddress++;
            compareLength--;
        }

        return Integer.compare(length, otherLength);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Slice)) {
            return false;
        }

        Slice that = (Slice) o;
        if (length() != that.length()) {
            return false;
        }

        return equalsUnchecked(0, that, 0, length());
    }

    @SuppressWarnings("NonFinalFieldReferencedInHashCode")
    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }

        hash = hashCode(0, size);
        return hash;
    }

    public int hashCode(int offset, int length) {
        return (int) XxHash64.hash(this, offset, length);
    }

    @SuppressWarnings("ObjectEquality")
    public boolean equals(int offset, int length, Slice that, int otherOffset, int otherLength) {
        if (length != otherLength) {
            return false;
        }

        if ((this == that) && (offset == otherOffset)) {
            return true;
        }

        checkIndexLength(offset, length);
        that.checkIndexLength(otherOffset, otherLength);

        return equalsUnchecked(offset, that, otherOffset, length);
    }

    @SuppressWarnings("ObjectEquality")
    public boolean equals(int offset, int length, byte[] that, int otherOffset, int otherLength) {
        if (length != otherLength) {
            return false;
        }

        checkIndexLength(offset, length);
        checkPositionIndexes(otherOffset, otherOffset + otherLength, that.length);

        return equalsUnchecked(offset, that, otherOffset, length);
    }

    boolean equalsUnchecked(int offset, Slice that, int otherOffset, int length) {
        long thisAddress = address + offset;
        long thatAddress = that.address + otherOffset;

        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, thisAddress);
            long thatLong = unsafe.getLong(that.base, thatAddress);

            if (thisLong != thatLong) {
                return false;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(base, thisAddress);
            byte thatByte = unsafe.getByte(that.base, thatAddress);
            if (thisByte != thatByte) {
                return false;
            }
            thisAddress++;
            thatAddress++;
            length--;
        }

        return true;
    }

    boolean equalsUnchecked(int offset, byte[] that, int otherOffset, int length) {
        long thisAddress = address + offset;
        long thatAddress = ARRAY_BYTE_BASE_OFFSET + otherOffset;
        final Object thisBase = base;

        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(thisBase, thisAddress);
            long thatLong = unsafe.getLong(that, thatAddress);

            if (thisLong != thatLong) {
                return false;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(thisBase, thisAddress);
            byte thatByte = unsafe.getByte(that, thatAddress);
            if (thisByte != thatByte) {
                return false;
            }
            thisAddress++;
            thatAddress++;
            length--;
        }

        return true;
    }

    public BasicSliceInput getInput() {
        return new BasicSliceInput(this);
    }

    public SliceOutput getOutput() {
        return new BasicSliceOutput(this);
    }

    public String toString(Charset charset) {
        return toString(0, length(), charset);
    }

    public String toStringUtf8() {
        return toString(UTF_8);
    }

    public String toStringAscii() {
        return toStringAscii(0, size);
    }

    public String toStringAscii(int index, int length) {
        checkIndexLength(index, length);
        if (length == 0) {
            return "";
        }

        if (base instanceof byte[]) {

            return new String((byte[]) base, 0, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
        }

        char[] chars = new char[length];
        for (int pos = index; pos < length; pos++) {
            chars[pos] = (char) (getByteUnchecked(pos) & 0x7F);
        }
        return new String(chars);
    }

    public String toString(int index, int length, Charset charset) {
        if (length == 0) {
            return "";
        }
        if (base instanceof byte[]) {
            return new String((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length, charset);
        }

        return decodeString(toByteBuffer(index, length), charset);
    }

    public ByteBuffer toByteBuffer() {
        return toByteBuffer(0, size);
    }

    public ByteBuffer toByteBuffer(int index, int length) {
        checkIndexLength(index, length);

        if (base instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
        }

        try {
            return (ByteBuffer) newByteBuffer.invokeExact(address + index, length, (Object) reference);
        } catch (Throwable throwable) {
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Slice{");
        if (base != null) {
            builder.append("base=").append(identityToString(base)).append(", ");
        }
        builder.append("address=").append(address);
        builder.append(", length=").append(length());
        builder.append('}');
        return builder.toString();
    }

    private static String identityToString(Object o) {
        if (o == null) {
            return null;
        }
        return o.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(o));
    }

    private static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, int length) {

        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }

    private void checkIndexLength(int index, int length) {
        checkPositionIndexes(index, index + length, length());
    }

    private static long fillLong(byte value) {
        return (value & 0xFFL) << 56
            | (value & 0xFFL) << 48
            | (value & 0xFFL) << 40
            | (value & 0xFFL) << 32
            | (value & 0xFFL) << 24
            | (value & 0xFFL) << 16
            | (value & 0xFFL) << 8
            | (value & 0xFFL);
    }

    private static int compareUnsignedBytes(byte thisByte, byte thatByte) {
        return unsignedByteToInt(thisByte) - unsignedByteToInt(thatByte);
    }

    private static int unsignedByteToInt(byte thisByte) {
        return thisByte & 0xFF;
    }

    private static long longBytesToLong(long bytes) {
        return Long.reverseBytes(bytes) ^ Long.MIN_VALUE;
    }
}
