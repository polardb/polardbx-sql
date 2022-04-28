
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;

import static io.airlift.slice.JvmUtils.getAddress;
import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Slices {

    public static final Slice EMPTY_SLICE = new Slice();

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static final int SLICE_ALLOC_THRESHOLD = 524_288;
    private static final double SLICE_ALLOW_SKEW = 1.25;

    private Slices() {
    }

    public static Slice ensureSize(Slice existingSlice, int minWritableBytes) {
        if (existingSlice == null) {
            return allocate(minWritableBytes);
        }

        if (minWritableBytes <= existingSlice.length()) {
            return existingSlice;
        }

        int newCapacity;
        if (existingSlice.length() == 0) {
            newCapacity = 1;
        } else {
            newCapacity = existingSlice.length();
        }
        int minNewCapacity = minWritableBytes;
        while (newCapacity < minNewCapacity) {
            if (newCapacity < SLICE_ALLOC_THRESHOLD) {
                newCapacity <<= 1;
            } else {
                newCapacity *= SLICE_ALLOW_SKEW;
            }
        }

        Slice newSlice = allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    public static Slice allocate(int capacity) {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        checkArgument(capacity <= MAX_ARRAY_SIZE, "Cannot allocate slice larger than " + MAX_ARRAY_SIZE + " bytes");
        return new Slice(new byte[capacity]);
    }

    public static Slice allocateDirect(int capacity) {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return wrappedBuffer(ByteBuffer.allocateDirect(capacity));
    }

    public static Slice copyOf(Slice slice) {
        return copyOf(slice, 0, slice.length());
    }

    public static Slice copyOf(Slice slice, int offset, int length) {
        checkPositionIndexes(offset, offset + length, slice.length());

        Slice copy = Slices.allocate(length);
        copy.setBytes(0, slice, offset, length);

        return copy;
    }

    @Deprecated
    public static Slice wrappedBuffer(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            long address = getAddress(buffer);
            return new Slice(null, address + buffer.position(), buffer.limit() - buffer.position(), buffer.capacity(),
                buffer);
        }

        if (buffer.hasArray()) {
            int address = ARRAY_BYTE_BASE_OFFSET + buffer.arrayOffset() + buffer.position();
            return new Slice(buffer.array(), address, buffer.limit() - buffer.position(), buffer.array().length, null);
        }

        throw new IllegalArgumentException("cannot wrap " + buffer.getClass().getName());
    }

    public static Slice wrappedBuffer(byte... array) {
        if (array.length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array);
    }

    public static Slice wrappedBuffer(byte[] array, int offset, int length, Slice output) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        output.resetSlice(array, offset, length);
        return output;
    }

    public static Slice wrappedBuffer(byte[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedBooleanArray(boolean... array) {
        return wrappedBooleanArray(array, 0, array.length);
    }

    public static Slice wrappedBooleanArray(boolean[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedShortArray(short... array) {
        return wrappedShortArray(array, 0, array.length);
    }

    public static Slice wrappedShortArray(short[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedIntArray(int... array) {
        return wrappedIntArray(array, 0, array.length);
    }

    public static Slice wrappedIntArray(int[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedLongArray(long... array) {
        return wrappedLongArray(array, 0, array.length);
    }

    public static Slice wrappedLongArray(long[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedFloatArray(float... array) {
        return wrappedFloatArray(array, 0, array.length);
    }

    public static Slice wrappedFloatArray(float[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice wrappedDoubleArray(double... array) {
        return wrappedDoubleArray(array, 0, array.length);
    }

    public static Slice wrappedDoubleArray(double[] array, int offset, int length) {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    public static Slice copiedBuffer(String string, Charset charset) {
        requireNonNull(string, "string is null");
        requireNonNull(charset, "charset is null");

        return wrappedBuffer(string.getBytes(charset));
    }

    public static Slice utf8Slice(String string) {
        return copiedBuffer(string, UTF_8);
    }

    public static Slice mapFileReadOnly(File file)
        throws IOException {
        requireNonNull(file, "file is null");

        if (!file.exists()) {
            throw new FileNotFoundException(file.toString());
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            FileChannel channel = randomAccessFile.getChannel()) {
            MappedByteBuffer byteBuffer = channel.map(MapMode.READ_ONLY, 0, file.length());
            return wrappedBuffer(byteBuffer);
        }
    }
}
