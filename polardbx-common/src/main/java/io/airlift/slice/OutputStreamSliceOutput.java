
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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public final class OutputStreamSliceOutput
    extends SliceOutput {
    public static final int DEFAULT_BUFFER_SIZE = 4 * 1024;
    private static final int MINIMUM_CHUNK_SIZE = 1024;

    private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(OutputStreamSliceOutput.class).instanceSize();

    private final OutputStream outputStream;

    private final Slice slice;
    private final byte[] buffer;

    private long bufferOffset;

    private int bufferPosition;

    public OutputStreamSliceOutput(OutputStream inputStream) {
        this(inputStream, DEFAULT_BUFFER_SIZE);
    }

    public OutputStreamSliceOutput(OutputStream outputStream, int bufferSize) {
        checkArgument(bufferSize >= MINIMUM_CHUNK_SIZE, "minimum buffer size of " + MINIMUM_CHUNK_SIZE + " required");
        if (outputStream == null) {
            throw new NullPointerException("outputStream is null");
        }

        this.outputStream = outputStream;
        this.buffer = new byte[bufferSize];
        this.slice = Slices.wrappedBuffer(buffer);
    }

    @Override
    public void flush()
        throws IOException {
        flushBufferToOutputStream();
        outputStream.flush();
    }

    @Override
    public void close()
        throws IOException {
        flushBufferToOutputStream();
        outputStream.close();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + FastMemoryCounter.sizeOf(slice) + VMSupport.align((int) SizeOf.sizeOf(buffer));
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("OutputStream can not be reset");
    }

    @Override
    public void reset(int position) {
        throw new UnsupportedOperationException("OutputStream can not be reset");
    }

    @Override
    public int size() {
        return checkedCast(bufferOffset + bufferPosition);
    }

    @Override
    public int getRetainedSize() {
        return slice.getRetainedSize() + INSTANCE_SIZE;
    }

    @Override
    public int writableBytes() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public void writeByte(int value) {
        ensureWritableBytes(SIZE_OF_BYTE);
        slice.setByteUnchecked(bufferPosition, value);
        bufferPosition += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value) {
        ensureWritableBytes(SIZE_OF_SHORT);
        slice.setShortUnchecked(bufferPosition, value);
        bufferPosition += SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value) {
        ensureWritableBytes(SIZE_OF_INT);
        slice.setIntUnchecked(bufferPosition, value);
        bufferPosition += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value) {
        ensureWritableBytes(SIZE_OF_LONG);
        slice.setLongUnchecked(bufferPosition, value);
        bufferPosition += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float value) {
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value) {
        writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void writeBytes(Slice source) {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length) {

        if (length >= MINIMUM_CHUNK_SIZE) {
            flushBufferToOutputStream();
            writeToOutputStream(source, sourceIndex, length);
            bufferOffset += length;
        } else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
        }
    }

    @Override
    public void writeBytes(byte[] source) {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length) {

        if (length >= MINIMUM_CHUNK_SIZE) {
            flushBufferToOutputStream();
            writeToOutputStream(source, sourceIndex, length);
            bufferOffset += length;
        } else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
        }
    }

    @Override
    public void writeBytes(InputStream in, int length)
        throws IOException {
        while (length > 0) {
            int batch = ensureBatchSize(length);
            slice.setBytes(bufferPosition, in, batch);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public void writeZero(int length) {
        checkArgument(length >= 0, "length must be 0 or greater than 0.");

        while (length > 0) {
            int batch = ensureBatchSize(length);
            Arrays.fill(buffer, bufferPosition, bufferPosition + batch, (byte) 0);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public SliceOutput appendByte(int value) {
        writeByte(value);
        return this;
    }

    @Override
    public SliceOutput appendShort(int value) {
        writeShort(value);
        return this;
    }

    @Override
    public SliceOutput appendInt(int value) {
        writeInt(value);
        return this;
    }

    @Override
    public SliceOutput appendLong(long value) {
        writeLong(value);
        return this;
    }

    @Override
    public SliceOutput appendDouble(double value) {
        writeDouble(value);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source, int sourceIndex, int length) {
        writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source) {
        writeBytes(source);
        return this;
    }

    @Override
    public SliceOutput appendBytes(Slice slice) {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getUnderlyingSlice() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(Charset charset) {
        return toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("OutputStreamSliceOutputAdapter{");
        builder.append("outputStream=").append(outputStream);
        builder.append("bufferSize=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    private void ensureWritableBytes(int minWritableBytes) {
        if (bufferPosition + minWritableBytes > slice.length()) {
            flushBufferToOutputStream();
        }
    }

    private int ensureBatchSize(int length) {
        ensureWritableBytes(Math.min(MINIMUM_CHUNK_SIZE, length));
        return Math.min(length, slice.length() - bufferPosition);
    }

    private void flushBufferToOutputStream() {
        writeToOutputStream(buffer, 0, bufferPosition);
        bufferOffset += bufferPosition;
        bufferPosition = 0;
    }

    private void writeToOutputStream(byte[] source, int sourceIndex, int length) {
        try {
            outputStream.write(source, sourceIndex, length);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void writeToOutputStream(Slice source, int sourceIndex, int length) {
        try {
            source.getBytes(sourceIndex, outputStream, length);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private static int checkedCast(long value) {
        int result = (int) value;
        checkArgument(result == value, "Size is greater than maximum int value");
        return result;
    }
}
