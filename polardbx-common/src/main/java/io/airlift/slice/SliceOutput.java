
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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

@SuppressWarnings("JavaDoc")
public abstract class SliceOutput
    extends OutputStream
    implements DataOutput {

    public abstract long getMemoryUsage();
    /**
     * Resets this stream to the initial position.
     */
    public abstract void reset();

    public abstract void reset(int position);

    public void ensureCapacity(int capacity) {

    }

    public abstract int size();

    public abstract int getRetainedSize();

    public abstract int writableBytes();

    public abstract boolean isWritable();

    @Override
    public final void writeBoolean(boolean value) {
        writeByte(value ? 1 : 0);
    }

    @Override
    public final void write(int value) {
        writeByte(value);
    }

    @Override
    public abstract void writeByte(int value);

    @Override
    public abstract void writeShort(int value);

    @Override
    public abstract void writeInt(int value);

    @Override
    public abstract void writeLong(long value);

    @Override
    public abstract void writeFloat(float v);

    @Override
    public abstract void writeDouble(double value);

    public abstract void writeBytes(Slice source);

    public abstract void writeBytes(Slice source, int sourceIndex, int length);

    @Override
    public final void write(byte[] source)
        throws IOException {
        writeBytes(source);
    }

    public abstract void writeBytes(byte[] source);

    @Override
    public final void write(byte[] source, int sourceIndex, int length) {
        writeBytes(source, sourceIndex, length);
    }

    public abstract void writeBytes(byte[] source, int sourceIndex, int length);

    public abstract void writeBytes(InputStream in, int length)
        throws IOException;

    public void writeZero(int length) {
        if (length == 0) {
            return;
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                "length must be 0 or greater than 0.");
        }
        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            writeLong(0);
        }
        if (nBytes == 4) {
            writeInt(0);
        } else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                writeByte((byte) 0);
            }
        } else {
            writeInt(0);
            for (int i = nBytes - 4; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
    }

    public abstract Slice slice();

    public abstract Slice getUnderlyingSlice();

    public abstract String toString(Charset charset);

    public abstract SliceOutput appendLong(long value);

    public abstract SliceOutput appendDouble(double value);

    public abstract SliceOutput appendInt(int value);

    public abstract SliceOutput appendShort(int value);

    public abstract SliceOutput appendByte(int value);

    public abstract SliceOutput appendBytes(byte[] source, int sourceIndex, int length);

    public abstract SliceOutput appendBytes(byte[] source);

    public abstract SliceOutput appendBytes(Slice slice);

    @Override
    public void writeChar(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChars(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(String s) {
        throw new UnsupportedOperationException();
    }

    public void skipBytes(int length) {
        throw new UnsupportedOperationException();
    }

    public Slice getRawSlice() {
        throw new UnsupportedOperationException();
    }

    public void setSizeUnchecked(int size) {
        throw new UnsupportedOperationException();
    }
}
