
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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@SuppressWarnings("JavaDoc")
public abstract class SliceInput
    extends InputStream
    implements DataInput {

    public abstract long position();

    public abstract void setPosition(long position);

    public abstract boolean isReadable();

    @SuppressWarnings("AbstractMethodOverridesConcreteMethod")
    @Override
    public abstract int available();

    @Override
    public abstract int read();

    @Override
    public abstract boolean readBoolean();

    @Override
    public abstract byte readByte();

    @Override
    public abstract int readUnsignedByte();

    @Override
    public abstract short readShort();

    @Override
    public abstract int readUnsignedShort();

    @Override
    public abstract int readInt();

    public final long readUnsignedInt() {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public abstract long readLong();

    @Override
    public abstract float readFloat();

    @Override
    public abstract double readDouble();

    public abstract Slice readSlice(int length);

    @Override
    public final void readFully(byte[] destination) {
        readBytes(destination);
    }

    @Override
    public final int read(byte[] destination) {
        return read(destination, 0, destination.length);
    }

    @Override
    public abstract int read(byte[] destination, int destinationIndex, int length);

    public final void readBytes(byte[] destination) {
        readBytes(destination, 0, destination.length);
    }

    @Override
    public final void readFully(byte[] destination, int offset, int length) {
        readBytes(destination, offset, length);
    }

    public abstract void readBytes(byte[] destination, int destinationIndex, int length);

    public final void readBytes(Slice destination) {
        readBytes(destination, 0, destination.length());
    }

    public final void readBytes(Slice destination, int length) {
        readBytes(destination, 0, length);
    }

    public abstract void readBytes(Slice destination, int destinationIndex, int length);

    public abstract void readBytes(OutputStream out, int length)
        throws IOException;

    @Override
    public abstract long skip(long length);

    @Override
    public abstract int skipBytes(int length);

    @Override
    public void close() {
    }

    @Override
    public final void mark(int readLimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean markSupported() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final String readUTF() {
        throw new UnsupportedOperationException();
    }
}
