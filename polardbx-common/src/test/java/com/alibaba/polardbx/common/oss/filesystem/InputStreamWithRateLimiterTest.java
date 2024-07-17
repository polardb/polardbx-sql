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

package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.mock.MockUtils;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@RunWith(MockitoJUnitRunner.class)
public class InputStreamWithRateLimiterTest {

    private FileSystemRateLimiter rateLimiter;
    private InputStreamWithRateLimiter inputStreamWithRateLimiter;
    private InputStream inputStream;

    @Before
    public void setUp() {
        rateLimiter = Mockito.mock(FileSystemRateLimiter.class);
        inputStream = Mockito.mock(SeekableByteArrayInputStream.class);
        inputStreamWithRateLimiter = new InputStreamWithRateLimiter(inputStream, rateLimiter);
    }

    @Test
    public void testNewStream() {
        MockUtils.assertThrows(
            IllegalArgumentException.class,
            "In is not an instance of Seekable or PositionedReadable",
            () -> {
                new InputStreamWithRateLimiter(new ByteArrayInputStream(new byte[0]), rateLimiter);
            }
        );
    }

    @Test
    public void testRead() throws IOException {
        // Arrange
        Mockito.when(inputStream.read()).thenReturn(1);
        Mockito.when(inputStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
            .thenReturn(2);
        Mockito.when(inputStream.read(Mockito.any(byte[].class))).thenReturn(3);
        Mockito.when(
            ((PositionedReadable) inputStream).read(Mockito.anyLong(), Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt())).thenReturn(4);

        int byteRead = inputStreamWithRateLimiter.read();
        Assert.assertEquals(1, byteRead);
        Mockito.verify(rateLimiter).acquireRead(1);
        Mockito.verify(inputStream).read();

        byte[] buf = new byte[3];
        byte[] buf2 = new byte[6];
        byteRead = inputStreamWithRateLimiter.read(buf, 0, 2);
        Assert.assertEquals(2, byteRead);
        Mockito.verify(rateLimiter).acquireRead(2);
        Mockito.verify(inputStream).read(buf, 0, 2);

        byteRead = inputStreamWithRateLimiter.read(buf);
        Assert.assertEquals(3, byteRead);
        Mockito.verify(rateLimiter).acquireRead(3);
        Mockito.verify(inputStream).read(buf);

        byteRead = inputStreamWithRateLimiter.read(0, buf2, 0, 4);
        Assert.assertEquals(4, byteRead);
        Mockito.verify(rateLimiter).acquireRead(4);
        Mockito.verify((PositionedReadable) inputStream).read(0, buf2, 0, 4);

        inputStreamWithRateLimiter.readFully(0, buf2, 0, 5);
        Mockito.verify(rateLimiter).acquireRead(5);
        Mockito.verify((PositionedReadable) inputStream).readFully(0, buf2, 0, 5);

        inputStreamWithRateLimiter.readFully(0, buf2);
        Mockito.verify(rateLimiter).acquireRead(6);
        Mockito.verify((PositionedReadable) inputStream).readFully(0, buf2);
    }

    @Test
    public void testForwardingMethods() throws IOException {
        inputStreamWithRateLimiter.seek(1);
        Mockito.verify(((Seekable) inputStream)).seek(1);
        inputStreamWithRateLimiter.seekToNewSource(1);
        Mockito.verify(((Seekable) inputStream)).seekToNewSource(1);
        inputStreamWithRateLimiter.getPos();
        Mockito.verify(((Seekable) inputStream)).getPos();

        inputStreamWithRateLimiter.skip(1);
        Mockito.verify(inputStream).skip(1);

        inputStreamWithRateLimiter.available();
        Mockito.verify(inputStream).available();

        inputStreamWithRateLimiter.mark(1);
        Mockito.verify(inputStream).mark(1);

        inputStreamWithRateLimiter.reset();
        Mockito.verify(inputStream).reset();

        inputStreamWithRateLimiter.markSupported();
        Mockito.verify(inputStream).markSupported();

        inputStreamWithRateLimiter.close();
        Mockito.verify(inputStream).close();
    }

    @Test
    public void testReadWhenRateLimiterThrowsIOException() throws IOException {
        // Arrange
        Mockito.doThrow(new IOException("Rate limit exceeded")).when(rateLimiter).acquireRead(1);

        // Act & Assert
        MockUtils.assertThrows(IOException.class, "Rate limit exceeded", () -> inputStreamWithRateLimiter.read());

        Mockito.verify(rateLimiter).acquireRead(1);
        Mockito.verify(inputStream, Mockito.never()).read();
    }

    public class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > count) {
                throw new IOException("Position out of bounds");
            }
            this.pos = (int) pos;
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position >= count) {
                return -1;
            }
            if (position + length > count) {
                length = count - (int) position;
            }
            System.arraycopy(buf, (int) position, buffer, offset, length);
            return length;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            int nread = read(position, buffer, offset, length);
            if (nread < length) {
                throw new IOException("Reached end of stream");
            }
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }
    }
}