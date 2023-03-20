package com.alibaba.polardbx.common.oss.filesystem;

import com.emc.ecs.nfsclient.nfs.NfsReadResponse;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import org.apache.hadoop.fs.FSInputStream;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class NFSDataInputStream extends FSInputStream {
    public static final int EOF = -1;
    private NfsFile<?, ?> file;
    private long offset;
    private final byte[] bytes;
    private int bytesInBuffer;
    private int currentBufferPosition;
    private boolean isEof;
    private boolean closed;

    public NFSDataInputStream(NfsFile<?, ?> nfsFile, long offset, int maximumBufferSize) throws IOException {
        this.bytesInBuffer = 0;
        this.currentBufferPosition = 0;
        this.isEof = false;
        this.closed = false;
        if (offset < 0L) {
            throw new IllegalArgumentException("Cannot start reading before offset 0: " + offset);
        } else if (maximumBufferSize <= 0) {
            throw new IllegalArgumentException("Cannot have a maximum buffer size <= 0: " + maximumBufferSize);
        } else if (!nfsFile.canRead()) {
            throw new IllegalArgumentException("The file must be readable by the client: " + nfsFile.getAbsolutePath());
        } else {
            this.file = nfsFile;
            this.offset = offset;
            maximumBufferSize =
                Math.min(maximumBufferSize, (int) Math.min(this.file.fsinfo().getFsInfo().rtmax, 2147483647L));
            this.bytes = this.makeBytes(maximumBufferSize);
        }
    }

    public NFSDataInputStream(NfsFile<?, ?> nfsFile, int maximumBufferSize) throws IOException {
        this(nfsFile, 0L, maximumBufferSize);
    }

    private byte[] makeBytes(int maximumBufferSize) throws IOException {
        int bufferSize = Math.min((int) Math.min(this.file.length() - this.offset, 2147483647L), maximumBufferSize);
        if (bufferSize == 0) {
            this.isEof = true;
        }

        return new byte[bufferSize];
    }

    @Override
    public int available() throws IOException {
        this.checkForClosed();
        return (int) Math.min(this.file.length() - this.offset + (long) this.bytesLeftInBuffer(), 2147483647L);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        super.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int bytesRead = this.read(b, 0, 1);
        return bytesRead == -1 ? -1 : b[0] & 255;
    }

    @Override
    public int read(byte @NotNull [] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) throws IOException {
        this.checkForClosed();
        if (off >= 0 && len >= 0 && off + len <= b.length) {
            if (len == 0) {
                return 0;
            } else {
                this.loadBytesAsNeeded();
                int bytesImmediatelyAvailable = this.bytesLeftInBuffer();
                if (len <= bytesImmediatelyAvailable) {
                    System.arraycopy(this.bytes, this.currentBufferPosition, b, off, len);
                    this.currentBufferPosition += len;
                    return len;
                } else {
                    int bytesRead = -1;
                    if (bytesImmediatelyAvailable > 0) {
                        bytesRead = this.read(b, off, bytesImmediatelyAvailable);
                        if (bytesRead != -1) {
                            int furtherBytesRead = this.read(b, off + bytesRead, len - bytesRead);
                            if (furtherBytesRead != -1) {
                                bytesRead += furtherBytesRead;
                            }
                        }
                    }

                    return bytesRead;
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        this.checkForClosed();
        super.reset();
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        this.checkForClosed();
        long bytesSkipped = 0L;

        while (bytesToSkip > (long) this.bytesLeftInBuffer()) {
            bytesSkipped += this.bytesLeftInBuffer();
            bytesToSkip -= this.bytesLeftInBuffer();
            this.currentBufferPosition = this.bytesInBuffer;
            if (this.isEof) {
                break;
            }

            this.loadBytesAsNeeded();
        }

        if (bytesToSkip > 0L && bytesToSkip <= (long) this.bytesLeftInBuffer()) {
            this.currentBufferPosition += (int) bytesToSkip;
            bytesSkipped += (int) bytesToSkip;
        }

        return bytesSkipped;
    }

    @Override
    public void seek(long l) throws IOException {
        this.checkForClosed();
        if (l >= this.file.length()) {
            throw new IOException(String.format("Seeking position is out of bound: %d!", l));
        }

        if (l >= this.offset - this.bytesInBuffer && l < this.offset) {
            /* optimize seek using buffer */
            /*
             *        l                     offset
             *        |                       |
             *   ------------------------------
             *   |       bytesInBuffer        |
             *   ------------------------------
             *                 |
             *               getPos()
             */
            this.currentBufferPosition = (int) (l - (this.offset - this.bytesInBuffer));
        } else {
            this.offset = l;
            this.bytesInBuffer = 0;
            this.currentBufferPosition = 0;
        }

        this.isEof = this.available() <= 0;
    }

    @Override
    public long getPos() throws IOException {
        this.checkForClosed();
        return this.offset - this.bytesLeftInBuffer();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        this.checkForClosed();
        return false;
    }

    private int bytesLeftInBuffer() {
        return this.bytesInBuffer - this.currentBufferPosition;
    }

    private void checkForClosed() throws IOException {
        if (this.closed) {
            throw new IOException("This stream has been closed.");
        }
    }

    private void loadBytesAsNeeded() throws IOException {
        this.isEof = this.available() <= 0;

        while (!this.isEof && this.bytesLeftInBuffer() <= 0) {
            this.currentBufferPosition = 0;
            NfsReadResponse response =
                this.file.read(this.offset, this.bytes.length, this.bytes, this.currentBufferPosition);
            this.bytesInBuffer = response.getBytesRead();
            this.offset += this.bytesInBuffer;
            this.isEof = response.isEof();
        }

    }
}
