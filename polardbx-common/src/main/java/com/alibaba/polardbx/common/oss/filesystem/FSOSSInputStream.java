package com.alibaba.polardbx.common.oss.filesystem;

import org.apache.hadoop.fs.FSInputStream;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FSOSSInputStream extends FSInputStream {
    private final OSSFileSystemStore ossFileSystemStore;
    private final String ossKeyPath;

    private final int bufferSize;
    private InputStream inputStream;

    private final long contentLength;
    private long pos;

    public FSOSSInputStream(OSSFileSystemStore ossFileSystemStore, String ossKeyPath, int bufferSize) {
        this.ossFileSystemStore = ossFileSystemStore;
        this.ossKeyPath = ossKeyPath;
        this.bufferSize = bufferSize;
        this.inputStream = new BufferedInputStream(ossFileSystemStore.retrieve(ossKeyPath, 0, -1), bufferSize);
        this.pos = 0;
        this.contentLength = ossFileSystemStore.getOssFileLength(ossKeyPath);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            return;
        }
        close();
        InputStream inputStream1 =
            new BufferedInputStream(ossFileSystemStore.retrieve(ossKeyPath, pos, -1), bufferSize);
        this.pos = pos;
        this.inputStream = inputStream1;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException("seekToNewSource is not supported!");
    }

    @Override
    public int read() throws IOException {
        int data = inputStream.read();
        pos++;
        return data;
    }

    @Override
    public int read(byte @NotNull [] b) throws IOException {
        int num = inputStream.read(b);
        pos += num;
        return num;
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) throws IOException {
        int num = inputStream.read(b, off, len);
        pos += num;
        return num;
    }

    @Override
    public long skip(long n) throws IOException {
        long num = inputStream.skip(n);
        pos += num;
        return num;
    }

    @Override
    public int available() throws IOException {
        return (int) (contentLength - pos);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
