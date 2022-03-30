package com.alibaba.polardbx.common.utils.hash;

import com.google.common.base.Preconditions;

import static com.alibaba.polardbx.common.utils.hash.ByteUtil.putInt64Uncheck;

public abstract class BufferedHasher extends Hasher implements IStreamingHasher {

    private static final int MAX_BUFFER_SIZE = 128;

    protected int pos = 0;
    /**
     * 当前总共处理的字节长度
     */
    protected int processedLength = 0;
    protected final byte[] buffer;
    protected final int bufferSize;
    protected final int blockSize;
    protected final int bufferNBlock;

    protected BufferedHasher(final int bufferSize, final int blockSize, final int seed) {
        super(seed);
        Preconditions.checkArgument(bufferSize > 0, "Buffer size must be positive");
        Preconditions.checkArgument(bufferSize <= MAX_BUFFER_SIZE, "Buffer size cannot exceed " + MAX_BUFFER_SIZE);
        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
        this.blockSize = blockSize;
        this.bufferNBlock = bufferSize / blockSize;
    }

    @Override
    public HashResult128 hash() {
        innerHash();
        reset();
        return this.result;
    }

    protected abstract void innerHash();

    @Override
    public IStreamingHasher putLong(long l) {
        putInt64ToBuffer(l);
        processIfFull();
        return this;
    }

    @Override
    public IStreamingHasher putBytes(byte[] inputBytes) {
        putBytes(inputBytes, 0, inputBytes.length);
        return this;
    }

    private void putBytes(final byte[] inputBytes, final int start, final int length) {
        if (length <= (buffer.length - pos)) {
            // 直接放入剩余空间内
            copyToBuffer(inputBytes, start, length);
            processIfFull();
            return;
        }

        int inputPos = 0;
        int fillLen = blockSize * bufferNBlock - pos;
        inputPos = fillLen;
        // 先填满缓冲区一个block并处理
        copyToBuffer(inputBytes, start, fillLen);
        processBuffer();

        // 由于按8字节对齐 此处pos必定为0
        // 防止以后更改对齐策略
        assert pos == 0;
        // 此时缓冲区为空 直接对输入字节数组进行处理
        int inputRemainNBlock = (length - inputPos) / blockSize;
        int toProcessLen = inputRemainNBlock * blockSize;
        processBlockBody(inputBytes, inputPos, inputRemainNBlock);
        processedLength += toProcessLen;
        inputPos += toProcessLen;
        // 剩余部分放入空的缓冲区
        copyToBuffer(inputBytes, inputPos, length - inputPos);
    }

    protected abstract void processBlockBody(byte[] inputBytes, int inputPos, int inputRemainNBlock);

    /**
     * 按块处理缓冲区
     * 并将残留部分以移动至缓冲区起始位置
     * 事实上由于目前基础类型按照8字节对齐(即long来处理) 不会有残留部分
     */
    protected void processBuffer() {
        processBlockBody(buffer, 0, bufferNBlock);
        int blockEnd = bufferNBlock * blockSize;
        processedLength += blockEnd;

        int remainingLen = pos - blockEnd;
        pos = 0;
        copyToBuffer(buffer, blockEnd, remainingLen);
    }

    protected void processIfFull() {
        if (bufferSize - pos < 8) {
            processBuffer();
        }
    }

    protected void copyToBuffer(byte[] src, int start, int len) {
        if (len > 0) {
            System.arraycopy(src, start, buffer, pos, len);
            pos += len;
        }
    }

    protected void putInt64ToBuffer(long l) {
        putInt64Uncheck(buffer, pos, l);
        pos += 8;
    }
}
