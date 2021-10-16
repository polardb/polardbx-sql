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

package com.alibaba.polardbx.net.compress;

import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.util.BufferUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 这个类同时处理真正压缩和小包免压缩功能 Created by simiao on 15-4-17.
 */
public class CompressPacketByteBufferOutputProxy extends PacketByteBufferOutputProxy {

    private LinkedBlockingQueue<ByteBufferHolder> waitForCompressQueue;
    private long waitForCompressContentLen = 0;
    private long compressThreadhold = 1024 * 1024; /* 没有设置threadhold */

    private int nestedPacketCount = 0;

    public CompressPacketByteBufferOutputProxy(FrontendConnection c) {
        super(c);
        if (c.getPacketCompressThreshold() > 0) {
            this.compressThreadhold = c.getPacketCompressThreshold();
        }
    }

    public CompressPacketByteBufferOutputProxy(FrontendConnection c, ByteBufferHolder buff) {
        super(c, buff);
        if (c.getPacketCompressThreshold() > 0) {
            this.compressThreadhold = c.getPacketCompressThreshold();
        }
    }

    @Override
    public void packetBegin() {
        if (nestedPacketCount++ == 0) {
            waitForCompressQueue = new LinkedBlockingQueue<ByteBufferHolder>();
        }
    }

    /**
     * currentBuffer一定不在等待队列中，处在等待队列中的ByteBuffer也不一定是完整的packet
     */
    @Override
    public void packetEnd() {
        int nested = --nestedPacketCount;
        if (nested < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "packetEnd nested: " + nested);
        } else if (nested == 0) {
            /* 真正压缩所有ByteBuffer并发送 */
            compressAndSend();
            /* 防止proxy被重用 */
            waitForCompressQueue = null;
            /* 这里表示复合packet发送完成，sequence被重置 */
            sequenceReset();
        } else {
            /**
             * 通常没有到复合包的最终结尾，但对于很大的返回值可能一次全部压缩很占内存并且会造成
             * 网络突发大数据包，所以这里根据动态配置的大小决定是否可以输出
             */
            if ((waitForCompressContentLen > 0 && compressThreadhold > 0)
                && waitForCompressContentLen >= compressThreadhold) {
                compressAndSend();
            }
        }
    }

    private void enqueue(ByteBufferHolder buffer) {
        while (true) {
            try {
                waitForCompressQueue.put(buffer);
                break;
            } catch (InterruptedException e) {
                /**
                 * 等待写锁被中断，buffer还未放入则重试
                 */
            }
        }
    }

    /**
     * 挂Queue
     */
    @Override
    public void checkWriteCapacity(int capacity) {
        /* 检查是否首先调用了begin */
        if (nestedPacketCount < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "nestedPacketCount: " + nestedPacketCount);
        }

        if (capacity > currentBuffer.remaining()) {
            enqueue(currentBuffer);
            currentBuffer = c.allocate();
        }
    }

    /**
     * 挂Queue
     */
    @Override
    public void write(byte[] src) {
        write(src, 0, src.length);
    }

    /**
     * 挂Queue
     */
    @Override
    public void write(byte[] src, int offset, int length) {
        /* 检查是否首先调用了begin */
        if (nestedPacketCount < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "nestedPacketCount:" + nestedPacketCount);
        }

        int remaining = currentBuffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                currentBuffer.put(src, offset, length);
                break;
            } else {
                currentBuffer.put(src, offset, remaining);
                enqueue(currentBuffer);
                currentBuffer = c.allocate();
                offset += remaining;
                length -= remaining;
                remaining = currentBuffer.remaining();
                continue;
            }
        }
        // 只有在这里表示真正写入缓冲的内容，enqueue太晚因为每个buffer可能很大而不会触发
        waitForCompressContentLen += src.length;
    }

    /**
     * 对于多段byte[]，必须连接在一次才能计算压缩，这里必须涉及一次拷贝， 对于非常小的完整packet可以不真正压缩而添加一个压缩头部直接输出
     */
    private void compressAndSend() {
        /* 临时原始拼接区 */
        ByteArrayOutputStream origOut = new ByteArrayOutputStream();
        for (ByteBufferHolder byteBuffer : waitForCompressQueue) {
            origOut.write(byteBuffer.array(), 0, byteBuffer.position());
            byteBuffer.clear();
        }
        waitForCompressQueue.clear();
        waitForCompressContentLen = 0;

        origOut.write(currentBuffer.array(), 0, currentBuffer.position());
        currentBuffer.clear();

        if (origOut.size() <= MIN_COMPRESS_LENGTH) {
            smallUncompressAndSend(origOut.toByteArray());
        } else {
            /* 压缩内容较大涉及不超过最大大小分片，但对于drds的压缩阈值判断不在这里 */
            splitCompressAndSent(origOut.toByteArray(), new CompressSpliter() {

                @Override
                public void sendCompressPiece(byte[] content, byte sequenceId, int beforeLen) {
                    /* 用于网络输出的buffer */
                    ByteBufferHolder outputBuffer = c.allocate();
                    /**
                     * http://dev.mysql.com/doc/internals/en/example-several-
                     * mysql-packets.html
                     */
                    BufferUtil.writeUB3(outputBuffer.getBuffer(), content.length);

                    outputBuffer.put(sequenceId);
                    BufferUtil.writeUB3(outputBuffer.getBuffer(), beforeLen);
                    //*writer index not change compensate，
                    outputBuffer.writerIndex(outputBuffer.writerIndex() + 6);
                    // 输出压缩内容
                    outputBuffer = c.checkWriteBuffer(outputBuffer, content.length);
                    outputBuffer = c.writeToBuffer(content, outputBuffer);

                    c.write(outputBuffer);
                }
            });
        }
    }

    private void smallUncompressAndSend(byte[] content) {
        ByteBufferHolder outputBuffer = c.allocate();
        /**
         * http://dev.mysql.com/doc/internals/en/uncompressed-payload.html
         */
        BufferUtil.writeUB3(outputBuffer.getBuffer(), content.length); // compress
        // payload
        // length
        outputBuffer.put((byte) 1); // sequenceId总是1
        BufferUtil.writeUB3(outputBuffer.getBuffer(), 0); // 总是0

        //*writer index not change compensate，
        outputBuffer.writerIndex(outputBuffer.writerIndex() + 6);
        /* 输出原始内容 */
        outputBuffer = c.checkWriteBuffer(outputBuffer, content.length);
        outputBuffer = c.writeToBuffer(content, outputBuffer);

        c.write(outputBuffer);
    }

}
