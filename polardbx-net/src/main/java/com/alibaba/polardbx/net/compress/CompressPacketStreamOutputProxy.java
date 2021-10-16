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
import com.alibaba.polardbx.net.util.StreamUtil;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 这个类同时处理真正压缩和小包免压缩功能 Created by simiao on 15-4-17.
 */
public class CompressPacketStreamOutputProxy extends PacketStreamOutputProxy {

    private int nestedPacketCount = 0;
    private long waitForCompressContentLen = 0;
    private long compressThreadhold = 1024 * 1024; /* 没有设置threadhold */

    public CompressPacketStreamOutputProxy(OutputStream out) {
        super(out);
    }

    public CompressPacketStreamOutputProxy(OutputStream out, long compressThreshold) {
        super(out);
        if (compressThreshold > 0) {
            this.compressThreadhold = compressThreshold;
        }
    }

    @Override
    public void checkWriteCapacity(int capacity) {
        /* 对于输出流不用检查是否够 */
    }

    @Override
    public FrontendConnection getConnection() {
        throw new NotSupportException("CompressPacketStreamOutputProxy not support getConnection");
    }

    @Override
    public void write(byte[] src) {
        try {
            waitForCompressStream.write(src);
            waitForCompressContentLen += src.length;
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void write(byte[] src, int off, int len) {
        waitForCompressStream.write(src, off, len);
        waitForCompressContentLen += len;
    }

    @Override
    public void packetBegin() {
        if (nestedPacketCount++ == 0) {
            waitForCompressStream = new ByteArrayOutputStream();
        }
    }

    @Override
    public void packetEnd() {
        int nested = --nestedPacketCount;
        if (nested < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "packetEnd nested: " + nested);
        } else if (nested == 0) {
            /* 真正压缩所有ByteBuffer并发送 */
            compressAndSend();
            try {
                waitForCompressStream.flush();
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
            /* 防止proxy被重用 */
            try {
                waitForCompressStream.close();
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
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

    private void compressAndSend() {
        waitForCompressContentLen = 0;

        if (waitForCompressStream.size() <= MIN_COMPRESS_LENGTH) {
            smallUncompressAndSend(waitForCompressStream.toByteArray());
        } else {
            /* 压缩内容较大涉及不超过最大大小分片，但对于drds的压缩阈值判断不在这里 */
            splitCompressAndSent(waitForCompressStream.toByteArray(), new CompressSpliter() {

                @Override
                public void sendCompressPiece(byte[] content, byte sequenceId, int beforeLen) {
                    /**
                     * http://dev.mysql.com/doc/internals/en/example-several-
                     * mysql-packets.html
                     */
                    try {
                        StreamUtil.writeUB3(out, content.length);
                        StreamUtil.write(out, sequenceId);
                        StreamUtil.writeUB3(out, beforeLen);
                        // 输出压缩内容
                        out.write(content);
                    } catch (IOException e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_NET_SEND, e, e.getMessage());
                    }
                }
            });
        }
    }

    private void smallUncompressAndSend(byte[] content) {
        /**
         * http://dev.mysql.com/doc/internals/en/uncompressed-payload.html
         */
        try {
            StreamUtil.writeUB3(out, content.length);
            StreamUtil.write(out, (byte) 0);
            StreamUtil.writeUB3(out, 0);
            /* 输出原始内容 */
            out.write(content);
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_NET_SEND, e, e.getMessage());
        }
    }
}
