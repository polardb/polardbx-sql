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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * Created by simiao on 15-4-17.
 */
public class RawPacketByteBufferOutputProxy extends PacketByteBufferOutputProxy {

    /**
     * 记录当前复合packet的调用深度，当深度为0时的packetEnd才做压缩并发送
     * 不使用AtomicInteger，因为不会多线程使用同一个proxy，可以提高性能
     */
    private int nestedPacketCount = 0;

    public RawPacketByteBufferOutputProxy(FrontendConnection c) {
        super(c);
    }

    public RawPacketByteBufferOutputProxy(FrontendConnection c, ByteBufferHolder buffer) {
        super(c, buffer);
    }

    /**
     * 针对非压缩的情况，直接通过conn边写边发送了，所以这个方法不需要特别处理
     */
    @Override
    public void packetEnd() {
        int nested = --nestedPacketCount;
        if (nested < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "packetEnd nested: " + nested);
        } else if (nested == 0) {
            /**
             * 只有最外层结束时才把缓冲区剩余的内容发送
             */
            c.write(currentBuffer);
        }
    }

    /**
     * 对于非压缩情况，可以在分配前直接输出.记住需要更新currentBuffer
     */
    @Override
    public void checkWriteCapacity(int capacity) {
        // 没用过，直接返回，不用新建

        currentBuffer = c.checkWriteBuffer(currentBuffer, capacity);
    }

    @Override
    public void write(byte[] src) {
        currentBuffer = c.writeToBuffer(src, currentBuffer);
    }

    @Override
    public void write(byte[] src, int off, int len) {
        currentBuffer = c.writeToBuffer(src, off, len, currentBuffer);
    }

    @Override
    public void packetBegin() {
        ++nestedPacketCount;
    }
}
