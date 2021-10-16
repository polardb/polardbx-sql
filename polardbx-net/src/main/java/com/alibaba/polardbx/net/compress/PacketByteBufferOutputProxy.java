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

/**
 * 这里无法直接继承ByteBuffer，因为构造需要额外无关的参数。而且所有的ByteBuffer也都是从conn
 * 中分配并管理的，所以这里继承ByteBuffer的目的仅仅是进行一个包装所以不需要给conn进行管理。 Created by simiao on
 * 15-4-17.
 */
public abstract class PacketByteBufferOutputProxy extends PacketOutputProxyCommon {

    protected FrontendConnection c;
    /**
     * 当前使用的bytebuffer，每次外部调用时，不需要返回前一个还可用的buffer
     * 所以不提供获取ByteBuffer的接口，好处是通用接口IPacketOutputProxy没有针对 输出方式的特别描述
     */
    protected ByteBufferHolder currentBuffer;

    protected boolean checked = false;

    public PacketByteBufferOutputProxy(FrontendConnection c) {
        this(c, c.allocate());
    }

    public PacketByteBufferOutputProxy(FrontendConnection c, ByteBufferHolder buffer) {
        this.c = c;
        currentBuffer = buffer;
    }

    public FrontendConnection getConnection() {
        return c;
    }

    /**
     * 假设只在当前的currentBuffer上进行写操作，而用packetEnd来切换当前的currentBuffer.
     * packetEnd还负责针对压缩的情况推动保存的足量packet进行压缩并发送出去
     */
    public ByteBufferHolder put(byte b) {
        if (!checked) {
            currentBuffer = c.checkWriteBuffer(currentBuffer, 1);
        }
        return currentBuffer.put(b);
    }

    @Override
    public void write(byte b) {
        put(b);
    }

    @Override
    public void writeUB2(int i) {
        put((byte) (i & 0xff));
        put((byte) (i >>> 8));
    }

    @Override
    public void writeUB3(int i) {
        put((byte) (i & 0xff));
        put((byte) (i >>> 8));
        put((byte) (i >>> 16));
    }

    @Override
    public void writeInt(int i) {
        put((byte) (i & 0xff));
        put((byte) (i >>> 8));
        put((byte) (i >>> 16));
        put((byte) (i >>> 24));
    }

    @Override
    public void writeFloat(float f) {
        writeInt(Float.floatToIntBits(f));
    }

    @Override
    public void writeUB4(long l) {
        put((byte) (l & 0xff));
        put((byte) (l >>> 8));
        put((byte) (l >>> 16));
        put((byte) (l >>> 24));
    }

    @Override
    public void writeLong(long l) {
        put((byte) (l & 0xff));
        put((byte) (l >>> 8));
        put((byte) (l >>> 16));
        put((byte) (l >>> 24));
        put((byte) (l >>> 32));
        put((byte) (l >>> 40));
        put((byte) (l >>> 48));
        put((byte) (l >>> 56));
    }

    @Override
    public void writeDouble(double d) {
        writeLong(Double.doubleToLongBits(d));
    }

    @Override
    public void writeLength(long l) {
        if (l < 251) {
            put((byte) l);
        } else if (l < 0x10000L) {
            put((byte) 252);
            writeUB2((int) l);
        } else if (l < 0x1000000L) {
            put((byte) 253);
            writeUB3((int) l);
        } else {
            put((byte) 254);
            writeLong(l);
        }
    }

    @Override
    public void writeWithNull(byte[] src) {
        write(src);
        put((byte) 0);
    }

    @Override
    public void writeWithLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            put((byte) length);
        } else if (length < 0x10000L) {
            put((byte) 252);
            writeUB2(length);
        } else if (length < 0x1000000L) {
            put((byte) 253);
            writeUB3(length);
        } else {
            put((byte) 254);
            writeLong(length);
        }
        write(src);
    }

    @Override
    public void writeWithLength(byte[] src, byte nullValue) {
        if (src == null) {
            put(nullValue);
        } else {
            writeWithLength(src);
        }
    }

    @Override
    public boolean avaliable() {
        return currentBuffer != null;
    }

}
