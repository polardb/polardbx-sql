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

import java.io.OutputStream;

/**
 * Created by simiao on 15-4-17.
 */
public class PacketOutputProxyFactory {

    private static final PacketOutputProxyFactory _instance = new PacketOutputProxyFactory();

    public static PacketOutputProxyFactory getInstance() {
        return _instance;
    }

    public IPacketOutputProxy createProxy(FrontendConnection c, ByteBufferHolder buffer) {
        if (c.isCompressProto()) {
            return new CompressPacketByteBufferOutputProxy(c, buffer);
        } else {
            return new RawPacketByteBufferOutputProxy(c, buffer);
        }
    }

    public IPacketOutputProxy createProxy(FrontendConnection c) {
        if (c.isCompressProto()) {
            return new CompressPacketByteBufferOutputProxy(c);
        } else {
            return new RawPacketByteBufferOutputProxy(c);
        }
    }

    public IPacketOutputProxy createProxy(FrontendConnection c, ByteBufferHolder buffer,
                                          boolean forceUseCompressProto) {
        if (forceUseCompressProto) {
            return new CompressPacketByteBufferOutputProxy(c, buffer);
        } else {
            return new RawPacketByteBufferOutputProxy(c, buffer);
        }
    }

    public IPacketOutputProxy createProxy(FrontendConnection c, boolean forceUseCompressProto) {
        if (forceUseCompressProto) {
            return new CompressPacketByteBufferOutputProxy(c);
        } else {
            return new RawPacketByteBufferOutputProxy(c);
        }
    }

    public IPacketOutputProxy createProxy(boolean isCompressProto, OutputStream out) {
        if (isCompressProto) {
            return new CompressPacketStreamOutputProxy(out);
        } else {
            return new RawPacketStreamOutputProxy(out);
        }
    }

    public IPacketOutputProxy createProxy(boolean isCompressProto, OutputStream out, long compressThreshold) {
        if (isCompressProto) {
            return new CompressPacketStreamOutputProxy(out, compressThreshold);
        } else {
            return new RawPacketStreamOutputProxy(out);
        }
    }

    public IPacketOutputProxy createProxy(OutputStream out) {
        /* Manager channel总是用非压缩协议 */
        return createProxy(false, out);
    }
}
