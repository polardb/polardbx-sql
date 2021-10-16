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

package com.alibaba.polardbx.net.packet;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.StreamUtil;

/**
 * @author xianmao.hexm 2011-5-6 上午10:58:33
 */
public class BinaryPacket extends MySQLPacket {

    public static final byte OK = 1;
    public static final byte ERROR = 2;
    public static final byte HEADER = 3;
    public static final byte FIELD = 4;
    public static final byte FIELD_EOF = 5;
    public static final byte ROW = 6;
    public static final byte PACKET_EOF = 7;
    public static final byte LOCAL_INFILE = -5;
    public byte[] data;

    public void read(InputStream in) throws IOException {
        packetLength = StreamUtil.readUB3(in);
        packetId = StreamUtil.read(in);
        byte[] ab = new byte[packetLength];
        StreamUtil.read(in, ab, 0, ab.length);
        data = ab;
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();
        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize());

        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);

        proxy.write(data);
        proxy.packetEnd();
        return proxy;
    }

    private int getPacketLength() {
        return data == null ? 0 : data.length;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Binary Packet";
    }

}
