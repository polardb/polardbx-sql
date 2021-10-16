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

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.handler.FrontendAuthenticator;
import com.alibaba.polardbx.net.util.MySQLMessage;

/**
 * From server to client during initial handshake.
 *
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 *
 * @author xianmao.hexm 2010-7-14 下午05:18:15
 */
public class HandshakePacket extends MySQLPacket {

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2(); // 读取Capability flag 的lower bytes
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();

        // modified by chenghui.lch
        serverCapabilities |= (mm.readUB2() << 16); // 读取Capability的upper bytes
        mm.move(11);
        // mm.move(13);

        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2(); // 读取Capability flag 的lower bytes
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();

        // modified by chenghui.lch
        serverCapabilities |= (mm.readUB2() << 16); // 读取Capability的upper bytes
        mm.move(11);
        // mm.move(13);

        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);

        proxy.write(protocolVersion);
        proxy.writeWithNull(serverVersion);
        proxy.writeUB4(threadId);
        proxy.writeWithNull(seed);
        proxy.writeUB2(serverCapabilities); // lower 2 bytes of
        // Capability Flags
        proxy.write(serverCharsetIndex);
        proxy.writeUB2(serverStatus);

        // modified by chenghui.lch by 2014.11.2
        // extend server Capability flag for CLIENT_MULTI_STATEMENTS,
        // CLIENT_MULTI_RESULTS and CLIENT_PS_MULTI_RESULTS
        proxy.writeUB2(serverCapabilities >>> 16); // upper 2 bytes
        // of Capability
        // Flags

        // buffer.position(buffer.position() + 13);
        // for (int i = 1; i <= 13; i++) {
        // buffer.put((byte) 0);
        // }
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            proxy.write((byte) 21);
        } else {
            proxy.write((byte) 0);
        }
        for (int i = 1; i <= 10; i++) {
            proxy.write((byte) 0);
        }

        proxy.writeWithNull(restOfScrambleBuff);
        proxy.writeWithNull(FrontendAuthenticator.authMethod.getBytes());

        proxy.packetEnd();
        return proxy;
    }

    private int getPacketLength() {
        int size = 1;
        size += serverVersion.length;// n
        size += 5;// 1+4
        size += seed.length;// 8
        size += 19;// 1+2+1+2+13
        size += restOfScrambleBuff.length;// 12
        size += 1;// 1
        size += FrontendAuthenticator.authMethod.getBytes().length;
        size += 1;

        return size;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Handshake Packet";
    }

}
