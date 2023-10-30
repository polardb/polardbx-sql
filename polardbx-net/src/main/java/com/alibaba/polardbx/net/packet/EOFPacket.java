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

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.MySQLMessage;

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 *
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     header, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 *
 * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
 * </pre>
 *
 * @author xianmao.hexm 2010-7-16 上午10:55:53
 */
public class EOFPacket extends MySQLPacket {

    public static final int PACKET_LEN = 5; // 1+2+2;
    public static final byte EOF_HEADER = (byte) 0xfe;

    public byte header = EOF_HEADER;
    public int warningCount;
    public int status = SERVER_STATUS_AUTOCOMMIT;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        header = mm.read();
        warningCount = mm.readUB2();
        status = mm.readUB2();
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        if (proxy.getConnection().isEofDeprecated()) {
            // Use Ok packet instead of EOF
            OkPacket ok = new OkPacket(true);
            ok.packetId = packetId;
            ok.serverStatus = status;
            ok.write(proxy);
            return proxy;
        }

        proxy.packetBegin();

        int size = getPacketLength();
        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize() + size);
        proxy.writeUB3(size);
        proxy.write(packetId);

        proxy.write(header);
        proxy.writeUB2(warningCount);
        proxy.writeUB2(status);

        proxy.packetEnd();
        return proxy;
    }

    private int getPacketLength() {
        return PACKET_LEN;
    }

    @Override
    protected String packetInfo() {
        return "MySQL EOF Packet";
    }

}
