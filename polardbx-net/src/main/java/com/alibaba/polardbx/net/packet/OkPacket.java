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
import com.alibaba.polardbx.net.util.BufferUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;

/**
 * From server to client in response to command, if no error and no result set.
 *
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           OK: 0x00; EOF: 0xFE
 * 1-9 (Length Coded Binary)   affected_rows
 * 1-9 (Length Coded Binary)   insert_id
 * 2                           server_status
 * 2                           warning_count
 * n   (until end of packet)   message fix:(Length Coded String)
 *
 * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
 * </pre>
 *
 * @author xianmao.hexm 2010-7-16 上午10:33:50
 */
public class OkPacket extends MySQLPacket {

    public static final byte OK_HEADER = 0x00;
    public static final byte EOF_HEADER = (byte) 0xFE;
    public static final byte[] OK = new byte[] {7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] OK_WITH_MORE = new byte[] {7, 0, 0, 1, 0, 0, 0, 10, 0, 0, 0};

    public byte header;
    public long affectedRows;
    public long insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message;

    public OkPacket() {
        this(false);
    }

    public OkPacket(boolean isEof) {
        if (isEof) {
            this.header = EOF_HEADER;
        } else {
            this.header = OK_HEADER;
        }
    }

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        header = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        header = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);

        proxy.write(header);
        proxy.writeLength(affectedRows);
        proxy.writeLength(insertId);
        proxy.writeUB2(serverStatus);
        proxy.writeUB2(warningCount);
        if (message != null) {
            proxy.writeWithLength(message);
        }

        proxy.packetEnd();
        return proxy;
    }

    protected int getPacketLength() {
        int i = 1;
        i += BufferUtil.getLength(affectedRows);
        i += BufferUtil.getLength(insertId);
        i += 4;
        if (message != null) {
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String packetInfo() {
        return "MySQL OK Packet";
    }

}
