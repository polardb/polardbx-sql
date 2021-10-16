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

import java.util.List;

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;

/**
 * Created by simiao.zw on 2014/8/4.
 */
public class MysqlBinaryResultSetPacket extends MySQLPacket {

    public BinaryResultSetHeaderPacket resulthead;
    public FieldPacket[] fieldPackets;
    public List<BinaryRowDataPacket> rowList;

    @Override
    protected String packetInfo() {
        return "Binary ResultSet Packet";
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();
        // write header
        resulthead.packetId = proxy.getConnection().getNewPacketId();
        resulthead.write(proxy);
        // write fields
        if (this.fieldPackets != null) {
            for (FieldPacket field : this.fieldPackets) {
                field.packetId = proxy.getConnection().getNewPacketId();
                field.write(proxy);
            }
        }
        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = proxy.getConnection().getNewPacketId();
        eof.write(proxy);

        proxy.packetEnd();
        // row data should be deal outside the function since it need other fact
        return proxy;
    }
}
