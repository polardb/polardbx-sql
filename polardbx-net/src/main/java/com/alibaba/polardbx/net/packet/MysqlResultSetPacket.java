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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;

/**
 * @author mengshi.sunmengshi 2013-11-20 下午6:13:48
 * @since 5.1.0
 */
public class MysqlResultSetPacket extends MySQLPacket {

    public ResultSetHeaderPacket resulthead;
    public FieldPacket[] fieldPackets;
    private List<RowDataPacket> rowList;

    public synchronized void addRowDataPacket(RowDataPacket row) {
        if (rowList == null) {
            rowList = new ArrayList<RowDataPacket>();
        }
        rowList.add(row);
    }

    public int calcPacketSize() {
        return 4;
    }

    @Override
    protected String packetInfo() {
        return "ResultSet Packet";
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        byte packetId = 0;
        // write header
        resulthead.packetId = ++packetId;
        resulthead.write(proxy);
        // write fields

        if (this.fieldPackets != null) {
            for (FieldPacket field : this.fieldPackets) {
                field.packetId = ++packetId;
                field.write(proxy);
            }
        }
        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        eof.write(proxy);

        // row data
        if (this.rowList != null) {
            for (RowDataPacket row : this.rowList) {
                row.packetId = ++packetId;
                row.write(proxy);
            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.write(proxy);

        proxy.packetEnd();
        return proxy;
    }

}
