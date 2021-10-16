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

/**
 * Created by simiao.zw on 2014/8/1.
 * http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
 */
public class MysqlStmtPrepareResponsePacket extends MySQLPacket {

    public StmtPrepareReponseHeaderPacket head;
    /**
     * if num_params > 0, it contains num_params paramPackets + EOF_Packet
     */
    public FieldPacket[] paramPackets;
    /**
     * if num_columns > 0, it contains num_columns paramPacket + EOF_Packet
     */
    public FieldPacket[] fieldPackets;

    @Override
    protected String packetInfo() {
        return "Mysql COM_STMT_PREPARE response packet";
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        byte packetId = 0;

        // write header
        head.packetId = ++packetId;
        head.write(proxy);

        // params
        if (head.num_params > 0 && (paramPackets != null && paramPackets.length == head.num_params)) {
            for (FieldPacket fp : paramPackets) {
                fp.packetId = ++packetId;
                fp.write(proxy);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            eof.write(proxy);
        }

        // columns
        if (head.num_columns > 0 && (fieldPackets != null && fieldPackets.length == head.num_columns)) {
            for (FieldPacket fp : fieldPackets) {
                fp.packetId = ++packetId;
                fp.write(proxy);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            eof.write(proxy);
        }

        proxy.packetEnd();
        return proxy;
    }
}
