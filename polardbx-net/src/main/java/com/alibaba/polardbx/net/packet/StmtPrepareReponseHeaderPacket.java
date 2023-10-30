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
public class StmtPrepareReponseHeaderPacket extends MySQLPacket {

    public byte status = 00; // (1) always be 00
    public int statement_id;   // (4)
    public int num_columns;    // (2) number of columns
    public int num_params;     // (2) number of params
    public byte reserved_1 = 00; // (1) always be 00
    public short warning_count;  // (2) number of warnings

    public StmtPrepareReponseHeaderPacket() {
        packetLength = 12; // fixed
        packetId = 1;
    }

    @Override
    protected String packetInfo() {
        return "Mysql COM_STMT_PREPARE response header";
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize() + packetLength);

        // common part
        proxy.writeUB3(packetLength);
        proxy.write(packetId);

        // COM_STMT_PREPARE response specific
        proxy.write(status);
        proxy.writeUB4(statement_id);
        proxy.writeUB2(num_columns); // number of fields
        proxy.writeUB2(num_params);
        proxy.write(reserved_1);
        proxy.writeUB2(warning_count);

        proxy.packetEnd();
        return proxy;
    }
}
