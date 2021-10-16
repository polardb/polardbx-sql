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
import java.util.Arrays;
import java.util.List;

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.BufferUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;

/**
 * From server to client. One packet for each row in the result set.
 *
 * <pre>
 * Bytes                   Name
 * -----                   ----
 * n (Length Coded String) (column value)
 * ...
 *
 * (column value):         The data in the column, as a character string.
 *                         If a column is defined as non-character, the
 *                         server converts the value into a character
 *                         before sending it. Since the value is a Length
 *                         Coded String, a NULL can be represented with a
 *                         single byte containing 251(see the description
 *                         of Length Coded Strings in section "Elements" above).
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
 * </pre>
 *
 * @author xianmao.hexm 2010-7-23 上午01:05:55
 */
public class RowDataPacket extends MySQLPacket {

    protected static final byte NULL_MARK = (byte) 251;

    public final int fieldCount;
    public final List<byte[]> fieldValues;

    public int size = -1;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
    }

    public RowDataPacket(int fieldCount, byte[][] rowBytes1) {
        this.fieldCount = fieldCount;
        this.fieldValues = Arrays.asList(rowBytes1);
    }

    public void add(byte[] value) {
        fieldValues.add(value);
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        for (int i = 0; i < fieldCount; i++) {
            fieldValues.add(mm.readBytesWithLength());
        }
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize());
        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);

        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv == null) {
                proxy.checkWriteCapacity(1);
                proxy.write(RowDataPacket.NULL_MARK);
            } else {
                proxy.checkWriteCapacity(BufferUtil.getLength(fv.length));
                proxy.writeLength(fv.length);
                proxy.write(fv);
            }
        }

        proxy.packetEnd();
        return proxy;
    }

    protected int getPacketLength() {
        if (this.size != -1) {
            return this.size;
        }

        int size = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] v = fieldValues.get(i);
            size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
        }
        return size;
    }

    @Override
    protected String packetInfo() {
        return "MySQL RowData Packet";
    }

}
