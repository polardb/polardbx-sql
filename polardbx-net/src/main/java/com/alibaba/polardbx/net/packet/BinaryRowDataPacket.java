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
 * Created by simiao.zw on 2014/8/5.
 * http://dev.mysql.com/doc/internals/en/binary
 * -protocol-resultset-row.html#packet-ProtocolBinary::ResultsetRow
 */
public class BinaryRowDataPacket extends MySQLPacket {

    public static final byte NULL_MARK = (byte) 251;

    public final int fieldCount;
    public final List<byte[]> fieldValues;

    public int size = -1;

    public BinaryRowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
    }

    public BinaryRowDataPacket(int fieldCount, byte[][] rowBytes1) {
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

        proxy.checkWriteCapacity(1);
        proxy.write((byte) 0); // packet header of row in a binary resultset

        // the first two bit is reserved for execute binary result
        byte[] null_bitmap = new byte[(fieldCount + 7 + 2) / 8];
        int bit = 4;
        int nullMaskPos = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);

            if (fv == null) {
                // fill null map
                null_bitmap[nullMaskPos] |= bit;
            }

            // refer to mysql connector MysqlIO.unpackBinaryResultSetRow
            if (((bit <<= 1) & 255) == 0) {
                bit = 1;
                nullMaskPos++;
            }
        }

        // int bytePos = 0;
        // int bitPos = 0;
        // for (int i = 0; i < fieldCount; i++) {
        // byte[] fv = fieldValues.get(i);
        // if (fv == null) {
        // bytePos = (i + 2) / 8;
        // bitPos = (i + 2) % 8;
        // null_bitmap[bytePos] |= 1 << bitPos;
        // }
        // }

        proxy.checkWriteCapacity(null_bitmap.length);
        proxy.write(null_bitmap);

        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv != null) {
                proxy.checkWriteCapacity(BufferUtil.getLength(fv.length));
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

        int size = 1; // first reserved 00;
        size += ((fieldCount + 7 + 2) / 8);
        for (int i = 0; i < fieldCount; i++) {
            byte[] v = fieldValues.get(i);
            if (v != null) {
                size += (v.length == 0) ? 1 : v.length;
            }
        }
        return size;
    }

    @Override
    protected String packetInfo() {
        return "MySQL RowData Packet";
    }
}
