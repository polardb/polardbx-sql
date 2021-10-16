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
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 *
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
 * </pre>
 *
 * @author xianmao.hexm 2010-7-22 下午05:43:34
 */
public class FieldPacket extends MySQLPacket {

    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[] {0, 0};

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public int charsetIndex;
    public long length;
    public int type;
    public int flags;
    public byte decimals;
    public byte[] definition;
    // 未解包的数据
    public byte[] unpacked = null;
    public Object field = null;
    public boolean isFieldList = false;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        this.packetLength = mm.readUB3();
        this.packetId = mm.read();
        this.catalog = mm.readBytesWithLength();
        this.db = mm.readBytesWithLength();
        this.table = mm.readBytesWithLength();
        this.orgTable = mm.readBytesWithLength();
        this.name = mm.readBytesWithLength();
        this.orgName = mm.readBytesWithLength();
        mm.move(1);
        this.charsetIndex = mm.readUB2();
        this.length = mm.readUB4();
        this.type = mm.read() & 0xff;
        this.flags = mm.readUB2();
        this.decimals = mm.read();
        mm.move(FILLER.length);
        if (mm.hasRemaining()) {
            this.definition = mm.readBytesWithLength();
        }
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();

        int size = getPacketLength();
        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize() + size);
        proxy.writeUB3(size);
        proxy.write(packetId);
        byte nullVal = 0;
        if (this.unpacked != null) {
            proxy.write(unpacked);
        } else {
            proxy.writeWithLength(catalog, nullVal);
            proxy.writeWithLength(db, nullVal);
            proxy.writeWithLength(table, nullVal);
            proxy.writeWithLength(orgTable, nullVal);
            proxy.writeWithLength(name, nullVal);
            proxy.writeWithLength(orgName, nullVal);
            proxy.write((byte) 0x0C);
            proxy.writeUB2(charsetIndex);
            proxy.writeUB4(length);
            proxy.write((byte) (type & 0xff));
            proxy.writeUB2(flags);
            proxy.write(decimals);
            proxy.write((byte) 0x00);
            proxy.write((byte) 0x00);
            // buffer.position(buffer.position() + FILLER.length);
            if (definition != null) {
                proxy.writeWithLength(definition);
            }
        }

        proxy.packetEnd();
        return proxy;
    }

    private int getPacketLength() {
        // Object field = this.field;
        // "std".getBytes("GBK")
        if (this.unpacked != null) {
            return unpacked.length;
        } else {
            int size = (catalog == null ? 1 : BufferUtil.getLength(catalog));
            size += (db == null ? 1 : BufferUtil.getLength(db));
            size += (table == null ? 1 : BufferUtil.getLength(table));
            size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
            size += (name == null ? 1 : BufferUtil.getLength(name));
            size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
            size += 13;// 1+2+4+1+2+1+2
            if (definition != null) {
                size += BufferUtil.getLength(definition);
            }

            return size;
        }
    }

    @Override
    protected String packetInfo() {
        return "MySQL Field Packet";
    }

}
