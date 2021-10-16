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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.PacketUtil;

/**
 * 简单select语句(select 常量表达式或session变量).
 *
 * @author arnkore 2016-12-26 17:07
 */
public abstract class AbstractSimpleSelect {

    protected ResultSetHeaderPacket header;

    protected FieldPacket[] fields;

    protected EOFPacket eof = new EOFPacket();

    protected RowDataPacket row;

    protected EOFPacket lastEof = new EOFPacket();

    protected int fieldCount;

    private boolean inited = false;

    public AbstractSimpleSelect(int fieldCount) {
        this.fieldCount = fieldCount;
        fields = new FieldPacket[fieldCount];
        row = new RowDataPacket(fieldCount);
    }

    public void init() {
        if (inited) {
            return;
        }

        byte packetId = 0;

        // init header
        header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;

        // init field
        initFields(++packetId);
        // 初始化为最后一个FieldPacket的packetId
        packetId = fields[fields.length - 1].packetId;

        // init eof
        eof.packetId = ++packetId;

        // init row
        row.packetId = ++packetId;

        // init last eof
        lastEof.packetId = ++packetId;

        inited = true;
    }

    private void write(IPacketOutputProxy proxy, ManagerConnection c) {
        init();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        initRowData(c.getCharset());
        proxy = row.write(proxy);

        // write last eof
        lastEof.write(proxy);
    }

    public void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();
        write(proxy, c);
        proxy.packetEnd();
    }

    protected abstract void initFields(byte packetId);

    protected abstract void initRowData(String resultCharset);

    public ResultSetHeaderPacket getHeader() {
        return header;
    }

    public FieldPacket[] getFields() {
        return fields;
    }

    public EOFPacket getEof() {
        return eof;
    }

    public RowDataPacket getRow() {
        return row;
    }

    public EOFPacket getLastEof() {
        return lastEof;
    }

    public int getFieldCount() {
        return fieldCount;
    }
}
