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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.IntegerUtil;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;

/**
 * 查看处理器状态
 *
 * @author xianmao.hexm 2010-1-25 下午01:11:00
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowProcessor {

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        byte packetId = eof.packetId;
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            RowDataPacket row = getRow(p, c.getCharset());
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(NIOProcessor processor, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(processor.getName().getBytes());
        row.add(LongUtil.toBytes(processor.getNetInBytes()));
        row.add(LongUtil.toBytes(processor.getNetOutBytes()));
        row.add(LongUtil.toBytes(processor.getReactCount()));
        row.add(IntegerUtil.toBytes(processor.getRegisterQueueSize()));
        row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
        row.add(IntegerUtil.toBytes(processor.getBufferPool().size()));
        row.add(IntegerUtil.toBytes(processor.getBufferPool().capacity()));
        row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
        row.add(IntegerUtil.toBytes(0));
        return row;
    }

}
