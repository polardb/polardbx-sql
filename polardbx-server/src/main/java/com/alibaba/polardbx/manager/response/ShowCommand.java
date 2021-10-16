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
import com.alibaba.polardbx.net.handler.CommandCount;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;

/**
 * 统计各类数据包的执行次数
 *
 * @author xianmao.hexm 2010-9-29 下午03:06:42
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowCommand {

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("PROCESSOR", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INIT_DB", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUERY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("STMT_PREPARE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("STMT_EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("STMT_CLOSE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PING", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("KILL", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUIT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OTHER", Fields.FIELD_TYPE_LONGLONG);
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
        CommandCount cc = processor.getCommands();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(processor.getName().getBytes());
        row.add(LongUtil.toBytes(cc.initDBCount()));
        row.add(LongUtil.toBytes(cc.queryCount()));
        row.add(LongUtil.toBytes(cc.stmtPrepareCount()));
        row.add(LongUtil.toBytes(cc.stmtExecuteCount()));
        row.add(LongUtil.toBytes(cc.stmtCloseCount()));
        row.add(LongUtil.toBytes(cc.pingCount()));
        row.add(LongUtil.toBytes(cc.killCount()));
        row.add(LongUtil.toBytes(cc.quitCount()));
        row.add(LongUtil.toBytes(cc.otherCount()));
        return row;
    }

}
