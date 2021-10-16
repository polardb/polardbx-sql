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

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.executor.handler.LogicalShowHtcHandler;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.List;

/**
 * 查询当前系统配置
 *
 * @author agapple 2014年12月12日 上午12:20:45
 * @since 5.1.17
 */
public final class ShowHtc {

    private static final int FIELD_COUNT = 18;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("CURRENT_TIME", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CPU", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOAD", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREEMEM", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NETIN", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NETOUT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NETIO", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FULLGCCOUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FULLGCTIME", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MEMORY_POOL_USAGE", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DDL_JOB_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUERY_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HIGH_RUNNER_PROCESS_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOW_RUNNER_PROCESS_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HIGH_PENDING_SPLIT_SIZE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOW_PENDING_SPLIT_SIZE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCKED_SPLIT_SIZE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("METRICS_INFO", Fields.FIELD_TYPE_STRING);
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

        RowDataPacket row = getRow(c.getCharset(), c.getSchema());
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String charset, String schemaName) {
        List<Object> obs = LogicalShowHtcHandler.getHostInfo4Manager(schemaName);
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        for (Object obj : obs) {
            row.add(StringUtil.encode(obj.toString(), charset));
        }
        return row;
    }

}
