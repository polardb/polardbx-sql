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
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

/**
 * @author xianmao.hexm
 */
public final class ShowConnectionSQL {

    private static final int FIELD_COUNT = 15;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ALIVE_TIME(s)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LAST_ACTIVE(ms)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTE_TIME(ms)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHANNELS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TRX", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NEED_RECONNECT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
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
        String charset = c.getCharset();
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && !fc.isClosed() && fc instanceof ServerConnection) {
                    RowDataPacket row = getRow((ServerConnection) fc, charset);
                    row.packetId = ++packetId;
                    proxy = row.write(proxy);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(ServerConnection sc, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        int count = 0;
        int trx = 0;
        if (sc.getTddlConnection() != null) {
            if (sc.getTddlConnection().getConnectionHolder() == null) {
                count = 0;
            } else {
                count = sc.getTddlConnection().getConnectionHolder().getAllConnection().size();
                if (sc.getTddlConnection().getTrx() != null
                    && !sc.getTddlConnection().getTrx().isClosed()) {
                    // 检查下事务是否关闭
                    trx = 1;
                }
            }
        }

        row.add(LongUtil.toBytes(sc.getId()));
        row.add(StringUtil.encode(sc.getHost(), charset));
        row.add(LongUtil.toBytes(sc.getPort()));
        row.add(LongUtil.toBytes(sc.getLocalPort()));
        row.add(StringUtil.encode(sc.getSchema(), charset));
        row.add(StringUtil.encode(sc.getCharset(), charset));
        row.add(LongUtil.toBytes(sc.getNetInBytes()));
        row.add(LongUtil.toBytes(sc.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - sc.getStartupTime()) / 1000L));
        row.add(LongUtil.toBytes((System.nanoTime() - sc.getLastActiveTime()) / (1000 * 1000)));
        if (sc.getSqlSample() == null || sc.getSqlSample().length() == 0) {
            row.add(LongUtil.toBytes(0));
        } else {
            row.add(LongUtil.toBytes((System.nanoTime() - sc.getLastSqlStartTime()) / (1000 * 1000)));
        }
        row.add(LongUtil.toBytes((count)));
        row.add(LongUtil.toBytes((trx)));
        row.add(LongUtil.toBytes(sc.isNeedReconnect() ? 1 : 0));
        row.add(StringUtil.encode(sc.getSqlSample(), charset));
        return row;
    }

}
