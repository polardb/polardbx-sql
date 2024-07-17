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

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.logback.AsyncAppender;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.PacketUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * show @@sql
 */
public final class ShowSqlLog {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF_PACKET = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER_PACKET.packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("LOG", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        EOF_PACKET.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        String charset = c.getResultSetCharset();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        executeInternal(proxy, charset);
    }

    public static void executeInternal(IPacketOutputProxy proxy, String charset) {

        proxy.packetBegin();

        // write header
        proxy = HEADER_PACKET.write(proxy);

        // write fields
        for (FieldPacket field : FIELD_PACKETS) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = EOF_PACKET.write(proxy);

        // write rows
        byte packetId = EOF_PACKET.packetId;
        List<byte[]> loggingEvents = takeLoggingEvents();
        if (loggingEvents != null) {
            for (byte[] loggingEvent : loggingEvents) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(loggingEvent);
                row.packetId = ++packetId;
                proxy = row.write(proxy);
            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static List<byte[]> takeLoggingEvents() {
        if (!AsyncAppender.appenderMap.isEmpty()) {
            int batchSize = DynamicConfig.getInstance().consumeLogBatchSize();
            ArrayList<byte[]> ret = new ArrayList<byte[]>(batchSize);
            for (AsyncAppender appender : AsyncAppender.appenderMap.values()) {
                ArrayList<ILoggingEvent> loggingEvents = new ArrayList<ILoggingEvent>(batchSize);
                appender.getBlockingQueue().drainTo(loggingEvents, batchSize);
                if (appender.getEncoder() != null) {
                    loggingEvents.stream().forEach(t -> ret.add(appender.getEncoder().encode(t)));
                } else {
                    loggingEvents.stream().forEach(t -> ret.add(t.getMessage().getBytes()));
                }
            }
            return ret;
        }
        return null;
    }
}
