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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.*;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import java.sql.Connection;
import java.sql.SQLException;

public class SelectTsoTimestamp {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("TSO_TIMESTAMP()", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(ServerConnection c, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = header.write(proxy);
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }
        proxy = eof.write(proxy);
        byte packetId = eof.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(getTsoTimestamp(c).getBytes());
        row.packetId = ++packetId;
        proxy = row.write(proxy);
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();

    }

    private static String getTsoTimestamp(ServerConnection c) {
        TConnection tConn = c.getTddlConnection();
        if (c.getTddlConnection() == null) {
            boolean ret = c.initTddlConnection();
            tConn = c.getTddlConnection();
            if (!ret) {
                return null;
            }
        }
        final ITimestampOracle timestampOracle =
            tConn.getDs().getConfigHolder().getExecutorContext().getTransactionManager().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }
        return Long.toString(timestampOracle.nextTimestamp());
    }
}
