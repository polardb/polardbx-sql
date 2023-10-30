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
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import org.apache.commons.lang.BooleanUtils;

/**
 * @author agapple 2016年3月17日 下午9:29:47
 * @since 5.1.25-2
 */
public final class SelectSessionTxReadOnly {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("SESSION.TX_READ_ONLY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
    }

    public static boolean response(ServerConnection c, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();

        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        // get value
        boolean v = getValue(c);

        // write rows
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.packetId = ++tmpPacketId;
        if (v) {
            row.add(LongUtil.toBytes(1));
        } else {
            row.add(LongUtil.toBytes(0)); // 默认返回false
        }
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
        return true;
    }

    private static boolean getValue(ServerConnection c) {
        Object o = c.isReadOnly();
        if (o == null) {
            return false;
        }

        if (o instanceof Boolean) {
            return (boolean) o;
        }

        if (o instanceof Integer) {
            /**
             * Only 1 is true, else is false.
             */
            return 1 == (Integer) o;
        }

        return BooleanUtils.toBoolean(o.toString());
    }
}
