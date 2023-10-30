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
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.stats.TransStatsColumn;
import com.alibaba.polardbx.executor.statistic.StatisticsUtils;

import java.text.DecimalFormat;

import static com.alibaba.polardbx.stats.TransStatsColumn.NUM_COLUMN;
import static com.alibaba.polardbx.stats.TransStatsColumn.SORTED_COLUMNS;

/**
 * @author yaozhili
 */
public final class ShowTransStats {

    private static final int FIELD_COUNT = NUM_COLUMN;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        HEADER.packetId = 1;

        for (TransStatsColumn.ColumnDef column : SORTED_COLUMNS) {
            int type;
            switch (column.columnType) {
            case LONG:
                type = Fields.FIELD_TYPE_LONGLONG;
                break;
            default:
                type = Fields.FIELD_TYPE_VAR_STRING;
            }
            FIELDS[column.index] = PacketUtil.getField(column.name, type);
            FIELDS[column.index].packetId = (byte) (column.index + 2);
        }

        EOF.packetId = (byte) (FIELD_COUNT + 2);
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = HEADER.write(proxy);

        // write fields
        for (FieldPacket field : FIELDS) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = EOF.write(proxy);

        // write rows
        byte packetId = EOF.packetId;

        RowDataPacket row = getRow(c.getCharset());
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String charset) {
        Object[] results = StatisticsUtils.getInstanceStats();

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        for (TransStatsColumn.ColumnDef column : SORTED_COLUMNS) {
            switch (column.columnType) {
            case LONG:
                row.add(LongUtil.toBytes((Long) results[column.index]));
                break;
            case DOUBLE:
                row.add(StringUtil.encode(new DecimalFormat("#.###").format(results[column.index]), charset));
                break;
            default:
                row.add(StringUtil.encode(String.valueOf(results[column.index]), charset));
            }
        }

        return row;
    }
}
