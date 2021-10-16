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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Map;

public class ShowStatistic {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("table_name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("table_rows", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        ;

        fields[i] = PacketUtil.getField("column_name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        ;

        fields[i] = PacketUtil.getField("cardinality", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        ;

        eof.packetId = ++packetId;
    }

    public static void execute(ServerConnection c) {
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

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + c.getSchema() + "'");
            return;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                return;
            }
        }

        Map<String, StatisticManager.CacheLine> statisticCache =
            ds.getConfigHolder().getStatisticManager().getStatisticCache();
        for (Map.Entry<String, StatisticManager.CacheLine> entry : statisticCache.entrySet()) {
            Map<String, Long> cardinalityMap = entry.getValue().getCardinalityMap();
            if (cardinalityMap == null || cardinalityMap.isEmpty()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(entry.getKey()), charset));
                row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(entry.getValue().getRowCount())));
                row.add(null);
                row.add(null);
                row.packetId = ++packetId;
                proxy = row.write(proxy);
            } else {
                for (Map.Entry<String, Long> columnCardinality : cardinalityMap.entrySet()) {
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode(DataTypes.StringType.convertFrom(entry.getKey()), charset));
                    row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(entry.getValue().getRowCount())));
                    row.add(StringUtil.encode(DataTypes.StringType.convertFrom(columnCardinality.getKey()), charset));
                    row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(columnCardinality.getValue())));
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
}
