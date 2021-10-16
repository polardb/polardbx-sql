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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.List;
import java.util.Map;

/**
 * 调整当前 Schema 的 PlanCache容量
 */
public final class ResizePlanCache {

    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF_PACKET = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER_PACKET.packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("COMPUTE_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("OLD_CNT", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("OLD_CAPACITY", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("NEW_CNT", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("NEW_CAPACITY", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        EOF_PACKET.packetId = ++packetId;
    }

    public static void response(ByteString stmt, ServerConnection c,
                                int offset, boolean hasMore) {
        String newSizeStr = stmt.substring(offset).trim();
        try {
            int newSize = Integer.parseInt(newSizeStr);
            response(c, newSize, hasMore);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("'%s is illegal'", newSizeStr));
        }
    }

    public static void response(ServerConnection c, int newSize, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
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
        String charset = c.getCharset();

        // 取得SCHEMA
        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
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

        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new ResizePlanCacheSyncAction(db, newSize));
        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }

            for (Map<String, Object> conn : rs) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("COMPUTE_NODE")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("OLD_CNT")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("OLD_CAPACITY")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("NEW_CNT")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("NEW_CAPACITY")), charset));

                row.packetId = ++packetId;
                proxy = row.write(proxy);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
    }

}
