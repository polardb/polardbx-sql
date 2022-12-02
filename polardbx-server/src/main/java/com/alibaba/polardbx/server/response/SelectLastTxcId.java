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
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.matrix.jdbc.TConnection;

import java.sql.SQLException;

/**
 * To keep compatible with TXC
 * <p>
 * Created by simiao on 15-3-4.
 */
public class SelectLastTxcId {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("last_txc_xid()", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(Object[] exData, ServerConnection c, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = header.write(proxy);

        FieldPacket[] thisFields = fields;
        if (exData[0] != null) {
            thisFields = new FieldPacket[FIELD_COUNT];
            int i = 0;
            byte packetId = header.packetId;
            thisFields[i] = PacketUtil.getField(String.format("last_txc_xid(%d)", (Long) exData[0]),
                Fields.FIELD_TYPE_VAR_STRING);
            thisFields[i++].packetId = ++packetId;
        }

        for (FieldPacket field : thisFields) {
            proxy = field.write(proxy);
        }
        proxy = eof.write(proxy);
        byte packetId = eof.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        try {
            if (c.getTddlConnection() == null) {
                boolean ret = c.initTddlConnection();
                if (!ret) {
                    return;
                }
            }
            String txcId = selectLastXid(c);
            row.add(txcId.getBytes());
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex, ex.getMessage());
        }

        // 需要处理异常
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

    private static String selectLastXid(ServerConnection c) throws SQLException {
        TConnection conn = c.getTddlConnection();
        if (conn.getAutoCommit()) {
            throw new SQLException("Set auto-commit mode to off");
        }
        // Return transaction trace-id.
        Long txid = c.getTxId();
        return Long.toHexString(txid);
    }
}
