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
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.HashSet;
import java.util.Set;

public final class ShowMpp {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        HEADER_PACKET.packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("ROLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("LEADER", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        String charset = c.getResultSetCharset();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        return executeInternal(proxy, charset, c.getSchema());
    }

    public static boolean executeInternal(IPacketOutputProxy proxy, String charset, String schema) {

        proxy.packetBegin();

        // write header
        proxy = HEADER_PACKET.write(proxy);

        // write fields
        for (FieldPacket field : FIELD_PACKETS) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!proxy.getConnection().isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        // write master rows
        for (InternalNode node : getNodes(MppScope.CURRENT)) {
            RowDataPacket row = getRow(charset, node, MppScope.CURRENT);
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        for (InternalNode node : getNodes(MppScope.SLAVE)) {
            RowDataPacket row = getRow(charset, node, MppScope.SLAVE);
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        for (InternalNode node : getNodes(MppScope.COLUMNAR)) {
            RowDataPacket row = getRow(charset, node, MppScope.COLUMNAR);
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

    protected static RowDataPacket getRow(String charset, InternalNode node, MppScope scope) {
        String role = null;

        if (scope == MppScope.CURRENT) {
            if (ConfigDataMode.isMasterMode()) {
                role = "W";
            } else if (ConfigDataMode.isRowSlaveMode()) {
                role = "R";
            } else {
                role = "CR";
            }
        } else if (scope == MppScope.SLAVE) {
            role = "R";
        } else {
            role = "CR";
        }

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(node.getInstId(), charset));
        row.add(StringUtil.encode(node.getHostPort(), charset));
        row.add(StringUtil.encode(role, charset));
        row.add(StringUtil.encode(node.isLeader() ? "Y" : "N", charset));
        return row;
    }

    public static Set<InternalNode> getNodes(MppScope scope) {
        InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
        Set<InternalNode> allActiveNodes = new HashSet<>();
        allActiveNodes.addAll(manager.getAllNodes().getAllWorkers(scope));
        return allActiveNodes;
    }
}
