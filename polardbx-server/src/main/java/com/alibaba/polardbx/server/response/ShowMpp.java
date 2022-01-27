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
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;

import java.util.HashSet;
import java.util.Set;

public final class ShowMpp {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF_PACKET = new EOFPacket();

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

        EOF_PACKET.packetId = ++packetId;
    }

    public static void execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        String charset = c.getCharset();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        executeInternal(proxy, charset, c.getSchema());
    }

    public static void executeInternal(IPacketOutputProxy proxy, String charset, String schema) {

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
        for (InternalNode node : getNodes()) {
            RowDataPacket row = getRow(charset, node);
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String charset, InternalNode node) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(node.getInstId(), charset));
        row.add(StringUtil.encode(node.getHostPort(), charset));
        row.add(StringUtil.encode(node.isMaster() ? "W" : "R", charset));
        row.add(StringUtil.encode(node.isLeader() ? "Y" : "N", charset));
        return row;
    }

    private static Set<InternalNode> getNodes() {
        InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
        Set<InternalNode> allActiveNodes = new HashSet<>();
        allActiveNodes.addAll(manager.getAllNodes().getActiveNodes());
        allActiveNodes.addAll(manager.getAllNodes().getOtherActiveNodes());
        return allActiveNodes;
    }
}
