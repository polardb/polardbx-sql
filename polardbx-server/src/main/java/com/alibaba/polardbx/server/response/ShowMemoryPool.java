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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.Fields;
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

import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.AdaptiveMemoryPool;

/**
 * 查看线程池状态
 *
 * @author chenghui.lch
 */
public final class ShowMemoryPool {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF_PACKET = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER_PACKET.packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("USED_BYTES", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("LIMIT_BYTES", Fields.FIELD_TYPE_LONG);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("INFO", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        EOF_PACKET.packetId = ++packetId;
    }

    public static void execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        String charset = c.getCharset();
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
        List<MemoryPool> memoryPools = collectMemoryPools();
        for (MemoryPool pool : memoryPools) {
            RowDataPacket row = getRow(pool, charset);
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

    private static RowDataPacket getRow(MemoryPool pool, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(pool.getFullName(), charset));
        row.add(LongUtil.toBytes(pool.getMemoryUsage()));
        long limit = pool.getMaxLimit();
        if (limit == MemorySetting.UNLIMITED_SIZE) {
            // -1 represents unlimited
            limit = -1;
        }
        row.add(LongUtil.toBytes(limit));

        String info = "";
        if (pool instanceof AdaptiveMemoryPool) {
            info = info + " [lowWater=" + ((AdaptiveMemoryPool) pool).getMinLimit() + ",highWater="
                + ((AdaptiveMemoryPool) pool).getMaxLimit() + "]";
        }

        row.add(StringUtil.encode(info, charset));

        return row;
    }

    private static List<MemoryPool> collectMemoryPools() {
        return collectMemoryPools(MemoryManager.getInstance().getGlobalMemoryPool());
    }

    private static List<MemoryPool> collectMemoryPools(MemoryPool root) {
        List<MemoryPool> results = new ArrayList<>();
        results.add(root);
        for (MemoryPool childPool : root.getChildren().values()) {
            results.addAll(collectMemoryPools(childPool));
        }
        return results;
    }
}
