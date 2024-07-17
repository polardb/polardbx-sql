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
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.gms.engine.FileStoreStatistics;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;

import java.util.ArrayList;
import java.util.List;

public class ShowCacheStats {
    private static final int FIELD_COUNT = FileStoreStatistics.CACHE_STATS_FIELD_COUNT;
    private static final ResultSetHeaderPacket header =
        PacketUtil.getHeader(FileStoreStatistics.CACHE_STATS_FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FileStoreStatistics.CACHE_STATS_FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ENGINE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_ENTRIES", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("IN_FLIGHT_MEMORY_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HIT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HOTHIT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MISS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUOTA_EXCEED", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("UNAVAILABLE_NUM", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_DICTIONARY", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_TTL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_CACHE_ENTRIES", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_CACHE_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c) {
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

        // write rows
        List<byte[][]> resultList = new ArrayList<>();
        List<byte[][]> fileCacheStats = FileStoreStatistics.generateCacheStatsPacket();
        byte[][] blockCacheStats = BlockCacheManager.getInstance().generateCacheStatsPacket();
        byte[][] stripeFootCache = DefaultScanPreProcessor.getCacheStat();
        byte[][] pruneCache = ColumnarPruneManager.getCacheStat();
        resultList.addAll(fileCacheStats);
        resultList.add(blockCacheStats);
        resultList.add(pruneCache);
        resultList.add(stripeFootCache);

        if (resultList != null) {
            for (byte[][] results : resultList) {
                RowDataPacket row = new RowDataPacket(FileStoreStatistics.CACHE_STATS_FIELD_COUNT);
                for (byte[] result : results) {
                    row.add(result);
                }
                row.packetId = ++tmpPacketId;
                proxy = row.write(proxy);
            }

        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }
}
