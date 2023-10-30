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
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;

import java.util.concurrent.atomic.AtomicLong;

public class SelectSequenceBenchmark {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);

    public static boolean response(ServerConnection c, boolean hasMore, Object[] exData, boolean isNextval) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = header.write(proxy);

        byte packetId = header.packetId;

        String fieldName = isNextval ? "SEQ_NEXTVAL_BENCHMARK (ns)" : "SEQ_SKIP_BENCHMARK (ns)";
        FieldPacket field = PacketUtil.getField(fieldName, Fields.FIELD_TYPE_VAR_STRING);
        field.packetId = ++packetId;
        proxy = field.write(proxy);

        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            proxy = eof.write(proxy);
        }

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        row.add(benchmarkSequence(c, (String) exData[0], (int) exData[1], isNextval).getBytes());

        row.packetId = ++packetId;
        proxy = row.write(proxy);

        EOFPacket lastEof = new EOFPacket();

        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }

        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }

    private static String benchmarkSequence(ServerConnection c, String seqName, int count, boolean isNextval) {
        final String schemaName = c.getSchema();
        long startTime = System.nanoTime();
        if (isNextval) {
            benchmarkNextval(schemaName, seqName, count);
        } else {
            benchmarkSkip(schemaName, seqName, count);
        }
        return String.valueOf(System.nanoTime() - startTime);
    }

    private static void benchmarkNextval(String schemaName, String seqName, int count) {
        for (int i = 0; i < count; i++) {
            SequenceManagerProxy.getInstance().nextValue(schemaName, seqName);
        }
    }

    private static final AtomicLong counter = new AtomicLong(1L);

    private static void benchmarkSkip(String schemaName, String seqName, int count) {
        long min = counter.getAndAdd(count);
        for (int i = 0; i < count; i++) {
            SequenceManagerProxy.getInstance().updateValue(schemaName, seqName, min++);
            // SequenceManagerProxy.getInstance() .updateValue(schemaName, seqName,
            // ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
        }
    }
}
